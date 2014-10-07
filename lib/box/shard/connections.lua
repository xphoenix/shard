local object = require 'box.oop'
local log    = require 'box.log'
local table  = require 'table'
local hitime = require 'time.hires'
local math   = require 'math'
local string = require 'string'

require 'box.net.box'

local error  = _G.error
local assert = _G.assert
local pairs  = _G.pairs
local type   = _G.type
local unpack = _G.unpack
local pcall  = _G.pcall
local tostring = _G.tostring

local box = _G.box

--local prefix = string.match(...,'(.+)%.[^.]+$')
-- assert(false,prefix)

--log.modlevel(log.INFO)
--log.modlevel(log.level) -- or noop

module(...)

local M = object:define({ __name = ... })


function M:init(config,schema)
	--log.info("configure connections from schema ",schema)
	self.schema = schema
	self.timeout = config.timeout or 1
	self.ping_interval = config.ping_interval or 1
	local cl = schema.cl
	local pl = schema.prev and schema.pl or nil
	
	local peers = {}
	local all = {}
	local sc = {
		curr = cl;
	}
	if pl and pl ~= cl then
		sc.prev = pl
	end
	for sch,list in pairs(sc) do
		for ix,shard in pairs(list) do
			for _,node in pairs(shard) do
				local mode,weight,host,port = unpack(node)
				local key = host ..':'..tostring(port)
				local peer
				if not peers[key] then
					peer = {
						key    = key;
						host   = host;
						port   = port;
						weight = weight;
						mode   = mode;
						roles  = {};
					}
					peers[key] = peer
					
				else
					peer = peers[key]
				end
				all[ sch ] = all[ sch ] or {}
				all[ sch ][ mode ] = all[ sch ][ mode ] or {}
				all[ sch ][ mode ][ ix ] = all[ sch ][ mode ][ ix ] or {}
				
				all[ sch ][ 'any' ] = all[ sch ][ 'any' ] or {}
				all[ sch ][ 'any' ][ ix ] = all[ sch ][ 'any' ][ ix ] or {}
				
				--table.insert(all[ sch ][ mode ][ ix ],peer)
				table.insert(peer.roles, { sch,ix,mode })
				log.debug("entry in ",sch," ",ix," all: ",table.concat(node, ', '))
			end
		end
	end
	
	self.all = all
	self.peers = peers
	self.me = {} -- curr = {}; prev = {}; }
	for _,sch in pairs({"curr","prev"}) do
		self.me[ sch ] = {}
		for _,mode in pairs({"rw","ro","any"}) do
			self.me[ sch ][mode] = { hash = {}; list = {} }
		end
	end
	-- debug 2
	--[[
	for sch,modes in pairs(all) do
		print(sch)
		for mode,shards in pairs(modes) do
			print("\t",mode)
			for shno,nodes in pairs(shards) do
				print("\t\t",shno)
				for _,node in pairs(nodes) do
					print("\t\t\t",node.key)
				end
			end
		end
	end
	]]--
	
	self:connect_all()
end

function M:whoami()
	local info = {}
	local list,mode = self:curr_me()
	if #list > 0 then
		table.insert(info,string.format("curr: %s:[%s]", mode,table.concat( list, ', ' )))
	end
	if self.schema.prev then
		list,mode = self:prev_me()
		if #list > 0 then
			table.insert(info, string.format("prev: %s:[%s]", mode,table.concat( list, ', ' )))
		end
	end
	return #info > 0 and table.concat(info, '; ') or 'just proxy'
end

function M:connect_all()
	-- TODO: reconnect/reconfigure
	self.waiting = {}
	for k,node in pairs(self.peers) do
			table.insert(self.waiting,node)
	end
	for k,node in pairs(self.peers) do
			local con = box.net.box(node.host, node.port, self.timeout)
			node.con = con
			node.fib = box.fiber.create(function()
				box.fiber.name("shardc."..node.key);
				box.fiber.detach()
				while true do
					local r,e = pcall(function()
						local t = con:timeout( self.timeout ):call('box.id')
						if t then return t[0] else return nil end
					end)
					if r and e then
						node.id = e
						if e == box._id then
							log.info("It's me / " .. node.key .. ' / ' .. box.id())
							con:close()
							node.con = box.net.self
							node.fib = nil
							node.self = true
							
							for _,role in pairs(node.roles) do
								local schema,shno,mode = unpack(role)
								--[[
								self.me[ schema ][ mode ] = self.me[ schema ][ mode ] or {
									list = {};
									hash = {};
								}
								self.me[ schema ][ 'any' ] = self.me[ schema ][ 'any' ] or {
									list = {};
									hash = {};
								}
								]]--
								table.insert( self.me[ schema ][ mode ].list, shno )
								self.me[ schema ][ mode ].hash[ shno ] = true
								
								table.insert( self.me[ schema ][ 'any' ].list, shno )
								self.me[ schema ][ 'any' ].hash[ shno ] = true
							end
							
							for schema, modes in pairs(self.me) do
								for mode,me in pairs(modes) do
									table.sort(me.list)
								end
							end
							
							for mode,me in pairs( self.me.curr ) do
								if mode ~= 'any' and #me.list > 0 then
									log.notice("I'm shard no", unpack(me.list), "for", mode, " / ", self:whoami())
								end
							end
							
							self:node_connected(node)
							
							return
						end
						break
					else
						log.warn(r,e)
						box.fiber.sleep(1)
					end
				end
				self:node_connected(node)
				local connected = true
				while true do
					-- box.fiber.sleep(1)
					local time1 = hitime();
					local r,e = pcall(function()
						return con:timeout(remote_timeout):ping()
					end)
					time1 = hitime() - time1
					--log.info("pong %s:%s -> %s %s %f", con.host, con.port, r,e, time1)
					if r and e then
						if not connected then
							self:node_connected(node)
							connected = true
						end
						box.fiber.sleep(self.ping_interval)
					else
						if connected then
							log.error("ping failed: %s %s in %f",r,e,time1)
							self:node_disconnected(node)
							connected = false
						else
							box.fiber.sleep(self.ping_interval)
						end
					end
				end
			end)
			box.fiber.resume(node.fib)
	end
	
end

function M:waitonline()
	assert(not self._waitonline)
	if #self.waiting > 0 then
		self._waitonline = box.ipc.channel(1)
		local x = self._waitonline:get()
		return x
	else
		return true
	end
	M.schema:waitonline()
end


function M:status(nodekey)
	local node = self.peers[nodekey]
	if not node then return '?' end
	if node.connected == nil then
		return '!'
	else
		return node.connected and '+' or '-'
	end
end

function M:node_connected(node)
	--log.warn("connected to node ",node.key)
	for _,role in pairs(node.roles) do
		local schema,shno,mode = unpack(role)
		for ix,one in pairs( self.all[ schema ][  mode ][ shno ] ) do
			if one == node then
				error("FATAL! Contact developer. Node "..node.key.." already exists for "..schema.."/"..mode.."/"..shno)
			end
		end
		node.connected = true
		table.insert( self.all[ schema ][  mode ][ shno ], node )
		table.insert( self.all[ schema ][ 'any' ][ shno ], node )
	end
	if #self.waiting > 0 then
		for _,one in pairs(self.waiting) do
			if one == node then
				-- log.info("removing node",node.key)
				table.remove(self.waiting,_)
				break
			end
		end
		if #self.waiting == 0 then
			log.notice("Cluster completely online")
			if self._waitonline then
				self._waitonline:put(true)
				self._waitonline = nil
			end
			--log.info('',box.cjson.encode(self.me))
		end
	end
end

function M:node_disconnected(node)
	log.error("node",node.key,"disconnected")
	node.connected = false
	for _,role in pairs(node.roles) do
		local schema,shno,mode = unpack(role)
		for ix,one in pairs( self.all[ schema ][  mode ][ shno ] ) do
			if one == node then
				table.remove( self.all[ schema ][  mode ][ shno ], ix )
				break
			end
		end
		for ix,one in pairs( self.all[ schema ][ 'any' ][ shno ] ) do
			if one == node then
				table.remove( self.all[ schema ][ 'any' ][ shno ], ix )
				break
			end
		end
	end
end


function M:curr(mode, ...)
	-- mode : rw / ro / any
	local  shno = self.schema:curr(...)
	local  set = self.all.curr[ mode ][ shno ]
	if not set or #set < 1 then return nil end
	return set[ math.random(1, #set) ].con
end

function M:prev(mode, ...)
	if not self.schema.prev then return nil end
	local  shno = self.schema:prev(...)
	local  set = self.all.prev[ mode ][ shno ]
	if not set or #set < 1 then return nil end
	return set[ math.random(1, #set) ].con
end

function M:curr_by(shno,mode)
	local  set = self.all.curr[ mode or 'any' ][ shno ]
	if not set or #set < 1 then return nil end
	return set[ math.random(1, #set) ].con
	--log.debug("select curr_by(",shno,mode,") from ",join(set,' '))
	--local node = set[ math.random(1, #set) ]
	--log.debug("taken node",node.key," ",node.con)
	--return node.con
end

function M:prev_by(shno,mode)
	if not self.schema.prev then return nil end
	local  set = self.all.prev[ mode or 'any' ][ shno ]
	if not set or #set < 1 then return nil end
	return set[ math.random(1, #set) ].con
end

function M:curr_me(mode)
	if mode then
		return self.me.curr[ mode ].list
	else
		if #self.me.curr[ 'rw' ].list > 0 then
			return self.me.curr[ 'rw' ].list, 'rw'
		elseif# self.me.curr[ 'ro' ].list > 0 then
			return self.me.curr[ 'ro' ].list, 'ro'
		else
			return {},'prx'
		end
	end
end

function M:prev_me(mode)
	if not self.schema.prev then return end
	if mode then
		return self.me.prev[ mode ].list
	else
		if #self.me.prev[ 'rw' ].list > 0 then
			return self.me.prev[ 'rw' ].list, 'rw'
		elseif #self.me.prev[ 'ro' ].list > 0 then
			return self.me.prev[ 'ro' ].list, 'ro'
		else
			return {},'prx'
		end
	end
end

function M:is_curr_me(shno,mode)
	return self.me.curr[ mode or 'any' ].hash[ shno ] or false
end

function M:is_prev_me(shno,mode)
	if not self.schema.prev then return nil end
	return self.me.prev[ mode or 'any' ].hash[ shno ] or false
end

function M:is_both(shno1,shno2,mode)
	-- mode = mode or 'any'
	return self.me.curr[ mode or 'any' ].hash[ shno1 ] and self.me.prev[ mode or 'any' ].hash[ shno ] or false
end

function M:curr_iterator(mode)
	mode = mode or 'rw'
	local shards = self.all.curr[ mode ]
	assert(shards, "Unknown mode: "..mode)
	assert(#shards > 0, "Misconfiguration: empty shard list")
	local lst = 1
	
	return function()
		local try = lst
		repeat
			try = math.fmod(try, #shards) + 1
			local shard = shards[ try ]
			if #shard > 0 then
				local node = shard[ math.random( 1, #shard ) ]
				lst = try
				return node
			end
		until try == lst
		return
		-- error("Can't reach any of "..mode.." nodes")
	end
end

return M
