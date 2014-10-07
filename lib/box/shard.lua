local object  = require 'box.oop'
local log     = require 'box.log'
local condvar = require 'box.cv'
local table   = require 'table'
local string  = require 'string'
local math    = require 'math'

local prefix   = ...

local Schema   = require( prefix..'.schema' )
local Conns    = require( prefix..'.connections' )
local Internal = require( prefix..'.internal' )
local Func     = require( prefix..'.func' )

local unpack_number  = Func.unpack_number
local extract_key    = Func.extract_key
local unlooper       = Func.unlooper
local join           = Func.join
local dumper         = Func.dumper

local error    = _G.error
local assert   = _G.assert
local pairs    = _G.pairs
local ipairs   = _G.ipairs
local type     = _G.type
local tonumber = _G.tonumber
local tostring = _G.tostring
local unpack   = _G.unpack
local select   = _G.select
local print    = _G.print

local box      = _G.box

box._id = box._id or box.uuid_hex()
box.id = box.id or function() return box._id end

local M
local configured = false

do
	local curr = _G
	local first = string.match(...,'([^.]+)')
	if package.loaded['strict'] then
		global(first)
	end
	--print(first)
	local prev
	local last

	for part in string.gmatch(...,'([^.]+)') do
		last = part prev = curr
		curr[part] = curr[part] or {}
		curr = curr[part]
	end

	M = object:define({ __name = ... })
	prev[last] = M
end


module(...)

box.fiber.wrap(function()
	box.fiber.name('rw-ro')
	while true do
		local prev = M.isrw
		M.isrw = box.info.status == 'primary'
		--if M.isrw ~= prev then
		--	M.reconfigure_rwro()
		--end
		box.fiber.sleep(0.05)
	end
end)

M.id = box.id
function M.fullid()
	return string.format('%s:%s[%s]',box.cfg.bind_ipaddr,box.cfg.primary_port, M.id())
end

function M.whoami()
	return M.conns:whoami()
end

function M.curr_me()
	local list,mode = M.conns:curr_me()
	return list or {}
end

function M.prev_me()
	local list,mode = M.conns:prev_me()
	return list or {}
end

local function p(f,...) print(string.format( f,... )) end

function M.statusline(info,...)
	info = info or M.statusraw()
	local extra = ''
	local vstatus = ''
	if info.online.curr then
		vstatus = vstatus .. string.format('curr:%s:%d of %d', info.online.curr.status,info.online.curr.con,info.online.curr.all)
	end
	if info.online.prev then
		vstatus = vstatus .. string.format('; prev:%s:%d of %d', info.online.prev.status,info.online.prev.con,info.online.prev.all)
	end
	if select('#',...) > 0 then
		for _,v in ipairs({...}) do
			extra = extra .. ' '..tostring(v)
		end
	end
	p('%s :: %s [%s] %s (%s)%s\n',info.id, info.srv, info.state, info.status, vstatus, extra)
end

function M.status(info,...)
	info = info or M.statusraw()
	M.statusline(info,...)
	local src = {curr = info.curr;prev = info.prev;}
	for mode,list in pairs(src) do
		p("%s\n",mode)
		for shno,shard in ipairs(list) do
			--print(dumper(shard).."\n")
			local line,rw,ro = {},{},{}
			for t, nodes in pairs(shard) do
				local to = t == 'rw' and rw or ro
				for _,v in ipairs(nodes) do
					table.insert(to,string.format("[%s]%s%s",v[3],v[1],v[2] > 1 and "*"..v[2] or ''))
				end
			end
			print("\t"..table.concat( {
				rw and table.concat(rw,', ') or nil,
				ro and '('..table.concat(ro,', ')..')' or nil,
			},' ' )..", -- shard "..shno)
			--p("\t -- shard %d\n",shno)
		end
	end
	
end

function M.fullstatus(each)
	M.status()
	local cv = condvar()
	local info = {}
	for k,node in pairs( M.conns.peers ) do
		if not node.self then
		info[k] = false
		cv:async(function()
			--print("fetching from ",k)
			local st = node.con:timeout(1):call(prefix..'.statusjson')
			info[k] = box.cjson.decode(st[0])
			--print("fetched from ",k, st)
		end)
		end
	end
	cv:wait()
	--print(dumper(info))
	for k,node in pairs(M.conns.peers) do
		if info[k] ~= nil then
			if info[k] then
				if each then
					M.status(info[k],'(remote)')
				else
					M.statusline(info[k],'(remote)')
				end
			else
				print("[WARN] Not received info from ",k)
			end
		end
	end
end

function M.statusjson()
	return box.cjson.encode(M.statusraw())
end

function M.statusraw()
	local sc = M.schema
	local info = {
		id = M.id();
		srv = string.format('%s:%s',box.cfg.bind_ipaddr,box.cfg.primary_port);
		state = M.schema.prev and "resharding" or "stable",
	}
	info.status = "unknown"
	
	local src = {
		curr = sc.cl;
		prev = sc.pl;
	}
	local online = {
		curr = { status = "unknown"; all = 0; con = 0 };
		prev = { status = "unknown"; all = 0; con = 0 };
	};
	local total = 0
	local connected = 0
	for mode,list in pairs(src) do
		--print(mode..":\n")
		info[mode] = {}
		for shno,shard in ipairs(list) do
			info[mode][shno] = {}
			local rw,ro = {},{}
			for _,node in ipairs(shard) do
				local to = node[1] == 'rw' and rw or ro
				local nk = node[3]..':'..node[4]
				local st = M.conns:status(nk)
				total = total + 1
				online[mode].all = online[mode].all + 1
				if st == '+' then
					connected = connected + 1
					online[mode].con = online[mode].con + 1
				end
				table.insert(to,{ nk; node[2]; st })
			end
			
			--status = status .. string.format("\t%d: %s\n", shno,dumper(shard))
			local shinfo = {}
			if #rw > 0 then
				info[mode][shno][ 'rw' ] = rw
				--table.insert(shinfo, table.concat(rw, ","))
			end
			if #ro > 0 then
				info[mode][shno][ 'ro' ] = ro
				--table.insert(shinfo, '('..table.concat(ro, ",")..')')
			end
			shinfo = table.concat(shinfo,' ')
			--print("\t'"..shinfo.."', -- shard "..shno.."\n")
			--table.insert(shards, shinfo)
			--status = status ..' ('..table.concat(ro,',')..')'.."\n"
		end
		--print("\n")
		if online[mode].all == online[mode].con then
			online[mode].status = "online"
		elseif online[mode].con == 0 then
			online[mode].status = "offline"
		else
			online[mode].status = "partial"
		end
	end
	info.online = online
	info.peers = total
	info.connected = connected
	if connected == total then
		info.status = "online"
	elseif connected == 0 then
		info.status = "offline"
	else
		info.status = "partial"
	end
	--print("\n")
	--print(dumper(info))
	return info
	--print(dumper(shards))
	
	--print(status)
	--return status
end

function M.reconfigure_rwro()
	
end



function M.waitonline()
	M.conns:waitonline()
end

function M.configure(config)
	assert(not configured,"Already configured")
	configured = true
	M.schema = Schema(config)
	M.conns = Conns(config,M.schema, prefix..'.id')
	
	-- M.internal = Internal()
end

function M.select(space,...)
	--return M.eselect('any',space,...)
	--log.level = log.DEBUG
	space = unpack_number(space)
	local shno = M.schema:curr(space,...)
	if M.conns:is_curr_me(shno,'rw') then
		-- select locally
		local tuple = box.select(space,0,...)
		if tuple then return tuple end
		-- passthrough to prev schema
		
	elseif M.conns:is_curr_me(shno,'ro') then
		-- select locally. then select from master
		local tuple = box.select(space,0,...)
		if tuple then return tuple end
		local remote = M.conns:curr_by(shno,'rw')
		if remote then
			local tuple = remote:select(space,0,...)
			if tuple then return tuple end
			-- passthrough to prev schema
		else
			-- passthrough to prev schema
		end
	else
		-- not my shard. take from other's master
		local remote = M.conns:curr_by(shno,'rw') or M.conns:curr_by(shno,'ro')
		log.debug("Not my curr shard ",shno,". Taking from ",remote)
		if remote then
			-- select from remote master or slave
			local tuple = remote:select(space,0,...)
			if tuple then return tuple end
			-- passthrough to prev schema
		else
			-- shard completely offline
			error("Shard no ",shno," is completely offline")
		end
	end
	log.debug("Falled back to prev schema")
	if not M.schema.prev then return end
	
	shno = M.schema:prev(space,...)
	
	if M.conns:is_prev_me(shno,'rw') then
		return box.select(space,0,...)
	elseif M.conns:is_prev_me(shno,'ro') then
		local tuple = box.select(space,0,...)
		if tuple then return tuple end
		local remote = M.conns:prev_by(shno,'rw')
		if remote then
			return remote:select(space,0,...)
		end
	else
		local remote = M.conns:prev_by(shno,'rw') or M.conns:prev_by(shno,'ro')
		log.debug("Not my prev shard ",shno,". Taking from ",remote)
		if remote then
			return remote:select(space,0,...)
		else
			-- shard completely offline
			error("Shard no ",shno," is completely offline")
		end
	end
	error("Unreach")
end

function M.replace(space,...) return M._replace(1,space,...) end
function M._replace(ttl,space,...)
	ttl = tonumber(ttl)
	space = unpack_number(space)
	local shno = M.schema:curr(space,...)
	if M.conns:is_curr_me(shno,'rw') then
		local x = box.replace(space, ...)
		--log.info("[",ttl,"] calling box.replace: ",space,..., "=",x)
		-- if have prev, delete?
		return x
	else
		--log.info("proxy replace ("..prefix..'._replace'.."): ",...)
		if ttl < 1 then
			error("Can't redirect request to shard "..shno.." (ttl=0)")
		end
		local remote = M.conns:curr_by(shno,'rw')
		if not remote then
			error("Master not accessible for "..shno)
		end
		local x = remote:call(prefix..'._replace',tostring(ttl - 1),tostring(space),...)
		--log.info("remote call returned: ",x)
		return x
	end
end

function M.insert(space,...) return M._insert(1,space,...) end
function M._insert(ttl,space,...)
	ttl = tonumber(ttl)
	space = unpack_number(space)
	local shno = M.schema:curr(space,...)
	if M.conns:is_curr_me(shno,'rw') then
		if not M.schema.prev then
			return box.insert(space, ...)
		end
		local key = extract_key(space, ...)
		local t = box.select(space,0,unpack(key))
		assert(t == nil, "Duplicate key exists in unique index 0")
		local pshno = M.schema:prev(spce,...)
		
		if M.conns:is_both(shno,pshno,'rw') then
			return box.insert(space,...)
		end
		
		local remote = M.conns:prev_by(pshno,'rw')
		assert(remote,"Master not accessible for prev "..shno)
		t = remote:select(space,0,unpack(key))
		assert(t == nil, "Duplicate key exists in unique index 0")
		
		return box.insert(space,...)
	else
		--log.info("proxy replace ("..prefix..'._replace'.."): ",...)
		assert(ttl > 0, "Can't redirect request to shard "..shno.." (ttl=0)")
		local remote = M.conns:curr_by(shno,'rw')
		assert(remote,"Master not accessible for curr "..shno)
		return remote:call(prefix..'._insert',tostring(ttl - 1),tostring(space),...)
	end
end

function M.delete(space,...) return M._delete(1,space,...) end
function M._delete(ttl,space,...)
	ttl = tonumber(ttl)
	space = unpack_number(space)
	local shno = M.schema:curr(space,...)
	
	if M.conns:is_curr_me(shno,'rw') then
		local ptuple
		if M.schema.prev then
			local pshno = M.schema:prev(space,...)
			local remote = M.conns:prev_by(pshno, 'rw' )
			if remote then
				ptuple = remote:delete(space,...)
			else
				log.error("Could not call delete key %d:{ %s } from prev shard %d: not accessible", space,table.concat({...}, ' '), pshno)
			end
		end
		if not ptuple then
			return box.delete(space, ...)
		else
			local tuple = box.delete(space,...)
			return tuple or ptuple
		end
	else
		--log.info("proxy replace ("..prefix..'._replace'.."): ",...)
		if ttl < 1 then
			error("Can't redirect request to shard "..shno.." (ttl=0)")
		end
		local remote = M.conns:curr_by(shno,'rw')
		if not remote then
			error("Master not accessible for "..shno)
		end
		local x = remote:call(prefix..'._delete',tostring(ttl - 1),tostring(space),...)
		--log.info("remote call returned: ",x)
		return x
	end
end

function M.update(space,...) return M._update(1,space,...) end
function M._update(ttl,space,...)
	ttl = tonumber(ttl)
	space = unpack_number(space)
	local format
	local oplist
	local key = {}
	local args = {...}
	for i = 1,Func.MAX_INDEX_SIZE do
		if box.space[space].index[ 0 ].key_field[i - 1] == nil then
			format = args[ i ]
			oplist = { select(i + 1, ...) }
			break
		end
		table.insert(key, args[i])
	end
	assert( #key > 0 , "Wrong key" )
	assert( format, "Wrong format")
	assert( math.fmod(#oplist, 2) == 0, "Wrong oplist (contains "..#oplist.." elements)" )
	
	local shno = M.schema:curr(space,unpack(key))
	if M.conns:is_curr_me(shno,'rw') then
		local tuple = box.select(space,0,unpack(key))
		if tuple or not M.schema.prev then
			return box.update(space,key,format,unpack(oplist))
		end
		local pshno = M.schema:prev(space,unpack(key))
		if M.conns:is_both(shno,pshno,'rw') then
			return box.update(space,key,format,unpack(oplist))
		end
		
		local remote = M.conns:prev_by(pshno, 'rw' )
		if remote then
			local ptuple = remote:select(space,0,unpack(key))
			if not ptuple then
				return box.update(space,key,format,unpack(oplist))
			end
			
			-- recheck cond
			tuple = box.select(space,0,unpack(key))
			if tuple then
				return box.update(space,key,format,unpack(oplist))
			end
			box.insert(space,ptuple:unpack())
			return box.update(space,key,format,unpack(oplist))
		else
			error("Could not call update key "..space..":{ ".. table.concat(key,' ') .." } from prev shard "..pshno..": not accessible")
		end
	else
		assert(ttl > 0, "Can't redirect request to shard "..shno.." (ttl=0)")
		local remote = M.conns:curr_by(shno,'rw')
		assert(remote,"Master not accessible for curr "..shno)
		local p = {}
		for i,v in ipairs({...}) do
			table.insert(p,tostring(v))
		end
		return remote:call(prefix..'._update',tostring(ttl - 1),tostring(space),unpack(p))
	end
end

function M.call(mode,proc,...)
	local shards = M.conns.all.curr[ mode ]
	assert(shards, "Unknown mode: "..mode)
	assert(#shards > 0, "Misconfiguration: empty shard list")
	local lst = M._last_call or 1 -- last called shard
	local try = lst
	local unloop = unlooper()
	repeat
		try = math.fmod(try, #shards)+1
		local shard = shards[ try ]
		--log.info("try %d of %d/%d, will exit after: %s",try,#shards,lst, try == lst)
		if #shard > 0 then
			local node = shard[ math.random( 1, #shard ) ]
			M._last_call = try
			--log.info("Try node %s from %s (%s)",node.key, try, node.con)
			return node.con:call( proc,... )
		end
		unloop()
	until try == lst
	error("Can't reach any of "..mode.." nodes")
end

return M

--[[
function M.eselect(mode,space,...)
	space = unpack_number(space)
	local shno = M.schema:curr(space,...)
	
	if M.conns:is_curr_me(mode) then
		local tuple = box.select(space, 0, ...)
		if tuple ~= nil then return tuple end
		
		log.info("Not found 0 from ",remote)
		-- danger: may make twice more queries to master
		if not M.isrw then
			-- replica lag
			local remote = M.conns:curr_by(shno,'rw')
			if remote then
				local tuple = remote:select(space,0,...)
				if tuple then return tuple end
			end
		end
		
	else
		local remote = M.conns:curr_by(shno,mode)
		local tuple = remote:select(space,0,...)
		if tuple then return tuple end
		log.info("Not found 1 from ",remote)
		local remote1 = M.conns:curr_by('rw')
		if remote1 and remote ~= remote1 then
			local tuple = remote1:select(space,0,...)
			if tuple then return tuple end
			log.info("Not found 2 from ",remote1)
		end
	end
	
	if not M.schema.prev then return end
	
	prev_shno = M.schema:prev(space,...)
	
	local remote = M.conns:prev_by(prev_shno,mode)
	log.info("remote is",remote)
	return remote:select(space,0,...)
end
]]--
