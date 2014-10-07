require 'box.net.box'
local object = require "box.oop"
local log = require "box.log"

local M = object:define({ __name = 'box.connpool' })
box.connpool = M

function M:init(list, opts)
	self.timeout = opts.timeout or 1
	self.name = opts.name or 'pool'
	self.list = list
	self.connected = {}
	self.connected.any = {}
	self.connected.rw = {}
	self.connected.ro = {}
	print("init pool (",self.name,") with ",#list, " servers")
end

function M:connect()
	for _,v in pairs(self.list) do
		local host,port
		if type(v) == 'table' then
			host,port = unpack(v)
		else
			host,port = v:match('^([^:]+):([^:]+)$')
		end
		box.fiber.wrap(function(host,port)
			box.fiber.name(self.name..":"..host..":"..port)
			local con = box.net.box(host, port, 1)
			local connected = false
			while true do
				local r,e = pcall(function()
					return con:timeout(self.timeout):ping()
				end)
				if r and e then
					if not connected then
						local r,e = pcall(function()
							return con:timeout(self.timeout):call('box.dostring','return box.info.status')
						end)
						if r and e[0] == 'primary' then
							table.insert(self.connected.rw, con)
						elseif r and string.find(e[0],'replica') then
							table.insert(self.connected.ro, con)
						end
						table.insert(self.connected.any, con)
						connected = true
						log.debug("connected for %s -> %s:%s",self.name,host,port)
					end
					box.fiber.sleep(1)
				else
					if connected then
						log.warn("connection for %s -> %s:%s was reset: %s",self.name,host,port, e)
						for st,c in pairs(self.connected) do
							for k,v in pairs(c) do
								if con == v then
									table.remove(c,k)
									break
								end
							end
						end
						connected = false
					elseif connected == nil then
						log.warn("connection for %s -> %s:%s was initially failed: %s",self.name,host,port, e)
					end
					box.fiber.sleep(0.05)
				end
			end
		end, host,port)
	end
	return self
end

function M:ready()
	return #self.connected.any > 0
end

function M:any()
	if #self.connected.any > 0 then
		return self.connected.any[ math.random(1, #self.connected.any) ]
	else
		return
	end
end

function M:rw()
	if #self.connected.rw > 0 then
		return self.connected.rw[ math.random(1, #self.connected.rw) ]
	else
		return
	end
end

function M:ro()
	if #self.connected.ro > 0 then
		return self.connected.ro[ math.random(1, #self.connected.ro) ]
	else
		return
	end
end

return M
