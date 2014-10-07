local object = require "box.oop"
local log = require "box.log"

local ipc    = box.ipc
-- local fiber  = box.fiber

-- local base = _G
--log.modlevel(log.DEBUG)
-- module("box.net", package.seeall)

---- test
-- local print    = base.print
-- local tonumber = base.tonumber
-- local tostring = base.tostring
-- local error    = base.error

local M = object:define({ __name = 'box.cv' })
box.cv = M
local seq = 0
function M:init()
	self.c = 0
end

function M:async(f,...)
	local args = {...}
	if self.c == 0 then
		self.chan = ipc.channel(1)
	end
	seq = seq + 1
	self.c = self.c + 1
	box.fiber.wrap(function()
		box.fiber.name('box.cv.'..tostring(seq))
		local r,e = pcall(f,unpack(args))
		assert(self.c > 0)
		self.c = self.c - 1
		if self.c == 0 then
			self.chan:put(true)
		end
		if not r then
			error(e)
		end
	end)
end

function M:wait()
	if self.c == 0 then
		return
	end
	local recv = self.chan:get()
	self.chan = nil
	return
end

return M
