local ffi = require "ffi"
local pcall = _G.pcall
local error = _G.error
--local print = _G.print
local string = require 'string'
local package = require 'package'

module ("guava")

ffi.cdef [[
	unsigned guava(long, unsigned);
]]

local libguava

do
	local r,e = pcall(function() return ffi.load("guava") end)
	if r then libguava = e
	else
	r,e = pcall(function() return ffi.load("./libguava.so") end)
	if r then libguava = e
	else
	
	for pp in string.gmatch(package.path, "([^;]+)") do
		pp = pp:sub(1,-6)
		--print(pp)
		r,e = pcall(function() return ffi.load(pp .. "libguava.so") end)
		if r then
			libguava = e
		break
		else --print (pp, " ", e)
		end
	end
	
	if not libguava then
		error("libguava.so: cannot open shared object file: No such file or directory")
	end
	
	end end
end

function guava(key, buckets)
	return libguava.guava(key,buckets)
end

return guava
