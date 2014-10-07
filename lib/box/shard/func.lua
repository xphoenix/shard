--local log    = require 'box.log'
local table  = require 'table'
local hitime = require 'time.hires'

local error  = _G.error
local assert = _G.assert
local pairs  = _G.pairs
local ipairs  = _G.ipairs
local type   = _G.type
--local unpack = _G.unpack
--local pcall  = _G.pcall
local tostring = _G.tostring
local tonumber = _G.tonumber

local box = _G.box

module(...)

local MAX_INDEX_SIZE = 10
local numbers = false

local function extract_key(space, ...)
	local tuple = {...}
	local key = {}
	-- there is no way to detect index keylen
	for i = 0, MAX_INDEX_SIZE - 1 do
		if box.space[space].index[0].key_field[ i ] == nil then
			break
		end
		local fno = box.space[space].index[0].key_field[i].fieldno + 1
		if tuple[fno] == nil then
			error("incomplete primary key in tuple")
		end
		table.insert(key, tuple[fno])
	end
	return key
end

local function unpack_number(no)
	if type(no) == 'number' then
		return no
	end
	if numbers then
		return box.unpack('i', no)
	end
	return tonumber(no)
end

local function unlooper(max)
	max = max or 100
	local cnt = 0
	return function()
		cnt = cnt + 1
		assert(cnt < max,"Force unloop "..tostring(max))
	end
end

local function join(t,sep)
	local buf = {}
	for _,v in pairs(t) do
		table.insert(buf,tostring(v))
	end
	return table.concat(buf, sep)
end

local _dump_seen = {}
local function _dumper(t)
	if type(t) == 'table' then
		
		if _dump_seen[ tostring(t) ] then
			return '\\'..tostring(t)
		end
		_dump_seen[ tostring(t) ] = true
		
		local keys = 0
		for _,_ in pairs(t) do keys = keys + 1 end
		if keys ~= #t then
			local sub = {}
			local prev = 0
			for k,v in pairs(t) do
				if type(k) == 'number' and k == prev + 1 then
					prev = k
					table.insert(sub,_dumper(v))
				else
					table.insert(sub,tostring(k)..'='.._dumper(v))
				end
			end
			return '{'..table.concat(sub,'; ')..'}'
		else
			local sub = {}
			for _,v in ipairs(t) do
				table.insert(sub,_dumper(v))
			end
			return '{'..table.concat(sub,', ')..'}'
		end
	elseif type(t) == 'number' then
		return tostring(t)
	elseif type(t) == 'string' then
		return "'" .. t .. "'"
	else
		return tostring(t)
	end
end
local function dumper(x)
	local r = _dumper(x)
	_dump_seen = {}
	return r
end

return {
	hitime = hitime;
	extract_key = extract_key;
	unpack_number = unpack_number;
	MAX_INDEX_SIZE = MAX_INDEX_SIZE;
	unlooper = unlooper;
	join = join;
	dumper = dumper;
}
