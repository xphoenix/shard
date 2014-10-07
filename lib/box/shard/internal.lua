local object = require 'box.oop'
local log    = require 'box.log'
local table  = require 'table'
local hitime = require 'time.hires'

local error  = _G.error
local pairs  = _G.pairs
local type   = _G.type
local unpack = _G.unpack
local pcall  = _G.pcall
local tostring = _G.tostring

local box = _G.box

--local prefix = string.match(...,'(.+)%.[^.]+$')
log.modlevel(log.DEBUG)

module(...)

local M = object:define({ __name = ... })

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


--[[
function M:copy_tuple(space,...)
	space = tonumber(space)
	local key = extract_key(space, ...)
	local exists = box.select(space, 0, unpack(key))
	if exists then return end -- tuple exists here
	--local shardno = curr(space, unpack(key))
	
	if shardno ~= me then
                    errorf("Can't copy tuple: curr points the other shard: %d",
                        shardno)
                end
                if mode ~= 'rw' then
                    errorf("Can't copy tuple: shard %d isn't in 'rw' mode",
                        me)
                end
                return box.insert(space, ...)
	
end
]]--

function M:replace(ttl, space, ...)
	ttl = tonumber(ttl)
	space = unpack_number(space)
	
	local key = extract_key(space, ...)
	
	--local shardno = curr_valid(space, unpack(key))
	--local tnt = connection(shardno, 'rw')
	
	if shardno ~= me or mode ~= 'rw' then
	if not numbers then space = tostring(space) end
	
	if ttl <= 0 then
                        errorf(
                            "Can't redirect request to shard %d (ttl=0)",
                            shardno
                        )
                    end
                    ttl = tostring(ttl - 1)
                    return
                        tnt:call('box.shard.internal.replace', ttl, space, ...)
                end

                return box.replace(space, ...)
end


return M