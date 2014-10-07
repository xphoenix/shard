local ffi = require('ffi')
local tonumber = tonumber

module(...)

local zlib = ffi.load('z')
ffi.cdef[[
    unsigned long crc32(unsigned long crc, const char *buf, unsigned len );
]]

local function crc32(data)
  return tonumber(zlib.crc32(0, data, #data))
end

return crc32
