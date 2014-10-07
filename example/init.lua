-- Config for each node

CONFDIR="/etc/mydatabase"
LIBDIR="/usr/lib/mydatabase/lua"

VERSION = 0.003
TIMEOUT = 1

if package and #LIBDIR > 0 then
	package.path = package.path..';'..LIBDIR..'/?.lua'
end

require 'box.connpool'
local log = require 'box.log'

local crc32 = require 'crc32'
local guava = require 'guava'

local condvar = require 'box.cv'

function hash(key,buckets)
	-- guava return value is in range [0..buckets)
	-- lua indexing is [1..buckets]
	-- so, return guava+1
	return guava(crc32(key),buckets)+1
end

local Servers = {}
Servers.my   = {
	{ -- shard 1
		{ 'rw',1,'127.7.1.0',33113 },
		{ 'ro',1,'127.7.1.1',33113 },
	},
	{ -- shard 2
		{ 'rw',1,'127.7.2.0',33113 },
		{ 'ro',1,'127.7.2.1',33113 },
	},
}

print("Servers: ",box.cjson.encode(Servers.my.raw))

local Sharding = require 'box.shard'

Sharding.configure({
	curr_list = Servers.my.curr;
	prev_list = Servers.my.prev;
	func = function(space, list, key)
		return hash(key,#list)
	end;
})

function lookup(email)
	return box.shard.select(0,email)
end

function resolve(email)
	local con = box.shard.conns:curr('rw', 0, email) -- active rw connect for given key
	local t
	if con then
		printf("%s: have connection to master. delegate",email)
		t = con:call('_master_resolve',email)
	else
		printf("%s: master not accessible. just read",email)
		t = box.shard.select(0,email)
	end
	if t then
		-- ...
		return t
	else
		return
	end
end

function _master_resolve(email)
	local t = box.update(0,email,"=p+p",F['atime'],math.floor(box.time()),F['acount'],1)
	if t then
		return t
	else
		return box.insert(0, 0,0,0,email,"R","",math.floor(box.time()),"")
	end
end

function delete (email)
	return box.shard.delete(0,email)
end
