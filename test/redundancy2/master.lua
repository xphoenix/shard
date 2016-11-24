#!/usr/bin/env tarantool
local os = require('os')
local fiber = require('fiber')

shard = require('shard')

local cfg = {
    servers = {
        { uri = 'localhost:33130', zone = '0' };
        { uri = 'localhost:33131', zone = '1' };
        { uri = 'localhost:33132', zone = '2' };
    };
    login = 'tester';
    password = 'pass';
    redundancy = 2;
    binary = 33130;
}

box.cfg {
    slab_alloc_arena = 0.1;
    wal_mode = 'none';
    listen = cfg.binary;
    custom_proc_title  = "master"
}

require('console').listen(os.getenv('ADMIN'))

if not box.space.demo then
    box.schema.user.create(cfg.login, { password = cfg.password })
    box.schema.user.grant(cfg.login, 'read,write,execute', 'universe')

    local demo = box.schema.create_space('demo')
    demo:create_index('primary', {type = 'tree', parts = {1, 'num'}})
end

function print_shard_map()
    local result = {}
    for uri, hb_table in pairs(shard.get_heartbeat()) do
        table.insert(result, uri)
        for server, data in pairs(hb_table) do
            table.insert(result, server)
            table.insert(result, data.try)
        end
    end
    return result
end

-- init shards
fiber.create(function()
    shard.init(cfg)
end)

