env = require('test_run')
test_run = env.new()
test_run:cmd("create server master1 with script='node_down/master1.lua', lua_libs='node_down/lua/shard.lua'")
test_run:cmd("start server master1")
shard.wait_connection()

-- bipahse operations
batch = shard.q_begin()
batch.demo:q_insert(1, {0, 'test'})
batch.demo:q_replace(2, {0, 'test2'})
batch.demo:q_update(3, 0, {{'=', 2, 'test3'}})
batch.demo:q_insert(4, {1, 'test4'})
batch.demo:q_insert(5, {2, 'test_to_delete'})
batch.demo:q_delete(6, 2)
batch:q_end()

_ = test_run:cmd("stop server master1")

shard.wait_operations()
box.space.demo:select()
box.space.operations:select()

-- check for operation q_insert is in shard
shard.demo:check_operation(6, 0)
-- check for not exists operations
shard.demo:check_operation('12345', 0)

test_run:cmd("cleanup server master1")
test_run:cmd("restart server default with cleanup=1")
