env = require('test_run')
test_run = env.new()
test_run:cmd("create server master1 with script='redundancy3/master1.lua', lua_libs='redundancy3/lua/shard.lua'")
test_run:cmd("create server master2 with script='redundancy3/master2.lua', lua_libs='redundancy3/lua/shard.lua'")
test_run:cmd("start server master1")
test_run:cmd("start server master2")
shard.wait_connection()

shard.multipart:insert{3, "hello", 1}
shard.multipart:insert{4, "hello", 1}
shard.multipart:insert{5, "hello", 1}
shard.multipart:insert{3, "hello1", 1}
shard.multipart:insert{4, "hello1", 1}
shard.multipart:insert{5, "hello1", 1}

shard.multipart:select{"hello", 3}
shard.multipart:select{"hello", 5}

_ = test_run:cmd("stop server master1")
_ = test_run:cmd("stop server master2")
test_run:cmd("cleanup server master1")
test_run:cmd("cleanup server master2")
test_run:cmd("restart server default with cleanup=1")
