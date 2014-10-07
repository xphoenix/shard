local object = require 'box.oop'
local log    = require 'box.log'
local table  = require 'table'
local error  = _G.error
local pairs  = _G.pairs
local type   = _G.type

local prefix = string.match(...,'(.+)%.[^.]+$')
local dumper = require(prefix..'.func').dumper
--module(...)

local M = object:define({ __name = ... })
--box.shard.schema1 = M

local function check_shardlist(list)
	for i, sh in pairs(list) do
		if type(sh) ~= 'table' then
			error("Each shard must be described by 'table of table'")
		end
		
		if #sh > 0 and type(sh[1]) ~= 'table' then
			error("Each host must be described by 'table'")
		end
		
		table.sort(sh,
			function(a, b)
				if a[1] == 'rw' then
					return true
				else
					return false
				end
			end
		)
		if #sh > 1 and sh[1][2] == 'rw' then
			error("Only one shard host can have 'rw' mode")
		end
	end
end


function M:init(opts)
	self:configure(opts)
end

function M:configure(opts)
	local cl,pl,cf,pf = opts.curr_list, opts.prev_list, opts.func or opts.curr_func, opts.prev_func
	
	if not cl or #cl < 1 then error("Non empty `curr_list' required") end
	if not cf then error("Defined `curr' function required") end
	
	self.cl = cl
	self.cf = cf
	
	self.curr = function(self,space,...)
		local shno = self.cf(space,self.cl,...)
		if shno > 0 and shno <= #self.cl then
			return shno
		else
			error("curr function returned wrong shard no: "..tostring(shno).."; avail range is: [1.."..tostring(#self.cl))
		end
	end
	
	if not pl and not pf then
		-- stable mode, no resharding
		check_shardlist(cl)
		self.mode = "stable"
		log.info("configured schema in mode %s, size=%d",self.mode, #cl)
	else
		self.mode = "resharding"
		if pl and not pf then
			-- TODO: check cl != pl
			check_shardlist(pl)
			pf = cf
		elseif pf and not pl then
			pl = cl
		end
		if #pl < 1 then error("Non empty `prev_list' required in resharding") end
		self.pl = pl
		self.pf = pf or error("Defined `prev' function required")
		
		self.prev = function(self,space,...)
			local shno = self.pf(space,self.pl,...)
			if shno > 0 and shno <= #self.pl then
				return shno
			else
				error("prev function returned wrong shard no: "..tostring(shno).."; avail range is: [1.."..tostring(#self.pl))
			end
		end
		log.info(
			"configured schema in mode %s, curr size=%d, prev size=%d; %s function",
			self.mode, #cl, #pl,
			cf == pf and "same" or "different"
		)
		
	end
end

function M:info()
	local src = {
		curr = self.cl;
		prev = self.pl;
	}
	local info = {}
	for mode,list in pairs(src) do
		info[mode] = {}
		print(mode..":\n")
		--info[mode] = {}
		for shno,shard in ipairs(list) do
			local rw,ro = {},{}
			for _,node in ipairs(shard) do
				local to = node[1] == 'rw' and rw or ro
				local nk = node[3]..':'..node[4]..(node[2] > 1 and '*'..node[2] or '')
				table.insert(to, nk)
			end
			info[mode][shno] = table.concat()
		end
	end
	--print
end

return M
