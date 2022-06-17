local function luaFuncLoadMsgs(r,count,MaxTotalSize)
	local len=0
	for i=1,count do 									    --max total msgs
		local msgs=redis.call('LRANGE',KEYS[2],i-1,i-1)     --在list中grab 1个msg                
    	len = len + #msgs[1]
		if len>MaxTotalSize then
			break
		end
		table.insert(r,msgs[1])
	end
end


local r={0,0}                                    --定义表变量
	local nh=redis.call('HMGET',KEYS[1],'1','2')	    	--在hash中得到下发人的上线连接器,handle
	r[2]=nh[2] and tonumber(nh[2]) or 0
	if nh[1] ~= ARGV[1] then
		r[1]=nh[1] and tonumber(nh[1]) or 0	
		return r
	end
    local count=redis.call('LLEN',KEYS[2])              --list中待发消息的数量
    if count==0 then
		return r
	end

	local ct=redis.call('HMGET',KEYS[1],'4','3')          --count,time
    if (ct[1] and tonumber(ct[1]) or 0)>0 
		and (ct[2] and tonumber(ct[2]) or 0)>=tonumber(ARGV[5]) then
		return r 												  --downing
	end
	luaFuncLoadMsgs(r,count>20 and 20 or count,tonumber(ARGV[3]))
    redis.call('HMSET',KEYS[1], '4',0,'5',ARGV[4],'6',ARGV[2])
    return r
