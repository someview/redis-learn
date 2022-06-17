local r={0,0}                                      --定义表变量
local MaxOfflineChatMsgCount=tonumber(ARGV[2])
local count=redis.call('LLEN',KEYS[2])              --list中待发消息的长度
 if count == 0 then                                  --原本无待发消息,本次添加后将需要通知
    local nodeCode=redis.call('HGET',KEYS[1],'1')	--在hash中得到下发人的上线连接器
    r[1]=nodeCode and tonumber(nodeCode) or 0		--有节点码,记录nodeCode,数组下标为manId的索引值(0开始)                     
  end 
  if count >= MaxOfflineChatMsgCount then				--太长溢出了,不存入
    	r[2]=1  										--溢出标志
        return r
   end                    
   redis.call('RPUSH',KEYS[2],ARGV[1])                 --在list中右边放入msg                
   return r
