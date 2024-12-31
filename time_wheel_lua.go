package timewheel

const (
	// 1 添加任务时，如果存在删除 key 的标识，则将其删除
	// 添加任务时，根据时间（所属的 min）决定数据从属于哪个分片{}
	LuaAddTasks = `
       -- 获取的首个 key 为 zset 的 key
       local zsetKey = KEYS[1]
       -- 获取的第二个 key 为标识已删除任务 set 的 key
       local deleteSetKey = KEYS[2]
       -- 获取的第一个 arg 为定时任务在 zset 中的 score
       local score = ARGV[1]
       -- 获取的第二个 arg 为定时任务明细数据
       local task = ARGV[2]
       -- 获取的第三个 arg 为定时任务唯一键，用于将其从已删除任务 set 中移除
       local taskKey = ARGV[3]
       -- 每次添加定时任务时，都直接将其从已删除任务 set 中移除，不管之前是否在 set 中
       redis.call('srem',deleteSetKey,taskKey)
       -- 调用 zadd 指令，将定时任务添加到 zset 中
       return redis.call('zadd',zsetKey,score,task)
    `

	// 2 删除任务时，将删除 key 的标识置为 true
	// !通过为 deleteSetKey 集合设置 120 秒的过期时间，可以确保在没有新的删除任务添加时，这个集合不会永久存在于 Redis 中，避免内存被无用数据占用
	LuaDeleteTask = `
       -- 获取标识删除任务的 set 集合的 key
       local deleteSetKey = KEYS[1]
       -- 获取定时任务的唯一键
       local taskKey = ARGV[1]
       -- 将定时任务唯一键添加到 set 中
       redis.call('sadd',deleteSetKey,taskKey)
       -- 倘若是 set 中的首个元素，则对 set 设置 120 s 的过期时间
       local scnt = redis.call('scard',deleteSetKey)
       if (tonumber(scnt) == 1)
       then
           redis.call('expire',deleteSetKey,120)
       end
       return scnt
)    `

	// 3 执行任务时，通过 zrange 操作取回所有不存在删除 key 标识的任务
	// 扫描 redis 时间轮. 获取分钟范围内,已删除任务集合 以及在时间上达到执行条件的定时任务进行返回
	LuaZrangeTasks = `
       -- 第一个 key 为存储定时任务的 zset key
       local zsetKey = KEYS[1]
       -- 第二个 key 为已删除任务 set 的 key
       local deleteSetKey = KEYS[2]
       -- 第一个 arg 为 zrange 检索的 score 左边界
       local score1 = ARGV[1]
       -- 第二个 arg 为 zrange 检索的 score 右边界
       local score2 = ARGV[2]
       -- 获取到已删除任务的集合
       local deleteSet = redis.call('smembers',deleteSetKey)
       -- 根据秒级时间戳对 zset 进行 zrange 检索，获取到满足时间条件的定时任务
       local targets = redis.call('zrange',zsetKey,score1,score2,'byscore')
       -- 检索到的定时任务直接从时间轮中移除，保证分布式场景下定时任务不被重复获取
       redis.call('zremrangebyscore',zsetKey,score1,score2)
       -- 返回的结果是一个 table
       local reply = {}
       -- table 的首个元素为已删除任务集合
       reply[1] = deleteSet
       -- 依次将检索到的定时任务追加到 table 中
       for i, v in ipairs(targets) do
           reply[#reply+1]=v
       end
       return reply
    `
)
