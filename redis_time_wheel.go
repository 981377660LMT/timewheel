// 基于 redis 实现分布式版本的时间轮
//
// 使用 redis 中的有序集合 sorted set（简称 zset） 进行定时任务的存储管理.
// 其中以每个定时任务执行时间对应的时间戳作为 zset 中的 score，完成定时任务的有序排列组合.
//
// 1. 分钟级时间分片，避免产生 redis 大 key 问题；
// 2. 惰性删除机制，用一个set集合存储已删除的任务，每次执行任务时，先检查是否已被删除。

package timewheel

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/demdxx/gocast"

	thttp "github.com/xiaoxuxiansheng/timewheel/pkg/http"
	"github.com/xiaoxuxiansheng/timewheel/pkg/redis"
	"github.com/xiaoxuxiansheng/timewheel/pkg/util"
)

type RTaskElement struct {
	Key string `json:"key"`

	CallbackURL string            `json:"callback_url"` // 定时任务执行时，回调的 http url
	Method      string            `json:"method"`
	Req         interface{}       `json:"req"`
	Header      map[string]string `json:"header"`
}

type RTimeWheel struct {
	sync.Once // 用于保证 stopc 只被关闭一次

	redisClient *redis.Client // 定时任务的存储是基于 redis zset 实现的
	httpClient  *thttp.Client // 定时任务执行时，是通过请求使用方预留回调地址的方式实现的

	stopc  chan struct{} // 用于停止时间轮的控制器 channel
	ticker *time.Ticker  // 触发定时扫描任务的定时器
}

func NewRTimeWheel(redisClient *redis.Client, httpClient *thttp.Client) *RTimeWheel {
	r := RTimeWheel{
		redisClient: redisClient,
		httpClient:  httpClient,
		stopc:       make(chan struct{}),
		ticker:      time.NewTicker(time.Second),
	}

	go r.run()
	return &r
}

func (r *RTimeWheel) Stop() {
	r.Do(func() {
		close(r.stopc)
		r.ticker.Stop()
	})
}

func (r *RTimeWheel) AddTask(ctx context.Context, key string, task *RTaskElement, executeAt time.Time) error {
	if err := r.addTaskPrecheck(task); err != nil {
		return err
	}

	task.Key = key
	taskBody, _ := json.Marshal(task)
	_, err := r.redisClient.Eval(ctx, LuaAddTasks, 2, []interface{}{
		// 分钟级 zset 时间片
		r.getMinuteSlice(executeAt),
		// 标识任务删除的集合
		r.getDeleteSetKey(executeAt),
		// 以执行时刻的秒级时间戳作为 zset 中的 score
		executeAt.Unix(),
		// 任务明细
		string(taskBody),
		// 任务 key，用于存放在删除集合中
		key,
	})
	return err
}

// 将定时任务追加到分钟级的已删除任务 set 中. 之后在检索定时任务时，会根据这个 set 对定时任务进行过滤，实现惰性删除机制
func (r *RTimeWheel) RemoveTask(ctx context.Context, key string, executeAt time.Time) error {
	// 标识任务已被删除
	_, err := r.redisClient.Eval(ctx, LuaDeleteTask, 1, []interface{}{
		r.getDeleteSetKey(executeAt),
		key,
	})
	return err
}

func (r *RTimeWheel) run() {
	for {
		select {
		case <-r.stopc:
			return
		case <-r.ticker.C:
			// 每次 tick 获取任务
			go r.executeTasks()
		}
	}
}

func (r *RTimeWheel) executeTasks() {
	defer func() {
		if err := recover(); err != nil {
			// log
		}
	}()

	// 并发控制，保证 30 s 之内完成该批次全量任务的执行，及时回收 goroutine，避免发生 goroutine 泄漏
	tctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	// 根据当前时间条件扫描 redis zset，获取所有满足执行条件的定时任务
	tasks, err := r.getExecutableTasks(tctx)
	if err != nil {
		// log
		return
	}

	// 并发执行任务，通过 waitGroup 进行聚合收口
	var wg sync.WaitGroup
	for _, task := range tasks {
		wg.Add(1)
		// shadow
		task := task
		go func() {
			defer func() {
				if err := recover(); err != nil {
				}
				wg.Done()
			}()
			// 执行定时任务
			if err := r.executeTask(tctx, task); err != nil {
				// log
			}
		}()
	}
	wg.Wait()
}

func (r *RTimeWheel) executeTask(ctx context.Context, task *RTaskElement) error {
	return r.httpClient.JSONDo(ctx, task.Method, task.CallbackURL, task.Header, task.Req, nil)
}

func (r *RTimeWheel) addTaskPrecheck(task *RTaskElement) error {
	if task.Method != http.MethodGet && task.Method != http.MethodPost {
		return fmt.Errorf("invalid method: %s", task.Method)
	}
	if !strings.HasPrefix(task.CallbackURL, "http://") && !strings.HasPrefix(task.CallbackURL, "https://") {
		return fmt.Errorf("invalid url: %s", task.CallbackURL)
	}
	return nil
}

// !检索定时任务
func (r *RTimeWheel) getExecutableTasks(ctx context.Context) ([]*RTaskElement, error) {
	now := time.Now()
	minuteSlice := r.getMinuteSlice(now)
	deleteSetKey := r.getDeleteSetKey(now)
	nowSecond := util.GetTimeSecond(now)
	score1 := nowSecond.Unix()
	score2 := nowSecond.Add(time.Second).Unix()
	rawReply, err := r.redisClient.Eval(ctx, LuaZrangeTasks, 2, []interface{}{
		minuteSlice, deleteSetKey, score1, score2,
	})
	if err != nil {
		return nil, err
	}

	replies := gocast.ToInterfaceSlice(rawReply) // 0: 已删除任务集合，1: 定时任务明细
	if len(replies) == 0 {
		return nil, fmt.Errorf("invalid replies: %v", replies)
	}

	deleteds := gocast.ToStringSlice(replies[0])
	deletedSet := make(map[string]struct{}, len(deleteds))
	for _, deleted := range deleteds {
		deletedSet[deleted] = struct{}{}
	}

	tasks := make([]*RTaskElement, 0, len(replies)-1)
	for i := 1; i < len(replies); i++ {
		var task RTaskElement
		if err := json.Unmarshal([]byte(gocast.ToString(replies[i])), &task); err != nil {
			// log
			continue
		}

		if _, ok := deletedSet[task.Key]; ok {
			continue
		}
		tasks = append(tasks, &task)
	}

	return tasks, nil
}

// 通过以分钟级表达式作为 {hash_tag} 的方式，确保 minuteSlice 和 deleteSet 一定会分发到相同的 redis 节点之上，进一步保证 lua 脚本的原子性能够生效
func (r *RTimeWheel) getMinuteSlice(executeAt time.Time) string {
	return fmt.Sprintf("xiaoxu_timewheel_task_{%s}", util.GetTimeMinuteStr(executeAt))
}

func (r *RTimeWheel) getDeleteSetKey(executeAt time.Time) string {
	return fmt.Sprintf("xiaoxu_timewheel_delset_{%s}", util.GetTimeMinuteStr(executeAt))
}
