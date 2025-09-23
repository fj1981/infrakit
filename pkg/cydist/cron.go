package cydist

import (
	"context"
	"sync"
	"time"

	"github.com/fj1981/infrakit/pkg/cylog"
	"github.com/fj1981/infrakit/pkg/cyutil"
	"github.com/robfig/cron/v3"
)

// Task 定义一个可调度的任务
type Task struct {
	ID      string                      // 任务唯一ID
	Spec    string                      // Cron 表达式，如 "0 2 * * *"
	Handler func(context.Context) error // 任务执行函数
	Timeout time.Duration               // 执行超时时间
}

// DistributedScheduler 分布式调度器
type DistributedScheduler struct {
	cron       *cron.Cron
	locker     *DistLockManager
	tasks      map[string]cron.EntryID // taskID -> cron entry ID
	tasksMu    sync.RWMutex
	running    sync.Map // taskID -> context.CancelFunc
	instanceID string   // 当前实例标识
	stopChan   chan struct{}
	closeOnce  sync.Once
}

// NewDistributedScheduler 创建调度器
// 注意：DistLockManager 应由外部创建并注入
func NewDistributedScheduler(locker *DistLockManager, instanceID string) *DistributedScheduler {
	if instanceID == "" {
		instanceID = cyutil.XID()
	}

	return &DistributedScheduler{
		cron:       cron.New(cron.WithChain(cron.Recover(cron.DefaultLogger))),
		locker:     locker,
		tasks:      make(map[string]cron.EntryID),
		instanceID: instanceID,
		stopChan:   make(chan struct{}),
	}
}

// AddTask 添加一个分布式任务
func (s *DistributedScheduler) AddTask(task *Task) (cron.EntryID, error) {
	if task.Timeout <= 0 {
		task.Timeout = 30 * time.Second
	}

	entryID, err := s.cron.AddFunc(task.Spec, func() {
		s.runTaskWithLock(task)
	})

	if err != nil {
		return 0, err
	}

	s.tasksMu.Lock()
	s.tasks[task.ID] = entryID
	s.tasksMu.Unlock()

	return entryID, nil
}

// runTaskWithLock 使用分布式锁确保任务不重复执行
func (s *DistributedScheduler) runTaskWithLock(task *Task) {
	lockKey := "cron:lock:" + task.ID

	// 尝试获取锁，最多等待 1 秒
	distLock, err := s.locker.Lock(
		context.Background(),
		lockKey,
		WithLockTimeout(1*time.Second),
	)
	if err != nil {
		cylog.Skip(0).Warnf("[Scheduler:%s] 跳过任务 %s: 未获取到锁（其他节点正在执行）", s.instanceID, task.ID)
		return
	}

	cylog.Skip(0).Infof("[Scheduler:%s] 成功获取锁，开始执行任务: %s", s.instanceID, task.ID)

	// 创建带超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), task.Timeout)
	defer cancel()

	// 标记为运行中，支持外部取消
	s.running.Store(task.ID, cancel)
	defer s.running.Delete(task.ID)

	// 执行业务逻辑
	err = task.Handler(ctx)
	if err != nil {
		cylog.Skip(0).Warnf("[Scheduler:%s] 任务 %s 执行失败: %v", s.instanceID, task.ID, err)
	} else {
		cylog.Skip(0).Infof("[Scheduler:%s] 任务 %s 执行成功", s.instanceID, task.ID)
	}

	// 释放锁
	unlocked, unlockErr := distLock.Unlock()
	if !unlocked {
		cylog.Skip(0).Warnf("[Scheduler:%s] 警告: 释放锁失败 %s: %v", s.instanceID, task.ID, unlockErr)
	}
}

// Start 启动调度器
func (s *DistributedScheduler) Start() {
	cylog.Skip(0).Infof("[Scheduler:%s] 分布式调度器启动", s.instanceID)
	s.cron.Start()
}

// Stop 停止调度器
func (s *DistributedScheduler) Stop() {
	s.closeOnce.Do(func() {
		close(s.stopChan)
		ctx := s.cron.Stop()
		<-ctx.Done()
		cylog.Skip(0).Infof("[Scheduler:%s] 调度器已停止", s.instanceID)
	})
}

// RemoveTask 删除任务
func (s *DistributedScheduler) RemoveTask(taskID string) bool {
	s.tasksMu.Lock()
	defer s.tasksMu.Unlock()
	if entryID, exists := s.tasks[taskID]; exists {
		s.cron.Remove(entryID)
		delete(s.tasks, taskID)
		return true
	}
	return false
}

// StopRunningTask 取消正在运行的任务
func (s *DistributedScheduler) StopRunningTask(taskID string) bool {
	if cancel, ok := s.running.Load(taskID); ok {
		cancel.(context.CancelFunc)()
		cylog.Skip(0).Infof("[Scheduler:%s] 已取消任务: %s", s.instanceID, taskID)
		return true
	}
	return false
}

// ListTasks 获取所有任务
func (s *DistributedScheduler) ListTasks() map[string]string {
	s.tasksMu.RLock()
	defer s.tasksMu.RUnlock()
	m := make(map[string]string)
	for taskID, entryID := range s.tasks {
		m[taskID] = cyutil.ToStr(entryID)
	}
	return m
}
