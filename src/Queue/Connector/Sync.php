<?php
namespace Yng\Queue\Queue\Connector;

use Exception;
use Yng\Queue\Queue\Connector;
use Yng\Queue\Queue\Event\JobFailed;
use Yng\Queue\Queue\Event\JobProcessed;
use Yng\Queue\Queue\Event\JobProcessing;
use Yng\Queue\Queue\Job\Sync as SyncJob;
use Throwable;


/**
 * 处理同步队列
 */
class Sync extends Connector
{

    /**
     * 获取队列任务数量
     */
    public function size($queue = null)
    {
        return 0;
    }

    /**
     * 将任务推送到队列中
     */
    public function push($job, $data = '', $queue = null)
    {
        // 创建队列任务
        $queueJob = $this->resolveJob($this->createPayload($job, $data), $queue);

        try {
            // 触发 JobProcessing 事件
            $this->triggerEvent(new JobProcessing($this->connection, $job));

            // 执行队列任务
            $queueJob->fire();

            $this->triggerEvent(new JobProcessed($this->connection, $job));
        } catch (Exception | Throwable $e) {

            $this->triggerEvent(new JobFailed($this->connection, $job, $e));

            throw $e;
        }

        return 0;
    }

    /**
     * 触发事件
     */
    protected function triggerEvent($event)
    {
        $this->app->event->trigger($event);
    }

    public function pop($queue = null)
    {
        // 该方法不需要实现，因为 Sync 队列是同步的，任务会立即执行
    }

    /**
     * 创建SyncJob实例对象
     */
    protected function resolveJob($payload, $queue)
    {
        return new SyncJob($this->app, $payload, $this->connection, $queue);
    }

    /**
     * 处理同步队列
     * @param  mixed  $payload
     * @param  string|null  $queue
     * @param  array  $options
     * @return void
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        // 该方法不需要实现，因为 Sync 队列只支持推送 Job 对象
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        // Sync 队列不支持延迟任务，所以直接调用 push 方法
        return $this->push($job, $data, $queue);
    }
}
