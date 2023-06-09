<?php
namespace Yng\Queue\Queue\Job;

use Yng\App;
use Yng\Queue\Queue\Connector\Redis as RedisQueue;
use Yng\Queue\Queue\Job;

class Redis extends Job
{

    /**
     * redis队列实例
     * @var RedisQueue
     */
    protected $redis;

    /**
     * 数据库作业负载
     * @var Object
     */
    protected $job;

    /**
     * 保留队列中的 Redis 队列任务负载
     *
     * @var string
     */
    protected $reserved;

    public function __construct(App $app, RedisQueue $redis, $job, $reserved, $connection, $queue)
    {
        $this->app        = $app;
        $this->job        = $job;
        $this->queue      = $queue;
        $this->connection = $connection;
        $this->redis      = $redis;
        $this->reserved   = $reserved;
    }

    /**
     * 获取队列任务重试的最大次数
     * @return int
     */
    public function attempts()
    {
        return $this->payload('attempts') + 1;
    }

    /**
     * 获取任务的原始正文字符串
     * @return string
     */
    public function getRawBody()
    {
        return $this->job;
    }

    /**
     * 删除任务
     *
     * @return void
     */
    public function delete()
    {
        parent::delete();

        $this->redis->deleteReserved($this->queue, $this);
    }

    /**
     * 重新发布任务
     *
     * @param int $delay
     * @return void
     */
    public function release($delay = 0)
    {
        parent::release($delay);

        $this->redis->deleteAndRelease($this->queue, $this, $delay);
    }

    /**
     * 获取队列任务的id
     *
     * @return string
     */
    public function getJobId()
    {
        return $this->payload('id');
    }

    /**
     * 获取底层保留的 Redis 作业
     * Get the underlying reserved Redis job.
     *
     * @return string
     */
    public function getReservedJob()
    {
        return $this->reserved;
    }
}
