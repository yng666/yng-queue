<?php
namespace Yng\Queue\Queue\Job;

use Yng\App;
use Yng\Queue\Queue\Job;


class Sync extends Job
{
    /**
     * 队列消息数据
     *
     * @var string
     */
    protected $job;

    public function __construct(App $app, $job, $connection, $queue)
    {
        $this->app        = $app;
        $this->connection = $connection;
        $this->queue      = $queue;
        $this->job        = $job;
    }

    /**
     * 获取尝试作业的次数
     * @return int
     */
    public function attempts()
    {
        return 1;
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
     * 获取队列任务的id
     *
     * @return string
     */
    public function getJobId()
    {
        return '';
    }

    public function getQueue()
    {
        return 'sync';
    }
}
