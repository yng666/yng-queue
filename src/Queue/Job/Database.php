<?php
namespace Yng\Queue\Job;

use Yng\App;
use Yng\Queue\Connector\Database as DatabaseQueue;
use Yng\Queue\Job;

class Database extends Job
{
    /**
     * 数据库队列实例
     * @var DatabaseQueue
     */
    protected $database;

    /**
     * 数据库作业负载
     * @var Object
     */
    protected $job;

    public function __construct(App $app, DatabaseQueue $database, $job, $connection, $queue)
    {
        $this->app        = $app;
        $this->job        = $job;
        $this->queue      = $queue;
        $this->database   = $database;
        $this->connection = $connection;
    }

    /**
     * 删除任务
     * @return void
     */
    public function delete()
    {
        parent::delete();
        $this->database->deleteReserved($this->job->id);
    }

    /**
     * 重新发布任务
     * @param int $delay
     * @return void
     */
    public function release($delay = 0)
    {
        parent::release($delay);

        $this->delete();

        $this->database->release($this->queue, $this->job, $delay);
    }

    /**
     * 获取当前任务尝试次数
     * @return int
     */
    public function attempts()
    {
        return (int) $this->job->attempts;
    }

    /**
     * 获取任务的原始正文字符串
     * @return string
     */
    public function getRawBody()
    {
        return $this->job->payload;
    }

    /**
     * 获取队列任务的id
     *
     * @return string
     */
    public function getJobId()
    {
        return $this->job->id;
    }
}
