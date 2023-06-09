<?php
declare (strict_types = 1);
namespace Yng\Queue\Queue;

/**
 * 定义Queue的一些常用方法
 */
trait Queueable
{

    /** @var string 连接 */
    public $connection;

    /** @var string 队列名称 */
    public $queue;

    /** @var integer 延迟时间 */
    public $delay;

    /**
     * 设置连接名
     * @param $connection
     * @return $this
     */
    public function onConnection($connection)
    {
        $this->connection = $connection;

        return $this;
    }

    /**
     * 设置队列名
     * @param $queue
     * @return $this
     */
    public function onQueue($queue)
    {
        $this->queue = $queue;

        return $this;
    }

    /**
     * 设置延迟时间
     * @param $delay
     * @return $this
     */
    public function delay($delay)
    {
        $this->delay = $delay;

        return $this;
    }
}
