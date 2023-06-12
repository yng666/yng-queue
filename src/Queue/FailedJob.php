<?php
declare (strict_types = 1);
namespace Yng\Queue;


/**
 * 队列失败类
 */
abstract class FailedJob
{
    /**
     * 将失败的队列任务记录到存储中
     *
     * @param string     $connection
     * @param string     $queue
     * @param string     $payload
     * @param \Exception $exception
     * @return int|null
     */
    abstract public function log($connection, $queue, $payload, $exception);

    /**
     * 获取全部失败队列任务
     *
     * @return array
     */
    abstract public function all();

    /**
     * 获取指定一个失败的队列任务
     *
     * @param mixed $id
     * @return object|null
     */
    abstract public function find($id);

    /**
     * 从缓存中删除指定一个失败的队列任务
     *
     * @param mixed $id
     * @return bool
     */
    abstract public function forget($id);

    /**
     * 从缓存中删除所有失败的队列任务
     *
     * @return void
     */
    abstract public function flush();
}
