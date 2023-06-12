<?php
namespace Yng\Queue\Failed;

use Yng\Queue\FailedJob;

/**
 * 空的失败类
 */
class None extends FailedJob
{

    /**
     * 将失败的队列任务记录到存储中
     *
     * @param string     $connection 连接名称 
     * @param string     $queue 队列名称 
     * @param string     $payload 任务负载 
     * @param \Exception $exception 异常信息 
     */
    public function log($connection, $queue, $payload, $exception)
    {

    }

    /**
     * 获取所有失败的任务
     *
     * @return array
     */
    public function all()
    {
        return [];
    }

    /**
     * 获取单个失败任务
     *
     * @param mixed $id
     */
    public function find($id)
    {

    }

    /**
     * 从存储中删除单个失败任务
     *
     * @param mixed $id
     * @return bool
     */
    public function forget($id)
    {
        return true;
    }

    /**
     * 从存储中删除所有失败任务
     *
     * @return void
     */
    public function flush()
    {

    }
}
