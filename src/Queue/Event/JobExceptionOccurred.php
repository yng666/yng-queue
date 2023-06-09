<?php
namespace Yng\Queue\Queue\Event;

use Exception;
use Yng\Queue\Queue\Job;

class JobExceptionOccurred
{
    /**
     * 连接名
     *
     * @var string
     */
    public $connectionName;

    /**
     * job实例
     *
     * @var Job
     */
    public $job;

    /**
     * 队列异常
     *
     * @var Exception
     */
    public $exception;

    /**
     * 创建一个新的事件实例
     *
     * @param string    $connectionName
     * @param Job       $job
     * @param Exception $exception
     * @return void
     */
    public function __construct($connectionName, $job, $exception)
    {
        $this->job            = $job;
        $this->exception      = $exception;
        $this->connectionName = $connectionName;
    }
}
