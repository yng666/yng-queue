<?php

namespace Yng\Queue\Event;

class WorkerStopping
{
    /**
     * 退出状态
     *
     * @var int
     */
    public $status;

    /**
     * 创建一个新的事件实例
     *
     * @param int $status
     * @return void
     */
    public function __construct($status = 0)
    {
        $this->status = $status;
    }
}
