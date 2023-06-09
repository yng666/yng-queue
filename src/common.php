<?php
declare (strict_types = 1);
use Yng\Facade\Queue;

if (!function_exists('queue')) {

    /**
     * 添加到队列
     * @param        $job   队列实例
     * @param string $data  数据
     * @param int    $delay 延迟
     * @param null   $queue 队列别名
     */
    function queue($job, $data = '', $delay = 0, $queue = null)
    {
        if ($delay > 0) {
            Queue::later($delay, $job, $data, $queue);
        } else {
            Queue::push($job, $data, $queue);
        }
    }
}
