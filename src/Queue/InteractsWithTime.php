<?php
declare (strict_types = 1);
namespace Yng\Queue\Queue;


use Carbon\Carbon;
use DateInterval;
use DateTimeInterface;

trait InteractsWithTime
{
    /**
     * 获取距离给定DateTime的秒数
     *
     * @param DateTimeInterface|DateInterval|int $delay 延迟的时间，可以是DateTimeInterface实例、DateInterval实例或整数
     * @return int 返回距离给定时间的秒数
     */
    protected function secondsUntil($delay)
    {
        $delay = $this->parseDateInterval($delay);
        return $delay instanceof DateTimeInterface ? max(0, $delay->getTimestamp() - $this->currentTime()) : (int) $delay;
    }

    /**
     * 获取给定延迟时间的"available at" UNIX时间戳。
     *
     * @param DateTimeInterface|DateInterval|int $delay 延迟的时间，可以是DateTimeInterface实例、DateInterval实例或整数
     * @return int 返回"available at" UNIX时间戳
     */
    protected function availableAt($delay = 0)
    {
        $delay = $this->parseDateInterval($delay);
        return $delay instanceof DateTimeInterface ? $delay->getTimestamp() : Carbon::now()->addRealSeconds($delay)->getTimestamp();
    }

    /**
     * 如果给定的值是一个时间间隔，将其转换为DateTime实例。
     *
     * @param DateTimeInterface|DateInterval|int $delay 延迟的时间，可以是DateTimeInterface实例、DateInterval实例或整数
     * @return DateTimeInterface|int 返回DateTimeInterface实例或整数
     */
    protected function parseDateInterval($delay)
    {
        if ($delay instanceof DateInterval) {
            $delay = Carbon::now()->add($delay);
        }
        return $delay;
    }

    /**
     * 获取当前系统时间的UNIX时间戳。
     *
     * @return int 返回当前系统时间的UNIX时间戳
     */
    protected function currentTime()
    {
        return Carbon::now()->getTimestamp();
    }
}