<?php
declare (strict_types = 1);
namespace Yng\Queue;

use Carbon\Carbon;
use Exception;
use RuntimeException;
use Yng\Cache;
use Yng\Event;
use Yng\Exception\Handle;
use Yng\Queue;
use Yng\Queue\Event\JobExceptionOccurred;
use Yng\Queue\Event\JobFailed;
use Yng\Queue\Event\JobProcessed;
use Yng\Queue\Event\JobProcessing;
use Yng\Queue\Event\WorkerStopping;
use Yng\Queue\Exception\MaxAttemptsExceededException;
use Throwable;


/**
 * 队列中监听任务之一
 * work 单进程的处理模式
 * 无 daemon 参数 work进程在处理完下一个消息后直接结束当前进程。当不存在新消息时，会sleep一段时间而后退出;
 * 有 daemon 参数 work进程会循环地处理队列中的消息，直到内存超出参数配置才结束进程。当不存在新消息时，会在每次循环中sleep一段时间
 */
class Worker
{
    /**
     * @var Event
     */
    protected $event;

    /**
     * @var Handle
     */
    protected $handle;

    /**
     * @var Queue
     */
    protected $queue;

    /**
     * @var Cache
     */
    protected $cache;

    /**
     * 队列工作器是否停止运行
     *
     * @var bool
     */
    public $shouldQuit = false;

    /**
     * 队列是否处于暂停状态
     *
     * @var bool
     */
    public $paused = false;

    public function __construct(Queue $queue, Event $event, Handle $handle, Cache $cache = null)
    {
        $this->queue  = $queue;
        $this->event  = $event;
        $this->handle = $handle;
        $this->cache  = $cache;
    }

    /**
     * 守护队列进程(有任务就执行) 
     * @param string $connection
     * @param string $queue
     * @param int    $delay
     * @param int    $sleep
     * @param int    $maxTries
     * @param int    $memory
     * @param int    $timeout
     */
    public function daemon($connection, $queue, $delay = 0, $sleep = 3, $maxTries = 0, $memory = 128, $timeout = 60)
    {
        if ($this->supportsAsyncSignals()) {
            $this->listenForSignals();
        }

        // 获取队列重启时间
        $lastRestart = $this->getTimestampOfLastQueueRestart();

        while (true) {

            // 获取下个任务
            $job = $this->getNextJob(
                $this->queue->connection($connection),
                $queue
            );

            // 判断是否支持异步信号
            if ($this->supportsAsyncSignals()) {
                $this->registerTimeoutHandler($job, $timeout);
            }

            if ($job) {
                $this->runJob($job, $connection, $maxTries, $delay);
            } else {
                $this->sleep($sleep);
            }

            // 判断队列工作器是否应该停止运行
            $this->stopIfNecessary($job, $lastRestart, $memory);
        }
    }


    /**
     * 检测队列工作器是否应该停止运行
     */
    protected function stopIfNecessary($job, $lastRestart, $memory)
    {
        if ($this->shouldQuit || $this->queueShouldRestart($lastRestart)) {
            $this->stop();
        } elseif ($this->memoryExceeded($memory)) {
            $this->stop(12);
        }
    }

    /**
     * 确定队列工作者是否应该重新启动
     *
     * @param int|null $lastRestart
     * @return bool
     */
    protected function queueShouldRestart($lastRestart)
    {
        return $this->getTimestampOfLastQueueRestart() != $lastRestart;
    }

    /**
     * 判断是否超出内存限制
     *
     * @param int $memoryLimit
     * @return bool
     */
    public function memoryExceeded($memoryLimit)
    {
        return (memory_get_usage(true) / 1024 / 1024) >= $memoryLimit;
    }

    /**
     * 获取队列重启时间
     * @return mixed
     */
    protected function getTimestampOfLastQueueRestart()
    {
        if ($this->cache) {
            return $this->cache->get('Yng:queue:restart');
        }
    }

    /**
     * 注册超时处理程序
     *
     * @param Job|null $job
     * @param int      $timeout
     * @return void
     */
    protected function registerTimeoutHandler($job, $timeout)
    {
        pcntl_signal(SIGALRM, function () {
            $this->kill(1);
        });

        pcntl_alarm(
            max($this->timeoutForJob($job, $timeout), 0)
        );
    }

    /**
     * 停止队列任务监听并退出脚本
     *
     * @param int $status
     * @return void
     */
    public function stop($status = 0)
    {
        $this->event->trigger(new WorkerStopping($status));

        exit($status);
    }

    /**
     * 终止队列任务
     *
     * @param int $status
     * @return void
     */
    public function kill($status = 0)
    {
        $this->event->trigger(new WorkerStopping($status));

        if (extension_loaded('posix')) {
            posix_kill(getmypid(), SIGKILL);
        }

        exit($status);
    }

    /**
     * 获取队列中给定的任务的最大执行时间
     *
     * @param Job|null $job
     * @param int      $timeout
     * @return int
     */
    protected function timeoutForJob($job, $timeout)
    {
        return $job && !is_null($job->timeout()) ? $job->timeout() : $timeout;
    }

    /**
     * 判断是否支持异步信号
     *
     * @return bool
     */
    protected function supportsAsyncSignals()
    {
        return extension_loaded('pcntl');
    }

    /**
     * 为进程启用异步信号
     *
     * @return void
     */
    protected function listenForSignals()
    {
        pcntl_async_signals(true);

        pcntl_signal(SIGTERM, function () {
            $this->shouldQuit = true;
        });

        pcntl_signal(SIGUSR2, function () {
            $this->paused = true;
        });

        pcntl_signal(SIGCONT, function () {
            $this->paused = false;
        });
    }

    /**
     * 执行下个任务
     * @param string $connection
     * @param string $queue
     * @param int    $delay
     * @param int    $sleep
     * @param int    $maxTries
     * @return void
     * @throws Exception
     */
    public function runNextJob($connection, $queue, $delay = 0, $sleep = 3, $maxTries = 0)
    {

        $job = $this->getNextJob($this->queue->connection($connection), $queue);

        if ($job) {
            $this->runJob($job, $connection, $maxTries, $delay);
        } else {
            $this->sleep($sleep);
        }
    }

    /**
     * 执行任务
     * @param Job    $job
     * @param string $connection
     * @param int    $maxTries
     * @param int    $delay
     * @return void
     */
    protected function runJob($job, $connection, $maxTries, $delay)
    {
        try {
            $this->process($connection, $job, $maxTries, $delay);
        } catch (Exception | Throwable $e) {
            $this->handle->report($e);
        }
    }

    /**
     * 获取下个任务
     * @param Connector $connector
     * @param string    $queue
     * @return Job
     */
    protected function getNextJob($connector, $queue)
    {
        try {
            foreach (explode(',', $queue) as $queue) {
                if (!is_null($job = $connector->pop($queue))) {
                    return $job;
                }
            }
        } catch (Exception | Throwable $e) {
            $this->handle->report($e);
            $this->sleep(1);
        }
    }

    /**
     * 处理队列中给定的任务
     * @param string $connection
     * @param Job    $job
     * @param int    $maxTries
     * @param int    $delay
     * @return void
     * @throws Exception
     */
    public function process($connection, $job, $maxTries = 0, $delay = 0)
    {
        try {
            $this->event->trigger(new JobProcessing($connection, $job));

            $this->markJobAsFailedIfAlreadyExceedsMaxAttempts(
                $connection,
                $job,
                (int) $maxTries
            );

            $job->fire();

            $this->event->trigger(new JobProcessed($connection, $job));
        } catch (Exception | Throwable $e) {
            try {
                if (!$job->hasFailed()) {
                    $this->markJobAsFailedIfWillExceedMaxAttempts($connection, $job, (int) $maxTries, $e);
                }

                $this->event->trigger(new JobExceptionOccurred($connection, $job, $e));
            } finally {
                if (!$job->isDeleted() && !$job->isReleased() && !$job->hasFailed()) {
                    $job->release($delay);
                }
            }

            throw $e;
        }
    }

    /**
     * 任务执行失败时调用的，用于检查任务的尝试次数是否已经超过了最大尝试次数，如果已经超过了最大尝试次数，则将任务标记为失败
     * @param string $connection
     * @param Job    $job
     * @param int    $maxTries
     */
    protected function markJobAsFailedIfAlreadyExceedsMaxAttempts($connection, $job, $maxTries)
    {
        $maxTries = !is_null($job->maxTries()) ? $job->maxTries() : $maxTries;

        $timeoutAt = $job->timeoutAt();

        if ($timeoutAt && Carbon::now()->getTimestamp() <= $timeoutAt) {
            return;
        }

        if (!$timeoutAt && (0 === $maxTries || $job->attempts() <= $maxTries)) {
            return;
        }

        $this->failJob($connection, $job, $e = new MaxAttemptsExceededException(
            $job->getName() . ' has been attempted too many times or run too long. The job may have previously timed out.'
        ));

        throw $e;
    }

    /**
     * 任务执行前调用的，用于检查任务的尝试次数是否将会超过最大尝试次数，如果将会超过，则将任务标记为失败
     * @param string    $connection
     * @param Job       $job
     * @param int       $maxTries
     * @param Exception $e
     */
    protected function markJobAsFailedIfWillExceedMaxAttempts($connection, $job, $maxTries, $e)
    {
        $maxTries = !is_null($job->maxTries()) ? $job->maxTries() : $maxTries;

        if ($job->timeoutAt() && $job->timeoutAt() <= Carbon::now()->getTimestamp()) {
            $this->failJob($connection, $job, $e);
        }

        if ($maxTries > 0 && $job->attempts() >= $maxTries) {
            $this->failJob($connection, $job, $e);
        }
    }

    /**
     * @param string    $connection
     * @param Job       $job
     * @param Exception $e
     */
    protected function failJob($connection, $job, $e)
    {
        $job->markAsFailed();

        if ($job->isDeleted()) {
            return;
        }

        try {
            $job->delete();

            $job->failed($e);
        } finally {
            $this->event->trigger(new JobFailed(
                $connection,
                $job,
                $e ?: new RuntimeException('ManuallyFailed')
            ));
        }
    }

    /**
     * 使脚本休眠给定的秒数
     * @param int $seconds
     * @return void
     */
    public function sleep($seconds)
    {
        if ($seconds < 1) {
            usleep($seconds * 1000000);
        } else {
            sleep($seconds);
        }
    }

}
