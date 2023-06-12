<?php
declare (strict_types = 1);
namespace Yng\Queue;

use DateTimeInterface;
use InvalidArgumentException;
use Yng\App;


/**
 * 连接抽象类
 */
abstract class Connector
{
    /**
     * @var App
     */
    protected $app;

    /**
     * 队列连接名
     *
     * @var string
     */
    protected $connection;

    protected $options = [];

    /**
     * 获取指定队列中的任务数量
     */
    abstract public function size($queue = null);

    /**
     * 向指定队列中添加一个新任务
     */
    abstract public function push($job, $data = '', $queue = null);


    /**
     * 向指定队列中添加一个新任务
     * @param string $queue 任务名
     * @param Job $job job实例
     * @param array $data 数据
     */
    public function pushOn($queue, $job, $data = '')
    {
        return $this->push($job, $data, $queue);
    }

    /**
     * 将原始的任务数据添加到队列中
     * @param mixed $queue job实例的一些配置
     * @param string $queue 任务名
     * @param Job $job job实例
     * @param array $data 数据
     */
    abstract public function pushRaw($payload, $queue = null, array $options = []);

    /**
     * 将一个延迟执行的任务添加到队列中
     * @param int $delay 延迟执行的时间，单位为秒
     * @param Job $job job实例
     * @param array $data 数据
     * @param string $queue 任务名
     */
    abstract public function later($delay, $job, $data = '', $queue = null);

    /**
     * 将一个延迟执行的任务添加到队列中
     * @param string $queue 任务名
     * @param int $delay 延迟执行的时间，单位为秒
     * @param Job $job job实例
     * @param array $data 数据
     */
    public function laterOn($queue, $delay, $job, $data = '')
    {
        return $this->later($delay, $job, $data, $queue);
    }

    /**
     * 一次性添加多个任务到队列中
     * @param Job $job job实例
     * @param array $data 数据
     * @param string $queue 任务名
     */
    public function bulk($jobs, $data = '', $queue = null)
    {
        foreach ((array) $jobs as $job) {
            $this->push($job, $data, $queue);
        }
    }

    abstract public function pop($queue = null);

    /**
     * 创建队列数据格式
     * 便于识别创建的job
     */
    protected function createPayload($job, $data = '')
    {
        $payload = $this->createPayloadArray($job, $data);

        $payload = json_encode($payload);

        if (JSON_ERROR_NONE !== json_last_error()) {
            throw new InvalidArgumentException('Unable to create payload: ' . json_last_error_msg());
        }

        return $payload;
    }

    /**
     * 创建队列初始数据
     */
    protected function createPayloadArray($job, $data = '')
    {
        return is_object($job) ? $this->createObjectPayload($job) : $this->createPlainPayload($job, $data);
    }

    /**
     * 创建普通job初始数据
     */
    protected function createPlainPayload($job, $data)
    {
        return [
            'job'      => $job,
            'maxTries' => null,
            'timeout'  => null,
            'data'     => $data,
        ];
    }

    /**
     * 创建job对象初始数据
     */
    protected function createObjectPayload($job)
    {
        return [
            'job'       => 'Yng\Queue\CallQueuedHandler@call',
            'maxTries'  => $job->tries ?? null,
            'timeout'   => $job->timeout ?? null,
            'timeoutAt' => $this->getJobExpiration($job),
            'data'      => [
                'commandName' => get_class($job),
                'command'     => serialize(clone $job),
            ],
        ];
    }

    /**
     * 获取job队列过期时间
     * @param $job
     */
    public function getJobExpiration($job)
    {
        if (!method_exists($job, 'retryUntil') && !isset($job->timeoutAt)) {
            return;
        }

        $expiration = $job->timeoutAt ?? $job->retryUntil();

        return $expiration instanceof DateTimeInterface ? $expiration->getTimestamp() : $expiration;
    }

    /**
     * 设置队列任务的元数据
     * @param $payload 
     * @param string $key 要设置的元数据键
     * @param mixed $value 要设置的元数据值
     */
    protected function setMeta($payload, $key, $value)
    {
        $payload       = json_decode($payload, true);
        $payload[$key] = $value;
        $payload       = json_encode($payload);

        if (JSON_ERROR_NONE !== json_last_error()) {
            throw new InvalidArgumentException('Unable to create payload: ' . json_last_error_msg());
        }

        return $payload;
    }

    public function setApp(App $app)
    {
        $this->app = $app;
        return $this;
    }

    /**
     * 获取队列连接名
     *
     * @return string
     */
    public function getConnection()
    {
        return $this->connection;
    }

    /**
     * 设置队列的连接器名称
     *
     * @param string $name
     * @return $this
     */
    public function setConnection($name)
    {
        $this->connection = $name;

        return $this;
    }
}
