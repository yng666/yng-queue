<?php
declare (strict_types = 1);
namespace Yng\Queue;

use Exception;
use Yng\App;
use Yng\Helper\Arr;
use Yng\Helper\Str;

abstract class Job
{

    /**
     * job实例
     * @var object
     */
    private $instance;

    /**
     * $job的JSON解码版本
     * @var array
     */
    private $payload;

    /**
     * @var App
     */
    protected $app;

    /**
     * 队列任务名称
     * @var string
     */
    protected $queue;

    /**
     * 连接名
     */
    protected $connection;

    /**
     * 是否删除队列任务
     * @var bool
     */
    protected $deleted = false;

    /**
     * 是否重新发布队列任务
     * @var bool
     */
    protected $released = false;

    /**
     * 队列任务是否失败
     *
     * @var bool
     */
    protected $failed = false;

    /**
     * 获取队列任务解码正文
     *
     * @return mixed
     */
    public function payload($name = null, $default = null)
    {
        if (empty($this->payload)) {
            $this->payload = json_decode($this->getRawBody(), true);
        }
        if (empty($name)) {
            return $this->payload;
        }
        return Arr::get($this->payload, $name, $default);
    }

    /**
     * 执行队列任务
     * @return void
     */
    public function fire()
    {
        $instance = $this->getResolvedJob();

        [, $method] = $this->getParsedJob();

        $instance->{$method}($this, $this->payload('data'));
    }

    /**
     * 处理导致队列任务失败的异常
     *
     * @param Exception $e
     * @return void
     */
    public function failed($e)
    {
        $instance = $this->getResolvedJob();

        if (method_exists($instance, 'failed')) {
            $instance->failed($this->payload('data'), $e);
        }
    }

    /**
     * 删除队列任务
     * @return void
     */
    public function delete()
    {
        $this->deleted = true;
    }

    /**
     * Determine if the job has been deleted.
     * @return bool
     */
    public function isDeleted()
    {
        return $this->deleted;
    }

    /**
     * 将队列任务重新发布
     * @param int $delay
     * @return void
     */
    public function release($delay = 0)
    {
        $this->released = true;
    }

    /**
     * 确定队列任务是否重新发布
     * @return bool
     */
    public function isReleased()
    {
        return $this->released;
    }

    /**
     * 确定队列任务是否已被删除或重新发布
     * @return bool
     */
    public function isDeletedOrReleased()
    {
        return $this->isDeleted() || $this->isReleased();
    }

    /**
     * 获取队列任务id
     *
     * @return string
     */
    abstract public function getJobId();

    /**
     * 获取重试的次数
     * @return int
     */
    abstract public function attempts();

    /**
     * 获取队列任务的原始数据字符串
     * @return string
     */
    abstract public function getRawBody();

    /**
     * 将队列任务声明为类和方法
     * @return array
     */
    protected function getParsedJob()
    {
        $job      = $this->payload('job');
        $segments = explode('@', $job);

        return count($segments) > 1 ? $segments : [$segments[0], 'fire'];
    }

    /**
     * 解析队列任务处理程序
     * @param string $name
     * @return mixed
     */
    protected function resolve($name, $param)
    {
        $namespace = $this->app->getNamespace() . '\\job\\';

        $class = false !== strpos($name, '\\') ? $name : $namespace . Str::studly($name);

        return $this->app->make($class, [$param], true);
    }

    /**
     * 获取job实例
     */
    public function getResolvedJob()
    {
        if (empty($this->instance)) {
            [$class] = $this->getParsedJob();

            $this->instance = $this->resolve($class, $this->payload('data'));
        }

        return $this->instance;
    }

    /**
     * 判断当前队列任务是否已标记为失败
     *
     * @return bool
     */
    public function hasFailed()
    {
        return $this->failed;
    }

    /**
     * 将当前队列任务标记为失败
     *
     * @return void
     */
    public function markAsFailed()
    {
        $this->failed = true;
    }

    /**
     * 获取重试的次数
     *
     * @return int|null
     */
    public function maxTries()
    {
        return $this->payload('maxTries');
    }

    /**
     * 获取当前队列超时时间
     *
     * @return int|null
     */
    public function timeout()
    {
        return $this->payload('timeout');
    }

    /**
     * 获取当前队列任务何时应超时的时间戳
     *
     * @return int|null
     */
    public function timeoutAt()
    {
        return $this->payload('timeoutAt');
    }

    /**
     * 获取排队中的队列类的名称
     *
     * @return string
     */
    public function getName()
    {
        return $this->payload('job');
    }

    /**
     * 获取当前队列驱动
     *
     * @return string
     */
    public function getConnection()
    {
        return $this->connection;
    }

    /**
     * 获取当前队列任务名
     * @return string
     */
    public function getQueue()
    {
        return $this->queue;
    }
}
