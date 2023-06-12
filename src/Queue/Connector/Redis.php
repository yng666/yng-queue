<?php
namespace Yng\Queue\Connector;

use Closure;
use Exception;
use RedisException;
use Yng\Helper\Str;
use Yng\Queue\Connector;
use Yng\Queue\InteractsWithTime;
use Yng\Queue\Job\Redis as RedisJob;

/**
 * 队列任务--Redis存储引擎
 */
class Redis extends Connector
{
    use InteractsWithTime;

    /** @var  \Redis */
    protected $redis;

    /**
     * 默认的队列任务名
     *
     * @var string
     */
    protected $default;

    /**
     * 队列任务过期时间,默认60秒
     *
     * @var int|null
     */
    protected $retryAfter = 60;

    /**
     * 队列任务阻塞的最大秒数
     *
     * @var int|null
     */
    protected $blockFor = null;

    public function __construct($redis, $default = 'default', $retryAfter = 60, $blockFor = null)
    {
        $this->redis      = $redis;
        $this->default    = $default;
        $this->retryAfter = $retryAfter;
        $this->blockFor   = $blockFor;
    }

    public static function __make($config)
    {
        if (!extension_loaded('redis')) {
            throw new Exception('redis扩展未安装');
        }

        $redis = new class($config) {
            protected $config;
            protected $client;

            public function __construct($config)
            {
                $this->config = $config;
                $this->client = $this->createClient();
            }

            protected function createClient()
            {
                $config = $this->config;
                $func   = $config['persistent'] ? 'pconnect' : 'connect';

                $client = new \Redis;
                $client->$func($config['host'], $config['port'], $config['timeout']);

                if ('' != $config['password']) {
                    $client->auth($config['password']);
                }

                if (0 != $config['select']) {
                    $client->select($config['select']);
                }
                return $client;
            }

            public function __call($name, $arguments)
            {
                try {
                    return call_user_func_array([$this->client, $name], $arguments);
                } catch (RedisException $e) {
                    if (Str::contains($e->getMessage(), 'went away')) {
                        $this->client = $this->createClient();
                    }

                    throw $e;
                }
            }
        };

        return new self($redis, $config['queue'], $config['retry_after'] ?? 60, $config['block_for'] ?? null);
    }

    /**
     * 获取队列任务数量
     */
    public function size($queue = null)
    {
        $queue = $this->getQueue($queue);

        return $this->redis->lLen($queue) + $this->redis->zCard("{$queue}:delayed") + $this->redis->zCard("{$queue}:reserved");
    }

    /**
     * 将任务数据添加到队列中
     * @param mixed $payload
     * @param array $data — 数据
     * @param array $options
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->pushRaw($this->createPayload($job, $data), $queue);
    }

    /**
     * 将原始的任务数据添加到队列中
     * @param mixed $payload
     * @param array $data — 数据
     * @param array $options
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        if ($this->redis->rPush($this->getQueue($queue), $payload)) {
            return json_decode($payload, true)['id'] ?? null;
        }
    }

    /**
     * 将一个延迟执行的任务添加到队列中
     * @param int $delay 延迟执行的时间，单位为秒
     * @param Job $job job实例
     * @param array $data 数据
     * @param string $queue 任务名
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        return $this->laterRaw($delay, $this->createPayload($job, $data), $queue);
    }

    protected function laterRaw($delay, $payload, $queue = null)
    {
        if ($this->redis->zadd(
            $this->getQueue($queue) . ':delayed',
            $this->availableAt($delay),
            $payload
        )) {
            return json_decode($payload, true)['id'] ?? null;
        }
    }

    public function pop($queue = null)
    {
        $this->migrate($prefixed = $this->getQueue($queue));

        if (empty($nextJob = $this->retrieveNextJob($prefixed))) {
            return;
        }

        [$job, $reserved] = $nextJob;

        if ($reserved) {
            return new RedisJob($this->app, $this, $job, $reserved, $this->connection, $queue);
        }
    }

    /**
     * 将任何延迟或过期的队列任务迁移到主队列
     *
     * @param string $queue
     * @return void
     */
    protected function migrate($queue)
    {
        $this->migrateExpiredJobs($queue . ':delayed', $queue);

        if (!is_null($this->retryAfter)) {
            $this->migrateExpiredJobs($queue . ':reserved', $queue);
        }
    }

    /**
     * 移动延迟任务
     *
     * @param string $from
     * @param string $to
     * @param bool $attempt
     */
    public function migrateExpiredJobs($from, $to, $attempt = true)
    {
        $this->redis->watch($from);

        $jobs = $this->redis->zRangeByScore($from, '-inf', $this->currentTime());

        if (!empty($jobs)) {
            $this->transaction(function () use ($from, $to, $jobs, $attempt) {

                $this->redis->zRemRangeByRank($from, 0, count($jobs) - 1);

                for ($i = 0; $i < count($jobs); $i += 100) {

                    $values = array_slice($jobs, $i, 100);

                    $this->redis->rPush($to, ...$values);
                }
            });
        }

        $this->redis->unwatch();
    }

    /**
     * 从队列中检索下一个队列任务
     *
     * @param string $queue
     * @return array
     */
    protected function retrieveNextJob($queue)
    {
        if (!is_null($this->blockFor)) {
            return $this->blockingPop($queue);
        }

        $job      = $this->redis->lpop($queue);
        $reserved = false;

        if ($job) {
            $reserved = json_decode($job, true);
            $reserved['attempts']++;
            $reserved = json_encode($reserved);
            $this->redis->zAdd($queue . ':reserved', $this->availableAt($this->retryAfter), $reserved);
        }

        return [$job, $reserved];
    }

    /**
     * 通过 blocking-pop 检索下一个队列任务
     *
     * @param string $queue
     * @return array
     */
    protected function blockingPop($queue)
    {
        $rawBody = $this->redis->blpop($queue, $this->blockFor);

        if (!empty($rawBody)) {
            $payload = json_decode($rawBody[1], true);

            $payload['attempts']++;

            $reserved = json_encode($payload);

            $this->redis->zadd($queue . ':reserved', $this->availableAt($this->retryAfter), $reserved);

            return [$rawBody[1], $reserved];
        }

        return [null, null];
    }

    /**
     * 删除任务
     *
     * @param string $queue
     * @param RedisJob $job
     * @return void
     */
    public function deleteReserved($queue, $job)
    {
        $this->redis->zRem($this->getQueue($queue) . ':reserved', $job->getReservedJob());
    }

    /**
     * 从保留队列中删除一个任务并释放它
     *
     * @param string $queue
     * @param RedisJob $job
     * @param int $delay
     * @return void
     */
    public function deleteAndRelease($queue, $job, $delay)
    {
        $queue = $this->getQueue($queue);

        $reserved = $job->getReservedJob();

        $this->redis->zRem($queue . ':reserved', $reserved);

        $this->redis->zAdd($queue . ':delayed', $this->availableAt($delay), $reserved);
    }

    /**
     * redis事务
     * @param Closure $closure
     */
    protected function transaction(Closure $closure)
    {
        $this->redis->multi();
        try {
            call_user_func($closure);
            if (!$this->redis->exec()) {
                $this->redis->discard();
            }
        } catch (Exception $e) {
            $this->redis->discard();
        }
    }

    protected function createPayloadArray($job, $data = '')
    {
        return array_merge(parent::createPayloadArray($job, $data), [
            'id'       => $this->getRandomId(),
            'attempts' => 0,
        ]);
    }

    /**
     * 随机id
     *
     * @return string
     */
    protected function getRandomId()
    {
        return Str::random(32);
    }

    /**
     * 获取队列名
     *
     * @param string|null $queue
     * @return string
     */
    protected function getQueue($queue)
    {
        $queue = $queue ?: $this->default;
        return "{queues:{$queue}}";
    }
}
