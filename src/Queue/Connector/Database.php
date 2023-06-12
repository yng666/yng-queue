<?php
namespace Yng\Queue\Connector;

use Carbon\Carbon;
use stdClass;
use Yng\Db;
use Yng\Db\ConnectionInterface;
use Yng\Db\Query;
use Yng\Queue\Connector;
use Yng\Queue\InteractsWithTime;
use Yng\Queue\Job\Database as DatabaseJob;


/**
 * 队列任务--数据库存储引擎
 */
class Database extends Connector
{

    use InteractsWithTime;

    protected $db;

    /**
     * 保存队列任务的表名
     *
     * @var string
     */
    protected $table;

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

    public function __construct(ConnectionInterface $db, $table, $default = 'default', $retryAfter = 60)
    {
        $this->db         = $db;
        $this->table      = $table;
        $this->default    = $default;
        $this->retryAfter = $retryAfter;
    }

    public static function __make(Db $db, $config)
    {
        $connection = $db->connect($config['connection'] ?? null);

        return new self($connection, $config['table'], $config['queue'], $config['retry_after'] ?? 60);
    }

    /**
     * 获取队列任务数量
     */
    public function size($queue = null)
    {
        return $this->db
            ->name($this->table)
            ->where('queue', $this->getQueue($queue))
            ->count();
    }

    /**
     * 向指定队列中添加一个新任务
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->pushToDatabase($queue, $this->createPayload($job, $data));
    }

    /**
     * 将原始的任务数据添加到队列中
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        return $this->pushToDatabase($queue, $payload);
    }

    /**
     * 将一个延迟执行的任务添加到队列中
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        return $this->pushToDatabase($queue, $this->createPayload($job, $data), $delay);
    }

    public function bulk($jobs, $data = '', $queue = null)
    {
        $queue = $this->getQueue($queue);

        $availableAt = $this->availableAt();

        return $this->db->name($this->table)->insertAll(collect((array) $jobs)->map(
            function ($job) use ($queue, $data, $availableAt) {
                return [
                    'queue'          => $queue,
                    'attempts'       => 0,
                    'reserve_time'   => null,
                    'available_time' => $availableAt,
                    'create_time'    => $this->currentTime(),
                    'payload'        => $this->createPayload($job, $data),
                ];
            }
        )->all());
    }

    /**
     * 重新发布任务
     *
     * @param string $queue
     * @param StdClass $job
     * @param int $delay
     * @return mixed
     */
    public function release($queue, $job, $delay)
    {
        return $this->pushToDatabase($queue, $job->payload, $delay, $job->attempts);
    }

    /**
     * 以给定的延迟时间将原始负载推送到数据库
     *
     * @param \DateTime|int $delay
     * @param string|null $queue
     * @param string $payload
     * @param int $attempts
     * @return mixed
     */
    protected function pushToDatabase($queue, $payload, $delay = 0, $attempts = 0)
    {
        return $this->db->name($this->table)->insertGetId([
            'queue'          => $this->getQueue($queue),
            'attempts'       => $attempts,
            'reserve_time'   => null,
            'available_time' => $this->availableAt($delay),
            'create_time'    => $this->currentTime(),
            'payload'        => $payload,
        ]);
    }

    public function pop($queue = null)
    {
        $queue = $this->getQueue($queue);

        return $this->db->transaction(function () use ($queue) {

            if ($job = $this->getNextAvailableJob($queue)) {

                $job = $this->markJobAsReserved($job);

                return new DatabaseJob($this->app, $this, $job, $this->connection, $queue);
            }
        });
    }

    /**
     * 获取下个有效任务
     *
     * @param string|null $queue
     * @return StdClass|null
     */
    protected function getNextAvailableJob($queue)
    {

        $job = $this->db
            ->name($this->table)
            ->lock(true)
            ->where('queue', $this->getQueue($queue))
            ->where(function (Query $query) {
                $query->where(function (Query $query) {
                    $query->whereNull('reserve_time')->where('available_time', '<=', $this->currentTime());
                });

                //超时任务重试
                $expiration = Carbon::now()->subSeconds($this->retryAfter)->getTimestamp();

                $query->whereOr(function (Query $query) use ($expiration) {
                    $query->where('reserve_time', '<=', $expiration);
                });
            })
            ->order('id', 'asc')
            ->find();

        return $job ? (object) $job : null;
    }

    /**
     * 标记任务正在执行.
     *
     * @param stdClass $job
     * @return stdClass
     */
    protected function markJobAsReserved($job)
    {
        $this->db
            ->name($this->table)
            ->where('id', $job->id)
            ->update([
                'reserve_time' => $job->reserve_time = $this->currentTime(),
                'attempts'     => ++$job->attempts,
            ]);

        return $job;
    }

    /**
     * 删除任务
     *
     * @param string $id
     * @return void
     */
    public function deleteReserved($id)
    {
        $this->db->transaction(function () use ($id) {
            if ($this->db->name($this->table)->lock(true)->find($id)) {
                $this->db->name($this->table)->where('id', $id)->delete();
            }
        });
    }

    /**
     * 获取队列任务名
     */
    protected function getQueue($queue)
    {
        return $queue ?: $this->default;
    }
}
