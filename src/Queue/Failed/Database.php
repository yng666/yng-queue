<?php

namespace Yng\Queue\Queue\Failed;

use Carbon\Carbon;
use Yng\Db;
use Yng\Queue\Queue\FailedJob;

/**
 * 失败存储数据库
 */
class Database extends FailedJob
{

    /**
     * 数据库连接实例
     * @var Db
     */
    protected $db;

    /**
     * 队列失败数据库名
     *
     * @var string
     */
    protected $table;

    public function __construct(Db $db, $table)
    {
        $this->db    = $db;
        $this->table = $table;
    }

    public static function __make(Db $db, $config)
    {
        return new self($db, $config['table']);
    }

    /**
     * 将失败的队列任务记录到存储中
     *
     * @param string     $connection 连接名称 
     * @param string     $queue 队列名称 
     * @param string     $payload 任务负载 
     * @param \Exception $exception 异常信息 
     * @return int|null
     */
    public function log($connection, $queue, $payload, $exception)
    {
        $fail_time = Carbon::now()->toDateTimeString();//记录失败时间

        $exception = (string) $exception;// 异常信息转为字符串

        return $this->getTable()->insertGetId(compact(
            'connection',
            'queue',
            'payload',
            'exception',
            'fail_time'
        ));
    }

    /**
     * 获取所有失败的任务
     *
     * @return array
     */
    public function all()
    {
        return collect($this->getTable()->order('id', 'desc')->select())->all();
    }

    /**
     * 获取单个失败任务 
     *
     * @param mixed $id
     * @return object|null
     */
    public function find($id)
    {
        return $this->getTable()->find($id);
    }

    /**
     * 从存储中删除单个失败任务.
     *
     * @param mixed $id
     * @return bool
     */
    public function forget($id)
    {
        return $this->getTable()->where('id', $id)->delete() > 0;
    }

    /**
     * 从存储中删除所有失败任务
     *
     * @return void
     */
    public function flush()
    {
        $this->getTable()->delete(true);
    }

    /**
     * 获取数据库表实例
     */
    protected function getTable()
    {
        return $this->db->name($this->table);
    }
}
