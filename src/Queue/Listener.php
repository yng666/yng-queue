<?php
declare (strict_types = 1);
namespace Yng\Queue;
use Closure;
use Symfony\Component\Process\PhpExecutableFinder;
use Symfony\Component\Process\Process;
use Yng\App;

/**
 * 队列中监听任务之一
 * listen模式  父进程 + 子进程 的处理模式
 * 会在所在的父进程会建立一个单次执行模式的work子进程,并经过该work子进程来处理队列中的下一个消息,当这个work子进程退出以后;
 * 所在的父进程会监听到该子进程的退出信号,并从新建立一个新的单次执行的work子进程;
 */
class Listener
{
    /**
     * @var string 命令路径
     */
    protected $commandPath;
     /**
     * @var string 工作命令
     */
    protected $workerCommand;
     /**
     * @var \Closure|null 输出处理器
     */
    protected $outputHandler;

    /**
     * 构造函数
     *
     * @param string $commandPath 命令路径
     */
    public function __construct($commandPath)
    {
        $this->commandPath = $commandPath;
    }

    /**
     * 创建Listener实例
     *
     * @param App $app
     * @return Listener
     */
    public static function __make(App $app)
    {
        return new self($app->getRootPath());
    }

    /**
     * 获取PHP二进制文件路径
     *
     * @return string
     */
    protected function phpBinary()
    {
        return (new PhpExecutableFinder)->find(false);
    }


    /**
     * 监听队列
     *
     * @param string $connection 连接名称
     * @param string $queue 队列名称
     * @param int    $delay 延迟时间
     * @param int    $sleep 休眠时间
     * @param int    $maxTries 最大尝试次数
     * @param int    $memory 内存限制
     * @param int    $timeout 超时时间
     * @return void
     */
    public function listen($connection, $queue, $delay = 0, $sleep = 3, $maxTries = 0, $memory = 128, $timeout = 60)
    {
        $process = $this->makeProcess($connection, $queue, $delay, $sleep, $maxTries, $memory, $timeout);
         while (true) {
            $this->runProcess($process, $memory);
        }
    }


    /**
     * 创建处理队列的进程
     *
     * @param string $connection 连接名称
     * @param string $queue 队列名称
     * @param int    $delay 延迟时间
     * @param int    $sleep 休眠时间
     * @param int    $maxTries 最大尝试次数
     * @param int    $memory 内存限制
     * @param int    $timeout 超时时间
     * @return Process
     */
    public function makeProcess($connection, $queue, $delay, $sleep, $maxTries, $memory, $timeout)
    {
        $command = array_filter([
            $this->phpBinary(),// PHP 二进制文件路径
            'yng', // yng 命令
            'queue:work', // 工作队列命令
            $connection, // 连接名称
            '--once', // 仅处理一个任务
            "--queue={$queue}", // 队列名称
            "--delay={$delay}", // 延迟时间
            "--memory={$memory}", // 内存限制
            "--sleep={$sleep}", // 睡眠时间
            "--tries={$maxTries}", // 最大尝试次数
        ], function ($value) {
            return !is_null($value);
        });
        return new Process($command, $this->commandPath, null, null, $timeout);
    }

    /**
     * 运行处理队列的进程
     *
     * @param Process $process 进程实例
     * @param int     $memory 内存限制
     */
    public function runProcess(Process $process, $memory)
    {
        $process->run(function ($type, $line) {
            $this->handleWorkerOutput($type, $line);
        });
         if ($this->memoryExceeded($memory)) {
            $this->stop();
        }
    }

    /**
     * 处理工作进程的输出
     *
     * @param int    $type 输出类型
     * @param string $line 输出内容
     * @return void
     */
    protected function handleWorkerOutput($type, $line)
    {
        if (isset($this->outputHandler)) {
            call_user_func($this->outputHandler, $type, $line);
        }
    }

    /**
     * 检查是否超出内存限制
     *
     * @param int $memoryLimit 内存限制
     * @return bool
     */
    public function memoryExceeded($memoryLimit)
    {
        return (memory_get_usage() / 1024 / 1024) >= $memoryLimit;
    }


    /**
     * 停止监听
     *
     * @return void
     */
    public function stop()
    {
        die;
    }

    /**
     * 设置输出处理器
     *
     * @param \Closure $outputHandler 输出处理器
     * @return void
     */
    public function setOutputHandler(Closure $outputHandler)
    {
        $this->outputHandler = $outputHandler;
    }
}