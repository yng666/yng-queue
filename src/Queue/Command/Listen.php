<?php
namespace Yng\Queue\Queue\Command;

use Yng\Console\Command;
use Yng\Console\Input;
use Yng\Console\Input\Argument;
use Yng\Console\Input\Option;
use Yng\Console\Output;
use Yng\Queue\Queue\Listener;


/**
 * listen 模式命令配置
 */
class Listen extends Command
{
    /**
     * @var Listener
     */
    protected $listener;


    /**
     * 构造函数，用于初始化监听器
     */
    public function __construct(Listener $listener)
    {
        parent::__construct();
        $this->listener = $listener;
        // 设置输出处理器，用于将监听器的输出写入控制台
        $this->listener->setOutputHandler(function ($type, $line) {
            $this->output->write($line);
        });
    }

    /**
     * 配置命令选项
     */
    protected function configure()
    {
        $this->setName('queue:listen')
            // 设置命令名称
            ->addArgument('connection', Argument::OPTIONAL, 'The name of the queue connection to work', null)
            // 添加可选的命令参数
            ->addOption('queue', null, Option::VALUE_OPTIONAL, 'The queue to listen on', null)
            // 添加延迟时间
            ->addOption('delay', null, Option::VALUE_OPTIONAL, 'Amount of time to delay failed jobs', 0)
            // 设置内存大小
            ->addOption('memory', null, Option::VALUE_OPTIONAL, 'The memory limit in megabytes', 128)
            // 添加超时时间
            ->addOption('timeout', null, Option::VALUE_OPTIONAL, 'Seconds a job may run before timing out', 60)
            // 设置队列任务等待的时间
            ->addOption('sleep', null, Option::VALUE_OPTIONAL, 'Seconds to wait before checking queue for jobs', 3)
            // 设置任务重试的最大次数
            ->addOption('tries', null, Option::VALUE_OPTIONAL, 'Number of times to attempt a job before logging it failed', 0)
            // 设置命令描述
            ->setDescription('Listen to a given queue');
    }

    /**
     * 执行命令，开始监听队列并处理任务
     */
    public function execute(Input $input, Output $output)
    {
        // 获取队列连接名称，默认为配置文件中的默认队列连接
        $connection = $input->getArgument('connection') ?: $this->app->config->get('queue.default');
        // 获取要监听的队列名称，默认为配置文件中连接的默认队列

        $queue   = $input->getOption('queue') ?: $this->app->config->get("queue.connections.{$connection}.queue", 'default');
        $delay   = $input->getOption('delay'); // 获取失败任务的延迟时间
        $memory  = $input->getOption('memory'); // 获取内存限制
        $timeout = $input->getOption('timeout'); // 获取任务超时时间
        $sleep   = $input->getOption('sleep'); // 获取检查队列的时间间隔
        $tries   = $input->getOption('tries'); // 获取任务尝试次数
        // 使用监听器开始监听队列并处理任务
        $this->listener->listen($connection, $queue, $delay, $sleep, $tries, $memory, $timeout);
    }
}