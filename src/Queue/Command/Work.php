<?php
namespace Yng\Queue\Command;

use Yng\Console\Command;
use Yng\Console\Input;
use Yng\Console\Input\Argument;
use Yng\Console\Input\Option;
use Yng\Console\Output;
use Yng\Queue\Event\JobFailed;
use Yng\Queue\Event\JobProcessed;
use Yng\Queue\Event\JobProcessing;
use Yng\Queue\Job;
use Yng\Queue\Worker;

/**
 * Work 模式命令配置
 */
class Work extends Command
{

    /**
     * 工作的队列实例
     * @var Worker
     */
    protected $worker;

    public function __construct(Worker $worker)
    {
        parent::__construct();
        $this->worker = $worker;
    }

    /**
     * 命令配置
     */
    protected function configure()
    {
        $this->setName('queue:work')
            // 设置队列连接名
            ->addArgument('connection', Argument::OPTIONAL, 'The name of the queue connection to work', null)
            // 设置队列名
            ->addOption('queue', null, Option::VALUE_OPTIONAL, 'The queue to listen on')
            // 只处理队列中的下一个任务
            ->addOption('once', null, Option::VALUE_NONE, 'Only process the next job on the queue')
            // 设置延迟时间,单位秒
            ->addOption('delay', null, Option::VALUE_OPTIONAL, 'Amount of time to delay failed jobs', 0)
            // 即使在维护模式下也强制队列运行
            ->addOption('force', null, Option::VALUE_NONE, 'Force the worker to run even in maintenance mode')
            // 设置运行内存大小
            ->addOption('memory', null, Option::VALUE_OPTIONAL, 'The memory limit in megabytes', 128)
            // 队列任务执行时间
            ->addOption('timeout', null, Option::VALUE_OPTIONAL, 'The number of seconds a child process can run', 60)
            // 设置队列任务等待的时间
            ->addOption('sleep', null, Option::VALUE_OPTIONAL, 'Number of seconds to sleep when no job is available', 3)
            // 设置任务重试的最大次数
            ->addOption('tries', null, Option::VALUE_OPTIONAL, 'Number of times to attempt a job before logging it failed', 0)
            ->setDescription('Process the next job on a queue');
    }

    /**
     * 执行命令，开始监听队列并处理任务
     * @param Input  $input
     * @param Output $output
     * @return int|null|void
     */
    public function execute(Input $input, Output $output)
    {
        $connection = $input->getArgument('connection') ?: $this->app->config->get('queue.default');

        $queue = $input->getOption('queue') ?: $this->app->config->get("queue.connections.{$connection}.queue", 'default');
        $delay = $input->getOption('delay');
        $sleep = $input->getOption('sleep');
        $tries = $input->getOption('tries');

        $this->listenForEvents();

        // 如果设置了once 就执行下一个任务
        if ($input->getOption('once')) {
            $this->worker->runNextJob($connection, $queue, $delay, $sleep, $tries);
        } else {
            $memory  = $input->getOption('memory');
            $timeout = $input->getOption('timeout');
            $this->worker->daemon($connection, $queue, $delay, $sleep, $tries, $memory, $timeout);
        }
    }

    /**
     * 注册事件
     */
    protected function listenForEvents()
    {
        $this->app->event->listen(JobProcessing::class, function (JobProcessing $event) {
            $this->writeOutput($event->job, 'starting');
        });

        $this->app->event->listen(JobProcessed::class, function (JobProcessed $event) {
            $this->writeOutput($event->job, 'success');
        });

        $this->app->event->listen(JobFailed::class, function (JobFailed $event) {
            $this->writeOutput($event->job, 'failed');

            $this->logFailedJob($event);
        });
    }

    /**
     * 为队列工作者写入状态输出
     *
     * @param Job $job
     * @param     $status
     */
    protected function writeOutput(Job $job, $status)
    {
        switch ($status) {
            case 'starting':
                $this->writeStatus($job, 'Processing', 'comment');
                break;
            case 'success':
                $this->writeStatus($job, 'Processed', 'info');
                break;
            case 'failed':
                $this->writeStatus($job, 'Failed', 'error');
                break;
        }
    }

    /**
     * 格式化队列任务的状态输出
     *
     * @param Job    $job
     * @param string $status
     * @param string $type
     * @return void
     */
    protected function writeStatus(Job $job, $status, $type)
    {
        $this->output->writeln(sprintf(
            "<{$type}>[%s][%s] %s</{$type}> %s",
            date('Y-m-d H:i:s'),
            $job->getJobId(),
            str_pad("{$status}:", 11),
            $job->getName()
        ));
    }

    /**
     * 记录失败任务
     * @param JobFailed $event
     */
    protected function logFailedJob(JobFailed $event)
    {
        $this->app['queue.failer']->log(
            $event->connection,
            $event->job->getQueue(),
            $event->job->getRawBody(),
            $event->exception
        );
    }

}
