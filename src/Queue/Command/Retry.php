<?php
namespace Yng\Queue\Queue\Command;

use Yng\Console\Command;

use stdClass;
use Yng\Console\Input\Argument;
use Yng\Helper\Arr;


/**
 * 重试队列任务命令类
 */
class Retry extends Command
{
    protected function configure()
    {
        // 设置命令名称、参数和描述
        $this->setName('queue:retry')
            ->addArgument('id', Argument::IS_ARRAY | Argument::REQUIRED, 'The ID of the failed job or "all" to retry all jobs')
            ->setDescription('Retry a failed queue job');
    }

    public function handle()
    {
        // 循环处理所有要重试的队列任务
        foreach ($this->getJobIds() as $id) {
            $job = $this->app['queue.failer']->find($id);

            // 队列任务不存在，输出错误信息
            if (is_null($job)) {
                $this->output->error("Unable to find failed job with ID [{$id}].");
            } else {
                // 重试队列任务
                $this->retryJob($job);

                // 输出信息
                $this->output->info("The failed job [{$id}] has been pushed back onto the queue!");

                // 从失败列表中删除该队列任务
                $this->app['queue.failer']->forget($id);
            }
        }
    }

    /**
     * 重试队列任务
     *
     * @param stdClass $job
     * @return void
     */
    protected function retryJob($job)
    {
        $this->app['queue']->connection($job['connection'])->pushRaw($this->resetAttempts($job['payload']),$job['queue']);
    }

    /**
     * 重置有效负载中的尝试次数
     *
     * 适用于在其负载中存储尝试的 Redis 队列任务
     *
     * @param string $payload
     * @return string
     */
    protected function resetAttempts($payload)
    {
        $payload = json_decode($payload, true);

        if (isset($payload['attempts'])) {
            $payload['attempts'] = 0;
        }

        return json_encode($payload);
    }

    /**
     * 获取要重试的队列任务ID
     *
     * @return array
     */
    protected function getJobIds()
    {
        $ids = (array) $this->input->getArgument('id');

        if (count($ids) === 1 && $ids[0] === 'all') {
            $ids = Arr::pluck($this->app['queue.failer']->all(), 'id');
        }

        return $ids;
    }
}
