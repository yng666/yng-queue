<?php
namespace Yng\Queue\Command;

use Yng\Console\Command;
use Yng\Console\Table;
use Yng\Helper\Arr;

/**
 * 配置队列任务失败命令
 */
class ListFailed extends Command
{
    /**
     * 命令的表头
     *
     * @var array
     */
    protected $headers = ['ID', 'Connection', 'Queue', 'Class', 'Fail Time'];

    protected function configure()
    {
        // 列出所有失败的队列作业
        $this->setName('queue:failed')->setDescription('List all of the failed queue jobs');
    }

    public function handle()
    {
        // 无失败的队列任务
        if (count($jobs = $this->getFailedJobs()) === 0) {
            $this->output->info('No failed jobs!');
            return;
        }
        $this->displayFailedJobs($jobs);
    }

    /**
     * 在控制台中显示失败的任务
     *
     * @param array $jobs
     * @return void
     */
    protected function displayFailedJobs(array $jobs)
    {
        $table = new Table();
        $table->setHeader($this->headers);
        $table->setRows($jobs);

        $this->table($table);
    }

    /**
     * 将失败的队列任务编译成可显示的格式
     *
     * @return array
     */
    protected function getFailedJobs()
    {
        $failed = $this->app['queue.failer']->all();

        return collect($failed)->map(function ($failed) {
            return $this->parseFailedJob((array) $failed);
        })->filter()->all();
    }

    /**
     * 解析失败的队列任务行
     *
     * @param array $failed
     * @return array
     */
    protected function parseFailedJob(array $failed)
    {
        $row = array_values(Arr::except($failed, ['payload', 'exception']));

        array_splice($row, 3, 0, $this->extractJobName($failed['payload']));

        return $row;
    }

    /**
     * 从载荷中提取失败的队列任务名称
     *
     * @param string $payload
     * @return string|null
     */
    private function extractJobName($payload)
    {
        $payload = json_decode($payload, true);

        if ($payload && (!isset($payload['data']['command']))) {
            return $payload['job'] ?? null;
        } elseif ($payload && isset($payload['data']['command'])) {
            return $this->matchJobName($payload);
        }
    }

    /**
     * 从载荷中匹配队列任务名称
     *
     * @param array $payload
     * @return string
     */
    protected function matchJobName($payload)
    {
        preg_match('/"([^"]+)"/', $payload['data']['command'], $matches);

        if (isset($matches[1])) {
            return $matches[1];
        }

        return $payload['job'] ?? null;
    }
}
