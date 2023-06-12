<?php

namespace Yng\Queue\Command;

use Yng\Console\Command;


/**
 * 清空所有失败任务
 */
class FlushFailed extends Command
{
    protected function configure()
    {
        $this->setName('queue:flush')->setDescription('Flush all of the failed queue jobs');
    }

    public function handle()
    {
        $this->app->get('queue.failer')->flush();

        $this->output->info('All failed jobs deleted successfully!');
    }
}
