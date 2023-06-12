<?php
namespace Yng\Queue\Command;

use Yng\Console\Command;

use Yng\Cache;
use Yng\Queue\InteractsWithTime;


/**
 * 重启队列
 */
class Restart extends Command
{
    use InteractsWithTime;

    protected function configure()
    {
        // 设置命令名称和描述
        // 在完成当前作业后重新启动队列工作守护进程
        $this->setName('queue:restart')->setDescription('Restart queue worker daemons after their current job');
    }

    public function handle(Cache $cache)
    {
        // 设置缓存并输出信息
        $cache->set('think:queue:restart', $this->currentTime());
        $this->output->info("Broadcasting queue restart signal.");
    }
}
