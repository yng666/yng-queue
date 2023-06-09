<?php

namespace Yng\Queue\Queue\Command;

use Yng\Console\Command;

use Yng\Console\Input\Argument;


/**
 * 删除失败的任务
 */
class ForgetFailed extends Command
{

    // 配置命令选项
    protected function configure()
    {
        $this->setName('queue:forget')// 设置命令名称
            ->addArgument('id', Argument::REQUIRED, 'The ID of the failed job')// 添加必需的命令参数
            ->setDescription('Delete a failed queue job');// 设置命令描述
    }

    public function handle()
    {
        // 获取失败任务的ID，并使用队列失败器删除该任务
        if ($this->app['queue.failer']->forget($this->input->getArgument('id'))) {
            $this->output->info('Failed job deleted successfully!');
        } else {
            $this->output->error('No failed job matches the given ID.');
        }
    }
}
