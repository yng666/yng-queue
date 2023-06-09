<?php

namespace Yng\Queue\Queue\Command;

use Yng\Console\Command;
use Yng\Helper\Str;
use Yng\Migration\Creator;


/**
 * 创建一个存储队列失败任务的数据库迁移表
 */
class FailedTable extends Command
{
    // 配置命令名称和描述
    protected function configure()
    {
        $this->setName('queue:failed-table')->setDescription('Create a migration for the failed queue jobs database table');
    }

    // 处理命令
    public function handle()
    {
        // 检查是否已安装 yng-migration，如果没有安装，则输出错误信息并返回
        if (!$this->app->has('migration.creator')) {
            $this->output->error('Install yng-migration first please');
            return;
        }

        // 获取队列失败任务的数据库表名
        $table = $this->app->config->get('queue.failed.table');

        // 创建迁移类名
        $className = Str::studly("create_{$table}_table");

        /**
         * 使用 migration.creator 服务创建迁移
         * @var Creator $creator
         */
        $creator = $this->app->get('migration.creator');

        // 获取迁移文件的路径
        $path = $creator->create($className);

        //读取模板内容
        $contents = file_get_contents(__DIR__ . '/Stubs/failed_jobs.stub');

        // 替换模板中的占位符为实际的类名和表名
        $contents = strtr($contents, [
            'CreateFailedJobsTable' => $className,
            '{{table}}'             => $table,
        ]);

        // 将替换后的模板内容写入迁移文件
        file_put_contents($path, $contents);

        // 输出成功信息
        $this->output->info('Migration created successfully!');
    }
}
