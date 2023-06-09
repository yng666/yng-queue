<?php
namespace Yng\Queue\Queue\Command;

use Yng\Console\Command;
use Yng\Helper\Str;
use Yng\Migration\Creator;


/**
 * 用于队列任务的数据库表的迁移
 */
class Table extends Command
{
    protected function configure()
    {
        // 设置命令名称和描述
        $this->setName('queue:table')->setDescription('Create a migration for the queue jobs database table');
    }
     public function handle()
    {
        // 检查是否安装了 yng-migration 扩展
        if (!$this->app->has('migration.creator')) {
            $this->output->error('Install yng-migration first please');
            return;
        }
        // 获取配置文件中的队列任务表名
        $table = $this->app->config->get('queue.connections.database.table');
        // 根据表名生成迁移类名
        $className = Str::studly("create_{$table}_table");


        /**
         * 获取 migration.creator 服务实例
         * @var Creator $creator
         */
        $creator = $this->app->get('migration.creator');

        // 创建迁移并返回迁移文件路径
        $path = $creator->create($className);

        // 加载替代模板（如果定义了的话）
        $contents = file_get_contents(__DIR__ . '/Stubs/jobs.stub');

        // 将模板中的占位符替换为实际的类名和表名
        $contents = strtr($contents, [
            'CreateJobsTable' => $className,
            '{{table}}'       => $table,
        ]);
        
        // 将替换后的模板写入迁移文件
        file_put_contents($path, $contents);

        // 输出成功信息
        $this->output->info('Migration created successfully!');
    }
}