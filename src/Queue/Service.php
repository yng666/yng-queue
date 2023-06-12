<?php
declare (strict_types = 1);
namespace Yng\Queue;

use Yng\Helper\Arr;
use Yng\Helper\Str;
use Yng\Queue;
use Yng\Queue\Command\FailedTable;
use Yng\Queue\Command\FlushFailed;
use Yng\Queue\Command\ForgetFailed;
use Yng\Queue\Command\Listen;
use Yng\Queue\Command\ListFailed;
use Yng\Queue\Command\Restart;
use Yng\Queue\Command\Retry;
use Yng\Queue\Command\Table;
use Yng\Queue\Command\Work;

/**
 * 队列服务
 */
class Service extends \Yng\Service
{
    public function register()
    {
        $this->app->bind('queue', Queue::class);
        $this->app->bind('queue.failer', function () {

            $config = $this->app->config->get('queue.failed', []);

            $type = Arr::pull($config, 'type', 'none');

            $class = false !== strpos($type, '\\') ? $type : '\\Yng\\Queue\\Failed\\' . Str::studly($type);

            return $this->app->invokeClass($class, [$config]);
        });
    }

    public function boot()
    {
        $this->commands([
            FailedJob::class,
            Table::class,
            FlushFailed::class,
            ForgetFailed::class,
            ListFailed::class,
            Retry::class,
            Work::class,
            Restart::class,
            Listen::class,
            FailedTable::class,
        ]);
    }
}
