<?php
declare (strict_types = 1);
namespace Yng\Queue\Queue;

use Yng\Helper\Arr;
use Yng\Helper\Str;
use Yng\Queue;
use Yng\Queue\Queue\Command\FailedTable;
use Yng\Queue\Queue\Command\FlushFailed;
use Yng\Queue\Queue\Command\ForgetFailed;
use Yng\Queue\Queue\Command\Listen;
use Yng\Queue\Queue\Command\ListFailed;
use Yng\Queue\Queue\Command\Restart;
use Yng\Queue\Queue\Command\Retry;
use Yng\Queue\Queue\Command\Table;
use Yng\Queue\Queue\Command\Work;

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

            $class = false !== strpos($type, '\\') ? $type : '\\Yng\\Queue\\Queue\\Failed\\' . Str::studly($type);

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
