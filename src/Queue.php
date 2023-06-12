<?php
declare (strict_types = 1);
namespace Yng;

use Yng\Queue\Connector;
use Yng\Queue\Connector\Database;
use Yng\Queue\Connector\Redis;

/**
 * 队列类
 * @package Yng\queue
 *
 * @mixin Database
 * @mixin Redis
 */
class Queue extends Manager
{
    protected $namespace = '\\Yng\\Queue\\Connector\\';

    /**
     * 获取驱动类型
     * @param string $name 驱动
     */
    protected function resolveType(string $name)
    {
        return $this->app->config->get("queue.connections.{$name}.type", 'sync');
    }

    /**
     * 获取驱动配置
     * @param string $name 驱动
     */
    protected function resolveConfig(string $name)
    {
        return $this->app->config->get("queue.connections.{$name}");
    }


    protected function createDriver(string $name)
    {
        /**
         * @var Connector $driver
         */
        $driver = parent::createDriver($name);

        return $driver->setApp($this->app)->setConnection($name);
    }

    /**
     * 切换队列驱动
     * @param null|string $name
     * @return Connector
     */
    public function connection($name = null)
    {
        return $this->driver($name);
    }

    /**
     * 默认驱动
     * @return string
     */
    public function getDefaultDriver()
    {
        return $this->app->config->get('queue.default');
    }
}
