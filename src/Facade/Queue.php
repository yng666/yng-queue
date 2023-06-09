<?php
namespace Yng\Facade;

use Yng\Facade;

/**
 * Queue门面
 * @package Yng\Facade
 * @mixin \Yng\Queue
 */
class Queue extends Facade
{
    protected static function getFacadeClass()
    {
        return 'queue';
    }
}
