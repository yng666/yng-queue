<?php
declare (strict_types = 1);
namespace Yng\Queue;

use Yng\App;

/**
 * 处理队列中的任务
 */
class CallQueuedHandler
{
    protected $app;

    public function __construct(App $app)
    {
        $this->app = $app;
    }

    public function call(Job $job, array $data)
    {
        $command = unserialize($data['command']);

        $this->app->invoke([$command, 'handle']);

        if (!$job->isDeletedOrReleased()) {
            $job->delete();
        }
    }

    public function failed(array $data)
    {
        $command = unserialize($data['command']);

        if (method_exists($command, 'failed')) {
            $command->failed();
        }
    }
}
