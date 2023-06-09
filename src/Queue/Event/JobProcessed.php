<?php

namespace Yng\Queue\Queue\Event;

use Yng\Queue\Queue\Job;
class JobProcessed
{
    /** @var string */
    public $connection;

    /** @var Job */
    public $job;

    public function __construct($connection, $job)
    {
        $this->connection = $connection;
        $this->job        = $job;
    }
}
