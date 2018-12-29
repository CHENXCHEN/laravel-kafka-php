<?php

namespace CHENXCHEN\LaravelQueueKafka\Console;

use CHENXCHEN\LaravelQueueKafka\KafkaQueue;
use Illuminate\Console\Command;
use Illuminate\Queue\QueueManager;

class QueueKafka extends Command
{
    protected $signature = 'queue:kafka
                            {connection : The name of kafka queue connection}
                            {--class=default : The name of consumer class}
                            {--memory=128 : The memory limit in megabytes}
                            {--timeout=60 : The number of seconds a child process can run}
                            {--tries=0 : Number of times to attempt a job before logging it failed}
                            ';
    protected $description = 'kafka queue consumer';

    /**
     * @var string
     */
    private $executeHandle;

    /**
     * @var array
     */
    private $executeOptions = [];

    public function __construct()
    {
        parent::__construct();
    }

    public function handle()
    {
        $connectionName = $this->argument('connection');
        $this->executeHandle = $this->option('class');
        $this->executeOptions = array_only($this->options(), [
            'memory',
            'timeout',
            'tries',
        ]);
        $this->runWorker($connectionName);
    }

    public function runWorker($connectionName) {
        /** @var QueueManager $queue */
        $queue = $this->laravel['queue'];
        /**
         * @var KafkaQueue $connection
         */
        $connection = $queue->connection($connectionName);
        $connection->getConsumer()->consumeCheck($this->executeHandle);
        $connection->getConsumer()->setOptions($this->executeOptions);
        $connection->getConsumer()->consume();
    }
}
