<?php

namespace CHENXCHEN\LaravelQueueKafka\Console;

use Illuminate\Console\Command;

class KafkaWorker
{
    public function __construct()
    {
    }

    /**
     * Register the worker timeout handler (PHP 7.1+).
     *
     * @return void
     */
    protected function registerTimeoutHandler()
    {
        if (version_compare(PHP_VERSION, '7.1.0') < 0 || !extension_loaded('pcntl')) {
            return;
        }

        pcntl_async_signals(true);

        pcntl_signal(SIGALRM, function () {
            if (extension_loaded('posix')) {
                posix_kill(getmypid(), SIGKILL);
            }

            exit(1);
        });

        pcntl_alarm(10);
    }

    /**
     * Determine if the memory limit has been exceeded.
     *
     * @param  int   $memoryLimit
     * @return bool
     */
    public function memoryExceeded($memoryLimit)
    {
        return (memory_get_usage() / 1024 / 1024) >= $memoryLimit;
    }
}
