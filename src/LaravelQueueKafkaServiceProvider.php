<?php

namespace CHENXCHEN\LaravelQueueKafka;

use CHENXCHEN\LaravelQueueKafka\Config\ConsumerConfigs;
use CHENXCHEN\LaravelQueueKafka\Config\ProducerConfigs;
use CHENXCHEN\LaravelQueueKafka\Connectors\KafkaConnector;
use CHENXCHEN\LaravelQueueKafka\Console\QueueKafka;
use Illuminate\Queue\QueueManager;
use Illuminate\Support\ServiceProvider;
use Kafka\Consumer;
use Kafka\ConsumerConfig;
use Kafka\Producer;

class LaravelQueueKafkaServiceProvider extends ServiceProvider
{
    /**
     * Register the service provider.
     */
    public function register()
    {
        if ($this->app->runningInConsole()) {
            $this->commands([
                QueueKafka::class,
            ]);
        }
        $this->registerDependencies();
    }

    /**
     * Register the application's event listeners.
     */
    public function boot()
    {
        /** @var QueueManager $queue */
        $queue = $this->app['queue'];
        $connector = new KafkaConnector($this->app);
        $queue->addConnector('d-kafka', function () use($connector) {
            return $connector;
        });
    }

    /**
     * Register adapter dependencies in the container.
     */
    protected function registerDependencies()
    {
        $this->app->bind('queue.d_kafka.producer', function ($app, $params) {
            return new DProducer($params['conf']);
        });

        $this->app->bind('queue.d_kafka.consumer', function ($app, $params) {
            return new DConsumer($params['conf']);
        });
    }
}
