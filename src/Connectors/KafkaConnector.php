<?php

namespace CHENXCHEN\LaravelQueueKafka\Connectors;

use CHENXCHEN\LaravelQueueKafka\Config\ConsumerConfigs;
use CHENXCHEN\LaravelQueueKafka\Config\ProducerConfigs;
use CHENXCHEN\LaravelQueueKafka\DConsumer;
use CHENXCHEN\LaravelQueueKafka\DProducer;
use CHENXCHEN\LaravelQueueKafka\KafkaQueue;
use Illuminate\Container\Container;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Kafka\Consumer;
use Kafka\ConsumerConfig;
use Kafka\Producer;
use Kafka\ProducerConfig;

class KafkaConnector implements ConnectorInterface
{
    /**
     * @var Container
     */
    private $container;

    /**
     * KafkaConnector constructor.
     *
     * @param Container $container
     */
    public function __construct(Container $container)
    {
        $this->container = $container;
    }

    /**
     * Establish a queue connection.
     *
     * @param array $config
     *
     * @return \Illuminate\Contracts\Queue\Queue
     */
    public function connect(array $config)
    {
        /**
         * @var DProducer $producer
         */
        $producer = $this->container->make('queue.d_kafka.producer', ['conf' => $config]);

        /**
         * @var DConsumer $consumer
         */
        $consumer = $this->container->make('queue.d_kafka.consumer', ['conf' => $config]);

        return new KafkaQueue(
            $producer,
            $consumer,
            $config
        );
    }
}
