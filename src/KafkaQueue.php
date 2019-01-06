<?php

namespace CHENXCHEN\LaravelQueueKafka;

use CHENXCHEN\LaravelQueueKafka\Exceptions\QueueKafkaException;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Kafka\Consumer;
use Kafka\ConsumerConfig;
use Kafka\Producer;
use Kafka\ProducerConfig;

class KafkaQueue extends Queue implements QueueContract
{
    /**
     * @var array
     */
    protected $config;
    /**
     * @var DProducer
     */
    protected $producer;
    /**
     * @var DConsumer
     */
    protected $consumer;

    /**
     * 设置批量push标识
     * @param bool $isBatchPush
     */
    public function setIsBatchPush($isBatchPush)
    {
        $this->isBatchPush = $isBatchPush;
    }

    /**
     * KafkaQueue constructor.
     * @param DProducer $producer
     * @param DConsumer $consumer
     * @param array $config
     */
    public function __construct(DProducer $producer, DConsumer $consumer, $config)
    {
        $this->producer = $producer;
        $this->consumer = $consumer;
        $this->config = $config;
    }

    /**
     * @return DConsumer
     */
    public function getConsumer() {
        return $this->consumer;
    }

    /**
     * @return DProducer
     */
    public function getProducer() {
        return $this->producer;
    }

    /**
     * @return array
     */
    public function getConfig()
    {
        return $this->config;
    }

    /**
     * Push a new job onto the queue.
     *
     * @param  object  $job
     * @param  mixed   $data
     * @param  string  $queue
     * @return mixed
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->pushRaw($this->createPayload($job, $data), $queue, []);
    }

    /**
     * @param string $payload
     * @param null $queue
     * @param array $options
     * @return string
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        try {
            $this->producer->send([$payload, ]);
        } catch (\Exception $exception) {
            $this->reportConnectionError('pushRaw', $exception);
        }
        return uniqid('', true);
    }

    public function reportConnectionError($action, \Exception $e) {
        $errStr = 'Kafka error while attempting ' . $action . ': ' . $e->getMessage();
        \Log::error($errStr);
        throw new QueueKafkaException('Error writing data to the connection with Kafka. ' . $errStr);
    }

    /**
     * @param object $job
     * @param string $data
     * @param string $queue
     *
     * @return string
     * @throws \InvalidArgumentException
     */
    public function createPayload($job, $data = '', $queue = null)
    {
        if (!method_exists($job, 'getData')) {
            throw new \InvalidArgumentException("Unable to get job data " . get_class($job) . "::getData undefined ");
        }
        $jobData = $job->getData();
        $payload = json_encode([
            'job' => get_class($job),
            'data' => $jobData,
            'ext' => [
                'epochSec' => time(),
            ],
        ]);

        if (JSON_ERROR_NONE !== json_last_error()) {
            throw new InvalidArgumentException('Unable to create payload: '.json_last_error_msg());
        }
        return $payload;
    }

    public function pop($queue = null)
    {
        throw new QueueKafkaException('Please use artisan queue:kafka consumer for this');
    }


    public function later($delay, $job, $data = '', $queue = null)
    {
        //Later is not sup
        throw new QueueKafkaException('Later not yet implemented');
    }

    public function size($queue = null)
    {
        // Since Kafka is an infinite queue we can't count the size of the queue.
        return 1;
    }
}
