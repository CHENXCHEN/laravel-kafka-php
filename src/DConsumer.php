<?php

namespace CHENXCHEN\LaravelQueueKafka;

use CHENXCHEN\LaravelQueueKafka\Exceptions\QueueKafkaException;
use CHENXCHEN\LaravelQueueKafka\Message\Message;
use CHENXCHEN\LaravelQueueKafka\Message\PartitionMessage;
use CHENXCHEN\LaravelQueueKafka\Message\TopicMessage;
use Kafka\Consumer;
use Kafka\ConsumerConfig;
use Monolog\Handler\StdoutHandler;
use Monolog\Logger;

class DConsumer
{
    /**
     * @var ConsumerConfig
     */
    protected $consumerConfig;

    /**
     * @var Consumer
     */
    protected $consumer;

    /**
     * @var array
     */
    private $configs;

    /**
     * @var array
     */
    private $options = [];

    /**
     * DConsumer constructor.
     * @param array $configs
     */
    public function __construct($configs = [])
    {
        $this->setConfigs($configs);
        $this->setConsumerConfig();
        $this->consumer = new Consumer(null, $this->getConsumerConfig());
    }

    /**
     * @return Consumer
     */
    public function getConsumer()
    {
        return $this->consumer;
    }

    /**
     * @return array
     */
    public function getConfigs()
    {
        return $this->configs;
    }

    /**
     * @return array
     */
    public function getOptions()
    {
        return $this->options;
    }

    public function getOptionTimeout()
    {
        return $this->options['timeout'] ?? 0;
    }

    public function getOptionTries() {
        return $this->options['tries'] ?? 0;
    }

    public function getOptionMemory() {
        return $this->options['memory'] ?? 128;
    }

    /**
     * @param array $options
     */
    public function setOptions($options)
    {
        if (isset($options['timeout']) && $options['timeout'] + 3 > $this->getConfigs()['sessionTimeout'] / 1000) {
            throw new QueueKafkaException("--timeout {$options['timeout']} can not greater than consumer sessionTimeout " . $this->getConfigs()['sessionTimeout'] / 1000 . " - 3");
        }
        if (!isset($options['timeout'])) {
            $options['timeout'] = max(intval($this->getConfigs()['sessionTimeout'] / 1000) - 3, 0);
        }
        $this->options = $options;
    }

    /**
     * @param array $configs
     */
    private function setConfigs($configs)
    {
        $defaultConfig = [
            'topics' => [],
            'metadataRefreshIntervalMs' => 10000,
            'consumeMode' => ConsumerConfig::CONSUME_BEFORE_COMMIT_OFFSET,
            'metadataBrokerList' => '',
            'groupId' => '',
            'brokerVersion' => '2.0.0',
            'sessionTimeout' => 70000,
            'rebalanceTimeout' => 30000,
            'maxBytes' => 65536,
            'maxWaitTime' => 100,
            'offsetReset' => 'latest',
            'clientId' => 'd-kafka-php',
            'isBatchExecute' => true,
        ];
        $consumerConfig = array_only($configs, [
            'metadataBrokerList',
            'topics',
            'brokerVersion',
        ]);
        $consumerConfig = array_merge($defaultConfig, $consumerConfig);
        $consumerConfig = array_merge($consumerConfig, $configs['consumer']);
        $consumerConfig['isBatchExecute'] = true;
        $this->configs = $consumerConfig;
    }

    /**
     * @return ConsumerConfig
     */
    public function getConsumerConfig()
    {
        return $this->consumerConfig;
    }

    /**
     * @return array
     */
    public function getConsumerTopics()
    {
        return $this->getConfigs()['topics'];
    }

    public function setConsumerConfig()
    {
        /**
         * @var ConsumerConfig $cConfig
         */
        $cConfig = new ConsumerConfig();
        $validConsumerConfig = array_only($this->getConfigs(), [
            "clientId", "brokerVersion", "metadataBrokerList", "messageMaxBytes", "metadataRequestTimeoutMs",
            "metadataRefreshIntervalMs", "metadataMaxAgeMs", "securityProtocol", "sslEnable", "sslLocalCert",
            "sslLocalPk", "sslVerifyPeer", "sslPassphrase", "sslCafile", "sslPeerName", "saslMechanism",
            "saslUsername", "saslPassword", "saslKeytab", "saslPrincipal",

            "groupId", "sessionTimeout", "rebalanceTimeout", "topics", "offsetReset", "maxBytes", "maxWaitTime",
            "isBatchExecute",
        ]);
        foreach ($validConsumerConfig as $key => $value) {
            $methodName = "set" . ucfirst($key);
            $cConfig->$methodName($value);
        }
        $this->consumerConfig = $cConfig;
    }

    /**
     * @param string $executeHandle
     */
    private function setExecuteHandle($executeHandle) {
        $this->configs['executeHandle'] = $executeHandle;
    }

    public function getExecuteHandle() {
        return $this->getConfigs()['executeHandle'];
    }

    public function consumeCheck($executeHandle = 'default') {
        // 判断传入的handle是否存在
        if ($executeHandle !== 'default') {
            $this->setExecuteHandle($executeHandle);
        }
        if (!method_exists($this->getExecuteHandle(), 'executes')) {
            throw new QueueKafkaException("executeHandle $executeHandle::executes not exists");
        }
    }

    public function consume()
    {
        $this->getConsumer()->start(function ($messages) {
            $parsedMessages = $this->getMessages($messages);
            $executeHandle = $this->getExecuteHandle();
            $handle = new $executeHandle();
            $this->registerTimeoutHandler();
            while ($this->getOptionTries() - 1 >= 0) {
                try {
                    call_user_func_array(
                        [$handle, 'executes'],
                        [$parsedMessages, ]
                    );
                } catch (\Exception $e) {
                }
            }
            call_user_func_array(
                [$handle, 'executes'],
                [$parsedMessages, ]
            );
            $this->clearTimeoutHandler();
        });
    }

    /**
     * @param array $messageBodies
     * @return TopicMessage[]
     */
    public function getMessages($messageBodies = []) {
        $retTopicMessages = [];
        foreach ($messageBodies as $topicName => $messageBody) {
            $retPartitionMessages = [];
            foreach ($messageBody as $partition => $messages) {
                $retMessages = [];
                foreach ($messages as $message) {
                    $retMessages []= new Message($message);
                }
                $retPartitionMessages []= new PartitionMessage($partition, $retMessages);
            }
            $retTopicMessages []= new TopicMessage($topicName, $retPartitionMessages);
        }
        return $retTopicMessages;
    }

    /**
     * Register the worker timeout handler (PHP 7.1+).
     *
     * @return void
     */
    protected function registerTimeoutHandler()
    {
        if ($this->getOptionTimeout() <= 0 || version_compare(PHP_VERSION, '7.1.0') < 0 || ! extension_loaded('pcntl')) {
            return;
        }

        pcntl_async_signals(true);

        pcntl_signal(SIGALRM, function () {
            throw new QueueKafkaException("Consumer time out in " . $this->getOptionTimeout() . " s");
        });

        $this->checkMemoryExceeded();
        pcntl_alarm($this->getOptionTimeout());
    }

    protected function clearTimeoutHandler() {
        pcntl_alarm(0);
    }

    public function checkMemoryExceeded() {
        if ($this->memoryExceeded($this->getOptionMemory())) {
            throw new QueueKafkaException("Memory exceeded ". $this->getOptionMemory() . 'M');
        }
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
