<?php
namespace CHENXCHEN\LaravelQueueKafka;


use Kafka\Producer;
use Kafka\ProducerConfig;

class DProducer
{
    /**
     * @var ProducerConfig
     */
    protected $producerConfig;
    /**
     * @var Producer
     */
    protected $producer;
    /**
     * @var array
     */
    protected $configs;

    /**
     * DProducer constructor.
     * @param array $configs
     */
    public function __construct($configs = [])
    {
        $this->setConfigs($configs);
        $this->setProducerConfig();
        $this->producer = new Producer(null, $this->producerConfig);
    }

    /**
     * @return Producer
     */
    public function getProducer()
    {
        return $this->producer;
    }

    /**
     * @return ProducerConfig
     */
    public function getProducerConfig()
    {
        return $this->producerConfig;
    }

    /**
     * @param array $configs
     */
    private function setConfigs($configs = []) {
        $defaultConfig = [
            'topics' => [],
            'compression' => \Kafka\Protocol\Produce::COMPRESSION_GZIP,
            'metadataRefreshIntervalMs' => 10000,
            'metadataBrokerList' => '',
            'brokerVersion' => '2.0.0',
            'requiredAck' => 1,
            'produceInterval' => 500,
            'timeout' => 5000,
            'requestTimeout' => 6000,
            'isAsyn' => true,
            'clientId' => 'd-kafka-php',
        ];
        $producerConfig = array_only($configs, [
            'metadataBrokerList',
            'topics',
            'brokerVersion',
        ]);
        $producerConfig = array_merge($defaultConfig, $producerConfig);
        $producerConfig = array_merge($producerConfig, $configs['producer']);
        $producerConfig['isAsyn'] = true;
        $this->configs = $producerConfig;
    }

    /**
     * @return array
     */
    public function getConfigs() {
        return $this->configs;
    }

    /**
     * @return array
     */
    public function getProducerTopics() {
        return $this->getConfigs()['topics'];
    }

    public function setProducerConfig()
    {
        /**
         * @var ProducerConfig $pConfig
         */
        $pConfig = new ProducerConfig();
        $validProducerConfig = array_only($this->getConfigs(), [
            /*"clientId", */"brokerVersion", "metadataBrokerList", "messageMaxBytes", "metadataRequestTimeoutMs",
            "metadataRefreshIntervalMs", "metadataMaxAgeMs", "securityProtocol", "sslEnable", "sslLocalCert",
            "sslLocalPk", "sslVerifyPeer", "sslPassphrase", "sslCafile", "sslPeerName", "saslMechanism",
            "saslUsername", "saslPassword", "saslKeytab", "saslPrincipal",

            "requiredAck","timeout","isAsyn","requestTimeout","produceInterval","compression"
        ]);
        foreach ($validProducerConfig as $key => $value) {
            $methodName = "set" . ucfirst($key);
            $pConfig->$methodName($value);
        }
        $this->producerConfig = $pConfig;
    }

    /**
     * @param array $messages
     */
    public function send($messages = []) {
        $msgs = [];
        $topics = $this->getProducerTopics();
        foreach ($messages as $message) {
            $_msg = $message;
            if (is_array($message)) $_msg = json_encode($message);
            foreach ($topics as $topic) {
                $msgs []= [
                    'topic' => $topic,
                    'value' => $_msg,
                ];
            }
        }
        $this->getProducer()->send($msgs);
    }
}
