<?php
namespace CHENXCHEN\LaravelQueueKafka\Message;

/**
 * Class TopicMessage
 * @package CHENXCHEN\LaravelQueueKafka\Message
 */
class TopicMessage implements \JsonSerializable
{
    /**
     * @var string
     */
    private $topicName = '';
    /**
     * @var PartitionMessage[]
     */
    private $partitionMessages = [];

    public function __construct($topicName, $partitionMessages)
    {
        $this->topicName = $topicName;
        $this->partitionMessages = $partitionMessages;
    }

    /**
     * @return string
     */
    public function getTopicName(): string
    {
        return $this->topicName;
    }

    /**
     * @return PartitionMessage[]
     */
    public function getPartitionMessages(): array
    {
        return $this->partitionMessages;
    }

    public function jsonSerialize()
    {
        return get_object_vars($this);
    }
}