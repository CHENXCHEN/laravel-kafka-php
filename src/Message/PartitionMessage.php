<?php
namespace CHENXCHEN\LaravelQueueKafka\Message;

/**
 * Class PartitionMessage
 * @package CHENXCHEN\LaravelQueueKafka\Message
 */
class PartitionMessage implements \JsonSerializable
{
    /**
     * @var int
     */
    private $partition = null;
    /**
     * @var Message[]
     */
    private $messages = [];

    public function __construct($partition, $messages)
    {
        $this->partition = $partition;
        $this->messages = $messages;
    }

    /**
     * @return int
     */
    public function getPartition(): int
    {
        return $this->partition;
    }

    /**
     * @return Message[]
     */
    public function getMessages(): array
    {
        return $this->messages;
    }

    public function jsonSerialize()
    {
        return get_object_vars($this);
    }
}