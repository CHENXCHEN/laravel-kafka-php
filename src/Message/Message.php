<?php
namespace CHENXCHEN\LaravelQueueKafka\Message;

/**
 * Class Message
 * @package CHENXCHEN\LaravelQueueKafka\Message
 */
class Message implements \JsonSerializable
{
    /**
     * @var int
     */
    private $offset = null;
    /**
     * @var int
     */
    private $size = null;
    /**
     * @var MessageUnit
     */
    private $messageUnit = null;

    public function __construct($message)
    {
        $this->offset = $message['offset'];
        $this->size = $message['size'];
        $this->messageUnit = new MessageUnit($message['message']);
    }

    /**
     * @return int
     */
    public function getOffset(): int
    {
        return $this->offset;
    }

    /**
     * @return int
     */
    public function getSize(): int
    {
        return $this->size;
    }

    /**
     * @return MessageUnit
     */
    public function getMessageUnit(): MessageUnit
    {
        return $this->messageUnit;
    }

    public function jsonSerialize()
    {
        return get_object_vars($this);
    }
}