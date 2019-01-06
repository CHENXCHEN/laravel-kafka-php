<?php
namespace CHENXCHEN\LaravelQueueKafka\Message;

/**
 * Class Message
 * @property int $crc
 * @property int $magic
 * @property int $attr
 * @property int $timestamp
 * @property int $key
 * @property int $value
 * @method int getCRC()
 * @method int getMagic()
 * @method int getTimestamp()
 * @method int getKey()
 * @method mixed getValue()
 * @package CHENXCHEN\LaravelQueueKafka\Message
 */
class MessageUnit implements \JsonSerializable
{
    /**
     * @var array
     */
    private $message = [];

    public function __construct($message)
    {
        $messageValue = $message['value'];
        $messageValue = json_decode($messageValue);
        if (json_last_error() == JSON_ERROR_NONE) {
            $message['value'] = $messageValue;
        }
        $this->message = $message;
    }

    public function __get($name)
    {
        return $this->message[$name] ?? null;
    }

    public function __call($name, $arguments)
    {
        $isGet = substr($name, 0, 3) === 'get';

        $option = strtolower(substr($name, 3));
        if ($isGet) {
            return $this->message[$option] ?? null;
        }
        return null;
    }

    /**
     * @return array
     */
    public function getMessage(): array
    {
        return $this->message;
    }

    public function jsonSerialize()
    {
        return get_object_vars($this);
    }
}