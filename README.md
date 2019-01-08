# Kafka Queue Driver for Laravel
## How to use
### 1. Install the [CHENXCHEN/kafka-php](https://github.com/CHENXCHEN/kafka-php)
```bash
composer require chenxchen/kafka-php:dev-baoshi
```
Here is a minimal example of a composer.json file:
```json
{
	"require": {
		"chenxchen/kafka-php": "dev-baoshi"
	}
}
```
### 2. Define you queue in `config/queue.php` :
 
 ```php
 return [
 // ...
     'connections' => [
         'kafka-demo' => [
             'driver' => 'd-kafka',
             'metadataBrokerList' => '127.0.0.1:9092', // If the producer and consumer do not configure this configuration item, this configuration item will be used by producers and consumers.
             'topics' => ['local_test', ], // If the producer and consumer do not configure this configuration item, this configuration item will be used by producers and consumers.
             'brokerVersion' => '2.0.0', // If the producer and consumer do not configure this configuration item, this configuration item will be used by producers and consumers.
             'producer' => [
//                 'topics' => ['local_test', 'local_test_2', ], 
                 'compression' => \Kafka\Protocol\Produce::COMPRESSION_GZIP,
                 'metadataRefreshIntervalMs' => 10000,
 //                'metadataBrokerList' => '',
 //                'brokerVersion' => '2.0.0',
                 'requiredAck' => '1',
                 'produceInterval' => 500,
                 'timeout' => 5000,
             ],
             'consumer' => [
 //                'topics' => ['local_test', 'local_test_2', ],
                 'metadataRefreshIntervalMs' => 10000,
                 'consumeMode' => \Kafka\ConsumerConfig::CONSUME_BEFORE_COMMIT_OFFSET,
 //                'metadataBrokerList' => '',
                 'groupId' => 'test-chc',
 //                'brokerVersion' => '2.0.0',
 //                'sessionTimeout' => 30000,
 //                'rebalanceTimeout' => 30000,
                 'maxBytes' => 65536,
                 'maxWaitTime' => 100,
                 'executeHandle' => \App\Jobs\KafkaDemo\KafkaDemoHandle::class, // consumer handle
             ],
         ]
     ],
 // ...
 ];
 ```
### 3. Define your Job
```php
<?php

namespace App\Jobs\KafkaDemo;

use Illuminate\Bus\Queueable;
use Illuminate\Queue\SerializesModels;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Contracts\Queue\ShouldQueue;

class KafkaDemoJob implements ShouldQueue
{
    use InteractsWithQueue, Queueable, SerializesModels;

    /**
     * 内容
     * @var array
     */
    protected $content = null;

    /**
     * Create a new job instance.
     *
     * @param array $content  
     */
    public function __construct($content)
    {
        $this->content = $content;

        // connection name define in queue.php
        $this->connection = 'kafka-demo';
    }

    /**
     * You must implement this function for get data
     * @return array
     */
    public function getData() {
        return $this->content;
    }
}
```
Then you can dispatch this job:
```php
$job = new KafkaDemoJob([33,567]);
dispatch($job);
```
### 4. Define your Consumer handle
```php
<?php

namespace App\Jobs\KafkaDemo;

use CHENXCHEN\LaravelQueueKafka\Message\TopicMessage;

class KafkaDemoHandle
{
    /**
     * @param TopicMessage[] $messages
     */
    public function executes($messages) {
        printf("%s\n", json_encode($messages));
    }
}

``` 
### 5. Start consumer
```bash
php artisan queue:kafka kafka-demo
```

## Supported versions of Laravel
Tested on: [5.3]
