<?php

namespace AliReaza\EventDriven\Kafka;

use AliReaza\EventDriven\EventDrivenListenerInterface;
use Closure;
use Psr\EventDispatcher\ListenerProviderInterface;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Message;

class ListenerProvider implements ListenerProviderInterface, EventDrivenListenerInterface
{
    private bool $unsubscribe = false;
    private ?Closure $message_provider = null;
    private ?Closure $listener_provider = null;
    private int $consumer_timeout_ms = 60 * 1000;
    public array $listeners = [];
    private ?array $partitions = null;

    public function __construct(public Conf $conf)
    {
    }

    public function getListenersForEvent(object $event): iterable
    {
        $name = str_replace('\\', '.', $event::class);

        if (array_key_exists($name, $this->listeners)) {
            return $this->listeners[$name];
        }

        return [];
    }

    public function subscribe(int $timeout_ms = 0): void
    {
        $consumer = new KafkaConsumer($this->conf);

        $consumer->assign($this->partitions);

        $events = array_map(function ($event_name) {
            return str_replace('\\', '.', $event_name);
        }, array_keys($this->listeners));

        $consumer->subscribe($events);

        $this->unsubscribe(false);

        $start_time = microtime(true);

        while (!$this->unsubscribe) {
            $message = $consumer->consume($timeout_ms > 0 ? $timeout_ms : $this->consumer_timeout_ms);

            if (is_null($message->topic_name)) continue;

            $topic_name = str_replace('.', '\\', $message->topic_name);

            if (array_key_exists($topic_name, $this->listeners)) {
                $message = $this->messageHandler($message);

                foreach ($this->listeners[$topic_name] as $listener) {
                    $this->listenerHandler($topic_name, $listener, $message);
                }
            }

            if ($timeout_ms > 0) {
                $time_elapsed_ms = (microtime(true) - $start_time) * 1000;

                if ($time_elapsed_ms > $timeout_ms) {
                    $this->unsubscribe();
                }
            }
        }

        $consumer->unsubscribe();

        $consumer->close();
    }

    public function unsubscribe($unsubscribe = true): void
    {
        $this->unsubscribe = $unsubscribe;
    }

    public function messageHandler(Message $message): Message
    {
        $provider = $this->message_provider;

        return is_null($provider) ? $message : $provider($message);
    }

    public function setMessageProvider(callable|Closure|string|null $provider): void
    {
        if (is_string($provider)) {
            $provider = new $provider;
        }

        if (is_callable($provider)) {
            $provider = Closure::fromCallable($provider);
        }

        $this->message_provider = $provider;
    }

    public function listenerHandler(string $event, mixed $listener, Message $message): void
    {
        if (is_null($this->listener_provider)) {
            if (is_string($listener)) {
                $listener = new $listener;
            }

            if (is_callable($listener)) {
                $listener = Closure::fromCallable($listener);
            }

            $listener($event, $message);
        } else {
            $provider = $this->listener_provider;

            $provider($event, $listener, $message);
        }
    }

    public function setListenerProvider(callable|Closure|string|null $provider): void
    {
        if (is_string($provider)) {
            $provider = new $provider;
        }

        if (is_callable($provider)) {
            $provider = Closure::fromCallable($provider);
        }

        $this->listener_provider = $provider;
    }

    public function setConsumerTimeoutMs(int $consumer_timeout_ms): void
    {
        $this->consumer_timeout_ms = $consumer_timeout_ms;
    }

    public function setPartitions(?array $partitions = null): void
    {
        $this->partitions = $partitions;
    }
}
