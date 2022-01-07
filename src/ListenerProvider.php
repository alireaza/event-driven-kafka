<?php

namespace AliReaza\EventDriven\Kafka;

use AliReaza\EventDriven\EventDrivenListenerInterface;
use AliReaza\UUID\V4 as UUID_V4;
use Closure;
use Exception;
use Psr\EventDispatcher\ListenerProviderInterface;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use ReflectionClass;
use Throwable;

/**
 * Class ListenerProvider
 *
 * @package AliReaza\EventDriven\Kafka
 */
class ListenerProvider implements ListenerProviderInterface, EventDrivenListenerInterface
{
    private Conf $connection;

    private bool $unsubscribe = false;
    private ?Closure $message_provider = null;
    private ?Closure $listener_provider = null;

    public array $listeners = [];

    public function __construct(public string $servers)
    {
        $this->connection = new Conf();
        $this->connection->set('bootstrap.servers', $this->servers);

        $this->connection->setRebalanceCb(function (KafkaConsumer $kafka, $err, array $partitions = null) {
            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    echo ' [.] Assign: ';
                    var_dump($partitions);
                    $kafka->assign($partitions);
                    break;

                case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                    echo ' [.] Revoke: ';
                    var_dump($partitions);
                    $kafka->assign();
                    break;

                default:
                    throw new Exception($err);
            }
        });

        $this->connection->set('auto.offset.reset', 'earliest');
        $this->connection->set('session.timeout.ms', (string)10 * 1000);

        $this->connection->set('group.id', 'G-' . ((string)new UUID_V4()));
    }

    public function getListenersForEvent(object $event): iterable
    {
        $reflect = new ReflectionClass($event);
        $name = $reflect->getShortName();

        if (array_key_exists($name, $this->listeners)) {
            return $this->listeners[$name];
        }

        return [];
    }

    /**
     * @throws Throwable
     */
    public function subscribe(): void
    {
        $consumer = new KafkaConsumer($this->connection);

        $events = array_keys($this->listeners);

        $consumer->subscribe($events);

        $dispatcher = new EventDispatcher($this->servers);

        $this->unsubscribe(false);
        while (!$this->unsubscribe) {
            $message = $consumer->consume(2 * 60 * 1000);

            if (array_key_exists($message->topic_name, $this->listeners)) {
                $message = $this->messageHandler($message);

                foreach ($this->listeners[$message->topic_name] as $listener) {
                    $headers = $message->headers;

                    if (!is_null($headers) && array_key_exists('correlation_id', $headers)) {
                        $dispatcher->setCorrelationId($headers['correlation_id']);
                        $dispatcher->setCausationId($headers['event_id']);
                    }

                    $this->listenerHandler($listener, $message, $dispatcher);
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

    public function listenerHandler(mixed $listener, Message $message, EventDispatcher $dispatcher): void
    {
        if (is_null($this->listener_provider)) {
            if (is_string($listener)) {
                $listener = new $listener;
            }

            if (is_callable($listener)) {
                $listener = Closure::fromCallable($listener);
            }

            $listener($message, $dispatcher);
        } else {
            $provider = $this->listener_provider;

            $provider($listener, $message, $dispatcher);
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
}