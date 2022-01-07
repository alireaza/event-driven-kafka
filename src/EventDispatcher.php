<?php

namespace AliReaza\EventDriven\Kafka;

use Closure;
use Psr\EventDispatcher\EventDispatcherInterface;
use RdKafka\Conf;
use ReflectionClass;
use RdKafka\Producer;
use AliReaza\EventDriven\EventDrivenInterface;
use AliReaza\UUID\V4 as UUID_V4;

/**
 * Class EventDispatcher
 *
 * @package AliReaza\EventDriven\Kafka
 */
class EventDispatcher implements EventDispatcherInterface, EventDrivenInterface
{
    private Conf $connection;
    private ?Closure $message_provider = null;
    private ?string $event_id = null;
    private ?string $correlation_id = null;
    private ?string $causation_id = null;

    public function __construct(public string $servers)
    {
        $this->connection = new Conf();
        $this->connection->set('bootstrap.servers', $this->servers);
    }

    public function dispatch(object $event): object
    {
        $this->event_id = null;

        $reflect = new ReflectionClass($event);
        $name = $reflect->getShortName();

        $producer = new Producer($this->connection);
        $topic = $producer->newTopic($name);

        $payload = $this->messageHandler($event);

        $topic->producev(RD_KAFKA_PARTITION_UA, 0, $payload, null, [
            'event_id' => $this->getEventId(),
            'correlation_id' => $this->getCorrelationId(),
            'causation_id' => $this->getCausationId(),
        ]);

        $producer->flush(10 * 1000);

        return $event;
    }

    public function messageHandler(object $event): ?string
    {
        $provider = $this->message_provider;

        return is_null($provider) ? json_encode($event) : $provider($event);
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

    public function getEventId(): string
    {
        if (is_null($this->event_id)) {
            $this->event_id = (string)new UUID_V4();
        }

        return $this->event_id;
    }

    public function setCorrelationId(string $correlation_id): void
    {
        $this->correlation_id = $correlation_id;
    }

    public function getCorrelationId(): string
    {
        return $this->correlation_id ?? $this->getEventId();
    }

    public function setCausationId(string $causation_id): void
    {
        $this->causation_id = $causation_id;
    }

    public function getCausationId(): string
    {
        return $this->causation_id ?? $this->getEventId();
    }
}