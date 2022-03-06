<?php

namespace AliReaza\EventDriven\Kafka;

use AliReaza\EventDriven\EventDrivenInterface;
use AliReaza\UUID\V4 as UUID_V4;
use Closure;
use Psr\EventDispatcher\EventDispatcherInterface;
use RdKafka\Conf;
use RdKafka\Producer;

class EventDispatcher implements EventDispatcherInterface, EventDrivenInterface
{
    private ?Closure $message_provider = null;
    private int $partition = RD_KAFKA_PARTITION_UA;
    private int $msg_flags = RD_KAFKA_MSG_F_BLOCK;
    private int $timeout_ms = 10000;
    private ?string $event_id = null;
    private ?string $correlation_id = null;
    private ?string $causation_id = null;

    public function __construct(public Conf $conf)
    {
    }

    public function dispatch(object $event): object
    {
        $name = str_replace('\\', '.', $event::class);

        $producer = new Producer($this->conf);
        $topic = $producer->newTopic($name);

        $payload = $this->messageHandler($event);

        $topic->producev($this->partition, $this->msg_flags, $payload, null, [
            'event_id' => $this->getEventId(),
            'correlation_id' => $this->getCorrelationId(),
            'causation_id' => $this->getCausationId(),
        ]);

        while ($producer->getOutQLen() > 0) {
            $producer->poll($this->timeout_ms);
        }

        $this->event_id = null;
        $this->correlation_id = null;
        $this->causation_id = null;

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

    public function setPartition(int $partition): void
    {
        $this->partition = $partition;
    }

    public function setMsgFlags(int $msg_flags): void
    {
        $this->msg_flags = $msg_flags;
    }

    public function setTimeoutMs(int $timeout_ms): void
    {
        $this->timeout_ms = $timeout_ms;
    }
}
