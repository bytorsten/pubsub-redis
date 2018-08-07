<?php
namespace byTorsten\PubSub\Redis;

use Clue\React\Redis\Client as Redis;
use Clue\React\Redis\Factory as RedisFactory;
use React\EventLoop\LoopInterface;
use React\Promise\PromiseInterface;
use byTorsten\GraphQL\Subscriptions\PubSub\PubSubAsyncIterator;
use byTorsten\GraphQL\Subscriptions\Iterator\AsyncIteratorInterface;
use byTorsten\GraphQL\Subscriptions\PubSubInterface;

class PubSub implements PubSubInterface
{
    /**
     * @var string
     */
    protected $address;

    /**
     * @var Redis
     */
    protected $subscriptionClient;

    /**
     * @var Redis
     */
    protected $publicationClient;

    /**
     * @var array
     */
    protected $subscriptions = [];

    /**
     * @var int
     */
    protected $subIdCounter = 0;

    /**
     * @param string $address
     */
    public function setAddress(string $address): void
    {
        $this->address = $address;
    }

    /**
     * @param string $triggerName
     * @param $payload
     * @return bool
     */
    public function publish(string $triggerName, $payload): bool
    {
        $this->publicationClient->publish($triggerName, json_encode($payload));
        return true;
    }

    /**
     * @param string $triggerName
     * @param callable $onMessage
     * @param array $options
     * @return PromiseInterface
     */
    public function subscribe(string $triggerName, callable $onMessage, array $options = []): PromiseInterface
    {
        $wrappedOnMessage = function (string $channelName, $payload) use ($triggerName, $onMessage) {
            if ($channelName === $triggerName) {
                $parsedPayload = json_decode($payload, true);
                $onMessage($parsedPayload);
            }
        };

        $this->subscriptionClient->on('message', $wrappedOnMessage);
        $this->subIdCounter += 1;
        $this->subscriptions[$this->subIdCounter] = [$triggerName, $wrappedOnMessage];

        return $this->subscriptionClient->subscribe($triggerName);
    }

    /**
     * @param int $subId
     */
    public function unsubscribe(int $subId): void
    {
        if (isset($this->subscriptions[$subId])) {
            [$triggerName, $onMessage] = $this->subscriptions[$subId];
            unset($this->subscriptions[$subId]);
            $this->subscriptionClient->removeListener('message', $onMessage);
            $this->subscriptionClient->unsubscribe($triggerName);
        }
    }

    /**
     * @param array $triggers
     * @return AsyncIteratorInterface
     */
    public function asyncIterator(array $triggers): AsyncIteratorInterface
    {
        return new PubSubAsyncIterator($this, $triggers);
    }

    /**
     * @param LoopInterface $loop
     * @return PromiseInterface
     * @throws \Exception
     */
    public function setup(LoopInterface $loop): PromiseInterface
    {
        $factory = new RedisFactory($loop);
        return $factory->createClient($this->address)->then(function (Redis $subscriptionClient) use ($factory) {
            $this->subscriptionClient = $subscriptionClient;

            return $factory->createClient($this->address)->then(function (Redis $publicationClient) use ($subscriptionClient) {
                $this->publicationClient = $publicationClient;

                return [$subscriptionClient, $publicationClient];
            });
        });
    }
}
