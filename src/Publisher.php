<?php
namespace byTorsten\PubSub\Redis;

use Clue\Redis\Protocol\Factory as RedisProtocolFactory;
use Clue\Redis\Protocol\Serializer\SerializerInterface;
use Socket\Raw\Factory as SocketFactory;
use Socket\Raw\Exception as SocketException;
use Socket\Raw\Socket;

class Publisher
{
    /**
     * @var string
     */
    protected $address;

    /**
     * @var Socket
     */
    protected $client;

    /**
     * @var SerializerInterface
     */
    protected $serializer;

    /**
     * @param string $address
     */
    public function __construct(string $address = null)
    {
        $this->address = $address;
        $factory = new RedisProtocolFactory();
        $this->serializer = $factory->createSerializer();
        register_shutdown_function([$this, 'shutdownClient']);
    }

    /**
     * @param string $address
     */
    public function setAddress(string $address): void
    {
        $this->address = $address;
    }

    /**
     * @return Socket
     */
    public function getClient(): Socket
    {
        if ($this->client === null) {
            $factory = new SocketFactory();
            $client = $factory->createClient($this->address);
            $client->setBlocking(false);
            $this->client = $client;
        }

        return $this->client;
    }

    /**
     * @param string $channelName
     * @param $payload
     */
    public function publish(string $channelName, $payload)
    {
        $encodedPayload = json_encode($payload);
        $message = $this->serializer->getRequestMessage('publish', [$channelName, $encodedPayload]);
        $client = $this->getClient();

        // automatically retry on disconnect
        try {
            $client->write($message);
        } catch (SocketException $exception) {
            $this->shutdownClient();
            $client = $this->getClient();
            $client->write($message);
        }
    }

    /**
     *
     */
    public function shutdownClient()
    {
        if ($this->client) {
            $this->client->close();
            $this->client = null;
        }
    }
}
