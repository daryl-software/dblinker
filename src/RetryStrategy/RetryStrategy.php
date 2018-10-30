<?php

namespace Ez\DbLinker\RetryStrategy;

use Exception;
use Ez\DbLinker\Slave;
use stdClass;
use Ez\DbLinker\Driver\Connection\MasterSlavesConnection;
use Ez\DbLinker\Driver\Connection\RetryConnection;

trait RetryStrategy
{
    private $retryLimit;

    public function __construct($retryLimit = INF)
    {
        $this->retryLimit = $retryLimit;
    }

    public function shouldRetry(Exception $exception, RetryConnection $connection) {
        if (!$this->canRetry($connection)) {
            return false;
        }
        $strategy = $this->errorCodeStrategy($this->errorCode($exception));
        $res = $this->applyStrategy($strategy, $connection);
        return $res;
    }

    public function retryLimit()
    {
        return $this->retryLimit > 0 ? (int) $this->retryLimit : 0;
    }

    private function canRetry(RetryConnection $connection)
    {
        return $this->retryLimit > 0 && $connection->transactionLevel() === 0;
    }

    private function errorCodeStrategy($errorCode)
    {
        $strategy = (object) [
            'retry' => true,
            'wait' => 0,
            'changeServer' => false,
            'reconnect' => false,
        ];
        $errorCodeStrategies = $this->errorCodeStrategies();
        if (array_key_exists($errorCode, $errorCodeStrategies)) {
            foreach ($errorCodeStrategies[$errorCode] as $behavior => $value) {
                $strategy->$behavior = $value;
            }
            return $strategy;
        }
        return (object) ['retry' => false];
    }

    private function applyStrategy(stdClass $strategy, RetryConnection $connection) {
        if ($strategy->retry === false || !$this->changeServer($strategy, $connection)) {
            return false;
        }
        sleep($strategy->wait);
        $this->reconnect($strategy, $connection);
        $this->retryLimit--;
        return true;
    }

    private function changeServer(stdClass $strategy, RetryConnection $connection): bool
    {
        if (!$strategy->changeServer) {
            return true;
        }
        /** @var MasterSlavesConnection $wrappedConnection */
        $wrappedConnection = $connection->wrappedConnection();
        if ($wrappedConnection instanceof MasterSlavesConnection && $wrappedConnection->getLastConnection() instanceOf Slave) {
            $wrappedConnection->getLastConnection()->disable();
            return true;
        }
        return false;
    }

    private function reconnect(stdClass $strategy, RetryConnection $connection): void
    {
        if ($strategy->reconnect) {
            $connection->close();
        }
    }

    protected abstract function errorCodeStrategies();
    protected abstract function errorCode(Exception $exception);
}
