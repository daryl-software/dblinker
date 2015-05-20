<?php

namespace Ez\DbLinker\RetryStrategy;

use Ez\DbLinker\RetryStrategy;
use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\DBALException;
use Ez\DbLinker\Driver\Connection\RetryConnection;
use Ez\DbLinker\Driver\Connection\MasterSlavesConnection;
use Doctrine\DBAL\Exception\DriverException;
use stdClass;

class MysqlRetryStrategy implements RetryStrategy
{
    private $retryLimit;

    private $errorCodeStrategies = [
        1152 => ['wait' => 1],
        1205 => ['wait' => 1],
        1044 => ['changeServer' => true],
        1045 => ['changeServer' => true],
        2006 => ['reconnect' => true],
    ];

    public function __construct($retryLimit = INF)
    {
        $this->retryLimit = $retryLimit;
    }

    private function errorCodeStrategy($errorCode)
    {
        $strategy = (object) [
            'wait' => 0,
            'changeServer' => false,
            'reconnect' => false,
        ];
        if (array_key_exists($errorCode, $this->errorCodeStrategies)) {
            foreach ($this->errorCodeStrategies[$errorCode] as $behavior => $value) {
                $strategy->$behavior = $value;
            }
            return $strategy;
        }
        return false;
    }

    private function errorCode(DBALException $exception)
    {
        while ($exception !== null) {
            if ($exception instanceof DriverException) {
                return $exception->getErrorCode();
            }
            $exception = $exception->getPrevious();
        }
    }

    private function changeServer(stdClass $strategy, Connection $connection)
    {
        if (!$strategy->changeServer) {
            return true;
        }
        if ($connection instanceof MasterSlavesConnection && !$connection->isConnectedToMaster()) {
            $connection->changeSlave();
            return true;
        }
        return false;
    }

    private function reconnect(stdClass $strategy, RetryConnection $connection)
    {
        if ($strategy->reconnect) {
            $connection->close();
        }
    }

    private function applyStrategy(stdClass $strategy, RetryConnection $connection, Connection $wrappedConnection) {
        if ($strategy === false || !$this->changeServer($strategy, $wrappedConnection)) {
            return false;
        }
        sleep($strategy->wait);
        $this->reconnect($strategy, $connection);
        $this->retryLimit--;
        return true;
    }

    public function shouldRetry(
        DBALException $exception,
        RetryConnection $connection,
        Connection $wrappedConnection,
        $method,
        Array $arguments
    ) {
        if ($connection->transactionLevel() > 0 || $this->retryLimit < 1) {
            return false;
        }
        $strategy = $this->errorCodeStrategy($this->errorCode($exception));
        return $this->applyStrategy($strategy, $connection, $wrappedConnection);
    }

    public function retryLimit()
    {
        return $this->retryLimit > 0 ? (int) $this->retryLimit : 0;
    }
}
