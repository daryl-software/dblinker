<?php

namespace Ez\DbLinker\RetryStrategy;

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Exception\DriverException;
use Ez\DbLinker\RetryStrategy as RetryStrategyInterface;

class PostgreSQLRetryStrategy implements RetryStrategyInterface
{
    use RetryStrategy;

    private function errorCodeStrategies() {
        return [
            // CONNECTION FAILURE
            "08006" => ["changeServer" => true],
            // TOO MANY CONNECTIONS
            "53300" => ["wait" => 1],
        ];
    }

    private function errorCode(DBALException $exception)
    {
        if (preg_match("/SQLSTATE\[(?<errorCode>[A-Z0-9]*)\]/", $exception->getMessage(), $matches)) {
            return $matches["errorCode"];
        }
    }
}
