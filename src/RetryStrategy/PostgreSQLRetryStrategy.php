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
        while ($exception !== null) {
            if ($exception instanceof DriverException) {
                preg_match("/SQLSTATE\[(?<errorCode>[A-Z0-9]*)\]/", $exception->getMessage(), $matches);
                if (array_key_exists("errorCode", $matches)) {
                    return $matches["errorCode"];
                }
            }
            $exception = $exception->getPrevious();
        }
    }
}
