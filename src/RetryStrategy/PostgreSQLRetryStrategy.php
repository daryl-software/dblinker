<?php

namespace Ez\DbLinker\RetryStrategy;

use Exception;
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

    private function errorCode(Exception $exception)
    {
        if (preg_match("/SQLSTATE\[(?<errorCode>[A-Z0-9]*)\]/", $exception->getMessage(), $matches)) {
            return $matches["errorCode"];
        }
    }
}
