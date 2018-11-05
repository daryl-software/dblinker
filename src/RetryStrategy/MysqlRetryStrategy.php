<?php

namespace Ez\DbLinker\RetryStrategy;

use Exception;
use Doctrine\DBAL\Driver\DriverException as DDriverException;
use Doctrine\DBAL\Exception\DriverException as EDriverException;
use Ez\DbLinker\RetryStrategy as RetryStrategyInterface;

class MysqlRetryStrategy implements RetryStrategyInterface
{
    use RetryStrategy;

    private function errorCodeStrategies() {
        return [
            // ER_CON_COUNT_ERROR
            1040 => ["wait" => 1],
            // ER_CON_USER_COUNT_ERROR
            1203 => ["wait" => 1],
            // ER_DBACCESS_DENIED_ERROR
            1044 => ["changeServer" => true],
            // ER_ACCESS_DENIED_ERROR
            1045 => ["changeServer" => true],
            // ER_BAD_DB_ERROR
            1049 => ["changeServer" => true],
            // ER_ABORTING_CONNECTION
            1152 => ["wait" => 1],
            // ER_LOCK_WAIT_TIMEOUT
            1205 => ["wait" => 1],
            // ER_LOCK_WAIT_TIMEOUT
            1213 => ["wait" => 1],
            // CR_SERVER_GONE_ERROR
            2006 => ["reconnect" => true],
        ];
    }

    private function errorCode(Exception $exception)
    {
        if ($exception instanceof DDriverException || $exception instanceof EDriverException) {
            return $exception->getErrorCode();
        }
    }
}
