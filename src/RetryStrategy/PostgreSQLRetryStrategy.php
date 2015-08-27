<?php

namespace Ez\DbLinker\RetryStrategy;

use Ez\DbLinker\RetryStrategy;
use Doctrine\DBAL\DBALException;
use Ez\DbLinker\Driver\Connection\MasterSlavesConnection;
use Ez\DbLinker\Driver\Connection\RetryConnection;
use Doctrine\DBAL\Exception\DriverException;
use stdClass;

class PostgreSQLRetryStrategy implements RetryStrategy
{
    public function shouldRetry(
        DBALException $exception,
        RetryConnection $connection,
        $method,
        array $arguments
    ) {
        return false;
    }
}
