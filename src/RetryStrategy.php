<?php

namespace Ez\DbLinker;

use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\DBALException;
use Ez\DbLinker\Driver\Connection\RetryConnection;

interface RetryStrategy
{
    public function shouldRetry(
        DBALException $exception,
        RetryConnection $connection,
        $method,
        Array $arguments
    );
}
