<?php

namespace Ez\DbLinker;

use Exception;
use Ez\DbLinker\Driver\Connection\RetryConnection;

interface RetryStrategy
{
    public function shouldRetry(
        Exception $exception,
        RetryConnection $connection
    );
}
