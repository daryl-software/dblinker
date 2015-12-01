<?php

namespace Ez\DbLinker\Driver\Connection;

use Exception;
use Ez\DbLinker\RetryStrategy;

trait CallAndRetry
{
    /**
     * call $callable and retry if necessary
     */
    private function callAndRetry(callable $callable, RetryStrategy $strategy, RetryConnection $connection)
    {
        do {
            try {
                return @$callable();
            } catch (Exception $exception) {
                if (!$strategy->shouldRetry(
                    $exception,
                    $connection
                )) {
                    // stop trying
                    throw $exception;
                }
            }
        } while (true);
    }
}
