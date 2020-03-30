<?php

namespace Ez\DbLinker\Driver\Connection;

use Exception;
use Ez\DbLinker\RetryStrategy;

trait CallAndRetry
{
    /**
     * call $callable and retry if necessary
     * @param callable $callable
     * @param RetryStrategy $strategy
     * @param RetryConnection $connection
     * @return
     * @throws Exception
     */
    private function callAndRetry(callable $callable, RetryStrategy $strategy, RetryConnection $connection)
    {
        do {
            try {
                return @$callable();
            } catch (Exception $exception) {
                if (!$strategy->shouldRetry($exception, $connection)) {
                    // stop trying
                    throw $exception;
                }
            }
        } while (true);
    }
}
