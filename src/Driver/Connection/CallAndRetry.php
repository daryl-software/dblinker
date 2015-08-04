<?php

namespace Ez\DbLinker\Driver\Connection;

use Doctrine\DBAL\DBALException;

trait CallAndRetry
{
    /**
     * call $method woth $arguments and retry if necessary
     * @param  string $method    method name
     * @param  array  $arguments [description]
     */
    private function callAndRetry($method, array $arguments)
    {
        do {
            try {
                return @call_user_func_array([$this->wrappedObject(), $method], $arguments);
            } catch (DBALException $exception) {
                if (!$this->retryStrategy->shouldRetry(
                    $exception,
                    $this->retryConnection(),
                    $method,
                    $arguments
                )) {
                    // stop trying
                    throw $exception;
                }
            }
        } while (true);
    }

    /**
     * @return mixed
     */
    abstract protected function wrappedObject();

    /**
     * @return Ez\DbLinker\RetryConnection
     */
    abstract protected function retryConnection();

    /**
     * @return Ez\DbLinker\RetryStrategy
     */
    abstract protected function retryStrategy();
}
