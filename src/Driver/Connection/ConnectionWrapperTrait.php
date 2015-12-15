<?php

namespace Ez\DbLinker\Driver\Connection;

use Closure;
use Exception;
use SplObjectStorage;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Driver\Connection;

trait ConnectionWrapperTrait
{
    private $wrappedConnection;
    private $wrappedDriver;

    /**
     * @inherit
     */
    public function wrappedConnection()
    {
        if ($this->wrappedConnection === null) {
            $this->wrap();
        }
        return $this->wrappedConnection;
    }

    public function wrappedDriver()
    {
        if ($this->wrappedDriver === null) {
            $this->wrap();
        }
        return $this->wrappedDriver;
    }

    abstract protected function wrap();
}
