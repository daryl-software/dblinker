<?php

namespace Ez\DbLinker\Driver;

use Doctrine\DBAL\Connection;
use Ez\DbLinker\Driver\Connection\ConnectionWrapper;

trait WrapperDriver
{
    /**
     * Gets the SchemaManager that can be used to inspect and change the underlying
     * database schema of the platform this driver connects to.
     *
     * @param \Doctrine\DBAL\Connection $connection
     *
     * @return \Doctrine\DBAL\Schema\AbstractSchemaManager
     */
    public function getSchemaManager(Connection $connection) {
        return $this->callOnWrappedDriver($connection, __FUNCTION__);
    }

    /**
     * Gets the name of the database connected to for this driver.
     *
     * @param \Doctrine\DBAL\Connection $connection
     *
     * @return string The name of the database.
     */
    public function getDatabase(Connection $connection) {
        return $this->callOnWrappedDriver($connection, __FUNCTION__);
    }

    private function callOnWrappedDriver(Connection $connection, $method)
    {
        $connection = $connection->getWrappedConnection();
        if (!$connection instanceof ConnectionWrapper) {
            $class = get_class($connection);
            $interface = ConnectionWrapper::class;
            throw new WrapperException(
                "This driver cannot $method on $class because it does not implements $interface"
            );
        }
        $driver = $connection->wrappedDriver();
        $connection = new Connection(
            ['pdo' => $connection->wrappedConnection()],
            $driver
        );
        return $driver->$method($connection);
    }
}
