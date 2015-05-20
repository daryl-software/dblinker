<?php

namespace Ez\DbLinker\Driver;

use \Doctrine\DBAL\Connection;
use \Doctrine\DBAL\Driver;
use \Doctrine\DBAL\DriverManager;
use Ez\DbLinker\Driver\Connection\MasterSlavesConnection;
use SplObjectStorage;
use Exception;

trait MasterSlavesDriver
{
    /**
     * Attempts to create a connection with the database.
     *
     * @param array       $params        All connection parameters passed by the user.
     * @param string|null $username      The username to use when connecting.
     * @param string|null $password      The password to use when connecting.
     * @param array       $driverOptions The driver options to use when connecting.
     *
     * @return \Doctrine\DBAL\Driver\Connection The database connection.
     */
    public function connect(Array $params, $username = null, $password = null, Array $driverOptions = []) {
        $driverKey = array_key_exists('driverClass', $params['master']) ? 'driverClass' : 'driver';
        $driverValue = $params['master'][$driverKey];
        $master = DriverManager::getConnection($params['master']);
        $slaves = new SplObjectStorage;
        foreach($params['slaves'] as $slaveParams) {
            $slaveParams[$driverKey] = $driverValue;
            $slaves->attach(DriverManager::getConnection($slaveParams), $slaveParams['weight']);
        }
        return new MasterSlavesConnection($master, $slaves);
    }

    /**
     * Gets the SchemaManager that can be used to inspect and change the underlying
     * database schema of the platform this driver connects to.
     *
     * @param \Doctrine\DBAL\Connection $conn
     *
     * @return \Doctrine\DBAL\Schema\AbstractSchemaManager
     */
    public function getSchemaManager(Connection $conn) {
        return $this->wrappedDriver($conn)->getSchemaManager($conn);
    }

    /**
     * Gets the name of the database connected to for this driver.
     *
     * @param \Doctrine\DBAL\Connection $conn
     *
     * @return string The name of the database.
     */
    public function getDatabase(Connection $conn) {
        return $this->wrappedDriver($conn)->getDatabase($conn);
    }

    private function wrappedDriver(Connection $connection)
    {
        return $connection->getCurrentConnection()->getDriver();
    }
}
