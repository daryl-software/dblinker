<?php

namespace Ez\DbLinker\Driver;

use Ez\DbLinker\Driver\Connection\MasterSlavesConnection;

trait MasterSlavesDriver
{
    use WrapperDriver;

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
        $slaves = [];
        foreach ($params['slaves'] as $slaveParams) {
            $slaveParams[$driverKey] = $driverValue;
            $slaves[] = $slaveParams;
        }
        return new MasterSlavesConnection($params['master'], $slaves);
    }
}
