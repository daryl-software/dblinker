<?php

namespace Ez\DbLinker\Driver;

use Ez\DbLinker\Driver\Connection\RetryConnection;

trait RetryDriver
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
        return new RetryConnection($params['connectionParams'], $params['retryStrategy']);
    }
}
