<?php

namespace Ez\DbLinker\Driver;

use Doctrine\DBAL\Driver\AbstractMySQLDriver;

class MysqlRetryDriver extends AbstractMySQLDriver
{
    use RetryDriver;

    /**
     * Gets the name of the driver.
     *
     * @return string The name of the driver.
     */
    public function getName() {
        return 'mysql-retry';
    }
}
