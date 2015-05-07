<?php

namespace Ez\DbLinker\Driver;

use Doctrine\DBAL\Driver\AbstractMySQLDriver;

class MysqlMasterSlavesDriver extends AbstractMySQLDriver
{
    use MasterSlavesDriver;

    /**
     * Gets the name of the driver.
     *
     * @return string The name of the driver.
     */
    public function getName() {
        return 'mysql-master-slaves';
    }
}
