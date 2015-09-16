<?php

namespace Ez\DbLinker\Driver;

use Doctrine\DBAL\Driver\AbstractPostgreSQLDriver;

class PostgresqlMasterSlavesDriver extends AbstractPostgreSQLDriver
{
    use MasterSlavesDriver;

    /**
     * Gets the name of the driver.
     *
     * @return string The name of the driver.
     */
    public function getName() {
        return 'postgresql-master-slaves';
    }
}
