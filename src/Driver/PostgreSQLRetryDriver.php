<?php

namespace Ez\DbLinker\Driver;

use Doctrine\DBAL\Driver\AbstractPostgreSQLDriver;

class PostgreSQLRetryDriver extends AbstractPostgreSQLDriver
{
    use RetryDriver;

    /**
     * Gets the name of the driver.
     *
     * @return string The name of the driver.
     */
    public function getName() {
        return 'postgresql-retry';
    }
}
