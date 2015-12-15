<?php

namespace Ez\DbLinker\Driver\Connection;

interface ConnectionWrapper
{
    /**
     * @return \Doctrine\DBAL\Driver\Connection
     */
    public function wrappedConnection();

    /**
     * @return \Doctrine\DBAL\Driver
     */
    public function wrappedDriver();
}
