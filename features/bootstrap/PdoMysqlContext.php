<?php

use Behat\Behat\Context\Context;

class PdoMysqlContext implements Context
{
    use FeatureContext;
    use MySQLContext;

    private function params(Array $params)
    {
        $params['driver'] = 'pdo_mysql';
        $params['driverOptions'] = [
            // todo move to retry driver
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ];
        return $params;
    }
}
