<?php

use Behat\Behat\Context\Context;
use Behat\Behat\Context\SnippetAcceptingContext;

class PdoMysqlContext implements Context, SnippetAcceptingContext
{
    use FeatureContext;

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
