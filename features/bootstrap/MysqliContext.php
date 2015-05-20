<?php

use Behat\Behat\Context\Context;
use Behat\Behat\Context\SnippetAcceptingContext;

class MysqliContext implements Context, SnippetAcceptingContext
{
    use FeatureContext;

    private function masterParams()
    {
        return [
            'host'          => getenv('DBLINKER_DB_1_PORT_3306_TCP_ADDR'),
            'user'          => getenv('DBLINKER_DB_1_ENV_MYSQL_USER'),
            'password'      => getenv('DBLINKER_DB_1_ENV_MYSQL_PASSWORD'),
            'dbname'        => getenv('DBLINKER_DB_1_ENV_MYSQL_DATABASE'),
            'driver'        => 'mysqli',
        ];
    }
}
