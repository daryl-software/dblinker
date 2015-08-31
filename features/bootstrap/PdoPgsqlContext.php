<?php

use Behat\Behat\Context\Context;
use Behat\Behat\Context\SnippetAcceptingContext;

class PdoPgsqlContext implements Context, SnippetAcceptingContext
{
    use FeatureContext;
    use PostgreSQLContext;

    private function params(Array $params)
    {
        $params['driver'] = 'pdo_pgsql';
        return $params;
    }
}
