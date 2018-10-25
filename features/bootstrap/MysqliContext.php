<?php

use Behat\Behat\Context\Context;

class MysqliContext implements Context
{
    use FeatureContext;
    use MySQLContext;

    private function params(Array $params)
    {
        $params['driver'] = 'mysqli';
        return $params;
    }
}
