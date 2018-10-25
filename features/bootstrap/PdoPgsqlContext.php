<?php

use Behat\Behat\Context\Context;

class PdoPgsqlContext implements Context
{
    use FeatureContext;
    use PostgreSQLContext;

    private function params(Array $params)
    {
        $params["driver"] = "pdo_pgsql";
        return $params;
    }
}
