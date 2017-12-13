<?php

use Behat\Behat\Context\Context;
use Behat\Behat\Context\SnippetAcceptingContext;

class MysqlReplicContext implements Context, SnippetAcceptingContext
{
    use FeatureContext;
    use MySQLContext;

    private function masterParams($username = null, $password = '') {
        $params = [
            'host'          => '192.168.0.8',
            'user'          => $username === null ? 'mcm' : $username,
            'password'      => 'uvieng7c',
            'dbname'        => 'mcm',
        ];
        return $this->params($params);
    }
    /**
     * @BeforeScenario
     */
    public function clearConnections() {
    }

    /**
     * @BeforeScenario
     */
    public function clearDatabase() {
    }

    /**
     * @BeforeScenario
     */
    public function assertNoActiveConnection() {
    }

    private function params(Array $params)
    {
        $params['driver'] = 'mysqli';
        return $params;
    }

    private function defaultDatabaseName()
    {
        return 'mcm';
    }
}
