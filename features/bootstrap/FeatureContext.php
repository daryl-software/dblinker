<?php

use Doctrine\DBAL\DriverManager;

trait FeatureContext
{
    private $connections = [];

    /**
     * @BeforeScenario
     */
    public function clearConnections()
    {
        $this->connections = [
            '@master' => [
                'params' => $this->masterParams(),
                'instance' => null,
                'last-result' => null,
                'last-error' => null,
            ]
        ];
    }

    /**
     * @BeforeScenario
     */
    public function clearDatabase()
    {
        $params = $this->masterParams();
        $dbname = $params['dbname'];
        unset($params['dbname']);
        $connection = DriverManager::getConnection($params);
        $connection->exec("DROP DATABASE IF EXISTS $dbname");
        $connection->exec("CREATE DATABASE $dbname");
    }
    /**
     * @AfterScenario
     */
    public function closeConnections()
    {
        foreach ($this->connections as $name => $connection) {
            if (array_key_exists('instance', $connection) && ($instance = $connection['instance'])) {
                $instance->close();
            }
        }
    }

    /**
     * @Given a master\/slaves connection :connectionName with :slaveCount slaves
     */
    public function aMasterSlavesConnectionWithSlaves($connectionName, $slaveCount)
    {
        $master = $this->masterParams();

        $slaveCount = (int) $slaveCount;
        $slaves = [];
        while ($slaveCount--) {
            $master['weight'] = $slaveCount;
            $slaves[] = $master;
        }

        $params = [
            'master' => $master,
            'slaves' => $slaves,
            'driverClass' => 'Ez\DbLinker\Driver\MysqlMasterSlavesDriver',
        ];
        $this->connections[$connectionName] = [
            'params' => $params,
            'instance' => null,
            'last-result' => null,
            'last-error' => null,
        ];
    }

    /**
     * @Given a retry master\/slaves connection :connectionName with :slaveCount slaves limited to :n retry
     * @Given a retry master\/slaves connection :connectionName with :slaveCount slaves limited to :n retries
     */
    public function aRetryMasterSlavesConnectionWithSlavesLimitedToRetries($connectionName, $slaveCount, $n)
    {
        $master = $this->masterParams();

        $slaveCount = (int) $slaveCount;
        $slaves = [];
        while ($slaveCount--) {
            $master['weight'] = 1;
            $slaves[] = $master;
        }

        $params = [
            'driverClass' => 'Ez\DbLinker\Driver\MysqlRetryDriver',
            'connectionParams' => [
                'master' => $master,
                'slaves' => $slaves,
                'driverClass' => 'Ez\DbLinker\Driver\MysqlMasterSlavesDriver',
            ],
            'retryStrategy' => new MysqlRetryStrategy($n),
        ];
        $this->connections[$connectionName] = [
            'params' => $params,
            'instance' => null,
            'last-result' => null,
            'last-error' => null,
        ];
    }

    /**
     * @Given a retry master\/slaves connection :connectionName with :slaveCount slaves limited to :n retry with username :userName
     * @Given a retry master\/slaves connection :connectionName with :slaveCount slaves limited to :n retries with username :userName
     */
    public function aRetryMasterSlavesConnectionWithSlavesLimitedToRetriesWithUsername($connectionName, $slaveCount, $n, $userName)
    {
        $master = $this->masterParams();
        $master['user'] = $userName;

        $slaveCount = (int) $slaveCount;
        $slaves = [];
        while ($slaveCount--) {
            $master['weight'] = 1;
            $slaves[] = $master;
        }

        $params = [
            'driverClass' => 'Ez\DbLinker\Driver\MysqlRetryDriver',
            'connectionParams' => [
                'master' => $master,
                'slaves' => $slaves,
                'driverClass' => 'Ez\DbLinker\Driver\MysqlMasterSlavesDriver',
            ],
            'retryStrategy' => new MysqlRetryStrategy($n),
        ];
        $this->connections[$connectionName] = [
            'params' => $params,
            'instance' => null,
            'last-result' => null,
            'last-error' => null,
        ];
    }

    /**
     * @Given requests are forced on master for :connectionName
     * @When I force requests on master for :connectionName
     */
    public function requestsAreForcedOnMasterFor($connectionName)
    {
        $connection = $this->getWrappedConnection($connectionName);
        if ($connection instanceof Ez\DbLinker\Driver\Connection\RetryConnection) {
            $connection = $connection->wrappedConnection()->getWrappedConnection();
        }
        $connection->connectToMaster();
    }

    /**
     * @When I force requests on slave for :connectionName
     */
    public function iForceRequestsOnSlaveFor($connectionName)
    {
        $this->getWrappedConnection($connectionName)->connectToSlave();
    }

    /**
     * @When I query :sql on :connectionName
     */
    public function iQueryOn($connectionName, $sql)
    {
        $this->connections[$connectionName]['last-result'] = null;
        $this->connections[$connectionName]['last-error']  = null;
        try {
            $this->connections[$connectionName]['last-result'] = $this->getConnection($connectionName)->query($sql);
        } catch (\Exception $e) {
            $this->connections[$connectionName]['last-error'] = $e;
        }
    }

    /**
     * @When I create a deadlock on :connectionName with :connectionNameFork
     */
    public function iCreateADeadlockOn($connectionName, $connectionNameFork)
    {
        $this->iExecOn($connectionName, 'CREATE TABLE test_deadlock (id INT PRIMARY KEY) Engine=InnoDb');
        $this->iExecOn($connectionName, 'SET AUTOCOMMIT = 0');
        $this->iExecOn($connectionNameFork, 'SET AUTOCOMMIT = 0');

        $this->iExecOn($connectionNameFork, 'INSERT INTO test_deadlock VALUES (1)');

        $pid = pcntl_fork();
        if ($pid) {
            $this->iQueryOn($connectionName, 'SELECT * FROM test_deadlock FOR UPDATE');

            pcntl_waitpid($pid, $status);
        } else {
            usleep(50000);
            $this->iExecOn($connectionNameFork, 'INSERT INTO test_deadlock VALUES (0)');
            usleep(1000000);
            exit;
        }
    }

    /**
     * @When I exec :sql on :connectionName
     */
    public function iExecOn($connectionName, $sql)
    {
        $this->connections[$connectionName]['last-result'] = null;
        $this->connections[$connectionName]['last-error']  = null;
        try {
            $this->connections[$connectionName]['last-result'] = $this->getConnection($connectionName)->exec($sql);
        } catch (\Exception $e) {
            $this->connections[$connectionName]['last-error'] = $e;
        }
    }

    /**
     * @Then :connectionName is on slave
     */
    public function isOnSlave($connectionName)
    {
        assert(!$this->getWrappedConnection($connectionName)->isConnectedToMaster());
    }

    /**
     * @Then :connectionName is on master
     */
    public function isOnMaster($connectionName)
    {
        assert($this->getWrappedConnection($connectionName)->isConnectedToMaster());
    }

    /**
     * @When I start a transaction on :connectionName
     * @Given a transaction is started on :connectionName
     */
    public function aTransactionIsStartedOn($connectionName)
    {
        $this->getConnection($connectionName)->beginTransaction();
    }

    /**
     * @Given a retry connection :connectionName limited to :n retry
     * @Given a retry connection :connectionName limited to :n retries
     */
    public function aRetryConnection($connectionName, $n)
    {
        $params = [
            'driverClass' => 'Ez\DbLinker\Driver\MysqlRetryDriver',
            'connectionParams' => $this->masterParams(),
            'retryStrategy' => new MysqlRetryStrategy($n),
        ];
        $this->connections[$connectionName] = [
            'params' => $params,
            'instance' => null,
            'last-result' => null,
            'last-error' => null,
        ];
    }

    /**
     * @Given a retry connection :connectionName limited to :n retry with username :username
     */
    public function aRetryConnectionLimitedToRetryWithUsername($connectionName, $n, $username)
    {
        $params = [
            'driverClass' => 'Ez\DbLinker\Driver\MysqlRetryDriver',
            'connectionParams' => $this->masterParams(),
            'retryStrategy' => new MysqlRetryStrategy($n),
        ];
        $params['connectionParams']['user'] = $username;
        $this->connections[$connectionName] = [
            'params' => $params,
            'instance' => null,
            'last-result' => null,
            'last-error' => null,
        ];
    }

    /**
     * @Given a retry connection :connectionName limited to :n retry with db :db
     */
    public function aRetryConnectionLimitedToRetryWithDb($connectionName, $n, $db)
    {
        $params = [
            'driverClass' => 'Ez\DbLinker\Driver\MysqlRetryDriver',
            'connectionParams' => $this->masterParams(),
            'retryStrategy' => new MysqlRetryStrategy($n),
        ];
        $params['connectionParams']['dbname'] = $db;
        $this->connections[$connectionName] = [
            'params' => $params,
            'instance' => null,
            'last-result' => null,
            'last-error' => null,
        ];
    }


    /**
     * @Given MySQL has Gone Away on :connectionName
     */
    public function mysqlHasGoneAwayOn($connectionName)
    {
        $this->getConnection($connectionName)->exec('SET SESSION WAIT_TIMEOUT=1');
        usleep(1100000);
    }

    /**
     * @Then :connectionName retry limit should be :n
     */
    public function retryLimitShouldBe($connectionName, $n)
    {
        $retryLimit = $this->getWrappedConnection($connectionName)->retryStrategy()->retryLimit();
        assert($retryLimit === (int) $n, "Retry limit is $retryLimit, $n expected.");
    }

    /**
     * @Given there is a table :tableName on :connectionName
     */
    public function thereIsATableOn($tableName, $connectionName)
    {
        $connection = $this->getConnection($connectionName);
        $sql = <<<SQL
    CREATE TABLE $tableName (id INTEGER(10), n INTEGER(10)) Engine=InnoDb
SQL;
        $connection->exec($sql);
    }

    /**
     * @Given there is a row Lock on table :tableName on :connectionName
     */
    public function thereIsARowLockOn($tableName, $connectionName)
    {
        $connection = $this->getConnection($connectionName);
        $connection->exec("INSERT INTO $tableName (id, n) VALUES (1, 1)");
        $connection->beginTransaction();
        $connection->exec("UPDATE $tableName SET n = 2");
    }

    /**
     * @When I commit the transaction on :connectionName
     */
    public function iCommitTheTransactionOn($connectionName)
    {
        $this->getConnection($connectionName)->commit();
    }

    /**
     * @Then the last query succeeded on :connectionName
     */
    public function theLastQuerySucceededOn($connectionName)
    {
        if ($this->connections[$connectionName]['last-result'] === null) {
            $lastError = $this->connections[$connectionName]['last-error'];
            if ($lastError instanceof \Exception) {
                $message = $lastError->getMessage();
            } else if ($lastError !== null) {
                $message = print_r($lastError, true);
            } else {
                $message = print_r($lastError, true);
            }
            assert(false, $message);
        }
    }

    /**
     * @Then the last error code should be :errorCode on :connectionName
     */
    public function theLastErrorCodeShouldBeOn($expectedErrorCode, $connectionName)
    {
        $errorCodeAssertFailureMessage = "No error found, error code $expectedErrorCode expected";
        $exception = $this->connections[$connectionName]['last-error'];
        if ($exception === null) {
            $exception = $this->getWrappedConnection($connectionName)->retryStrategy()->lastError();
        }
        $errorCode = null;
        while ($exception !== null && !($exception instanceof \Doctrine\DBAL\Exception\DriverException)) {
            $exception = $exception->getPrevious();
        }
        if ($exception !== null) {
            $errorCode = $exception->getErrorCode();
            $errorCodeAssertFailureMessage = "Error code is $errorCode, error code $expectedErrorCode expected";
        }
        assert($errorCode === (int) $expectedErrorCode, $errorCodeAssertFailureMessage);
    }

    /**
     * @Then the last query failed on :connectionName
     */
    public function theLastQueryFailedOn($connectionName)
    {
        assert($this->connections[$connectionName]['last-result'] === null);
    }


    private function getWrappedConnection($connectionName)
    {
        return $this->getConnection($connectionName)->getWrappedConnection();
    }

    private function getConnection($connectionName)
    {
        if ($this->connections[$connectionName]['instance'] === null) {
            $this->connections[$connectionName]['instance'] = DriverManager::getConnection(
                $this->connections[$connectionName]['params']
            );
        }
        return $this->connections[$connectionName]['instance'];
    }

    abstract protected function masterParams();
}

class MysqlRetryStrategy extends Ez\DbLinker\RetryStrategy\MysqlRetryStrategy
{
    private $lastError = null;

    public function shouldRetry(
        Doctrine\DBAL\DBALException $exception,
        Ez\DbLinker\Driver\Connection\RetryConnection $connection,
        Doctrine\DBAL\Driver\Connection $wrappedConnection,
        $method,
        Array $arguments
    ) {
        $this->lastError = $exception;
        return parent::shouldRetry($exception, $connection, $wrappedConnection, $method, $arguments);
    }

    public function lastError()
    {
        return $this->lastError;
    }
}
