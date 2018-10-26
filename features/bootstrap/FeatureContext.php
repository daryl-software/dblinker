<?php

use Doctrine\DBAL\DriverManager;
use Ez\DbLinker\Driver\Connection\RetryConnection;

trait FeatureContext
{
    private $connections = [];
    private $statement;

    /**
     * @BeforeScenario
     */
    abstract public function clearConnections();

    /**
     * @BeforeScenario
     */
    public function clearDatabase()
    {
        $dbname = $this->masterParams()['dbname'];
        $connection = $this->rootConnection();
        $connection->exec("DROP DATABASE IF EXISTS $dbname");
        $connection->exec("CREATE DATABASE $dbname");
        $connection->close();
        $connection = null;
        gc_collect_cycles();
    }

    /**
     * @BeforeScenario
     */
    public function assertNoActiveConnection()
    {
        $n = $this->activeConnectionsCount();
        assert($n === 0, "There is $n active connection(s) on the test server");
    }

    abstract protected function activeConnectionsCount();

    private function rootConnection()
    {
        $params = $this->masterParams('root');
        unset($params['dbname']);
        return DriverManager::getConnection($params);
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
        $this->connections = [];
        gc_collect_cycles();
    }

    /**
     * @Given the server accept :n more connection
     * @Given the server accept :n more connections
     */
    abstract public function theServerAcceptMoreConnections($n);

    /**
     * @Given a master-slaves connection :connectionName with :slaveCount slaves
     */
    public function aMasterSlavesConnectionWithSlaves(string $connectionName, int $slaveCount)
    {
        $slaves = [];
        for ($i = 1; $i <= $slaveCount; $i++) {
            $slaves[] = $this->slaveParams($i) + ['weight' => rand(1, 20)];
        }

        $this->connections[$connectionName] = [
            'params' => [
                'master' => $this->masterParams(),
                'slaves' => $slaves,
                'driverClass' => $this->masterSlaveDriverClass(),
            ],
            'instance' => null,
            'last-result' => null,
            'last-error' => null,
        ];
    }

    /**
     * @Given a retry master-slaves connection :connectionName with :slaveCount slaves limited to :n retry
     * @Given a retry master-slaves connection :connectionName with :slaveCount slaves limited to :n retries
     * @Given a retry master-slaves connection :connectionName with :slaveCount slaves limited to :n retry with username :username
     * @Given a retry master-slaves connection :connectionName with :slaveCount slaves limited to :n retries with username :username
     */
    public function aRetryMasterSlavesConnectionWithSlavesLimitedToRetriesWithusername(string $connectionName, int $slaveCount, $n, $username = null)
    {
        $slaves = [];
        for ($i = 1; $i <= $slaveCount; $i++) {
            $slaves[] = $this->slaveParams($i) + ['weight' => rand(1, 20)];
        }

        $this->connections[$connectionName] = [
            'params' => [
                'driverClass' => $this->retryDriverClass(),
                'connectionParams' => [
                    'master' => $this->masterParams($username),
                    'slaves' => $slaves,
                    'driverClass' => $this->masterSlaveDriverClass(),
                ],
                'retryStrategy' => $this->retryStrategy($n),
            ],
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
        $connection->forceMaster(true);
    }

    /**
     * @When I force requests on slave for :connectionName
     */
    public function iForceRequestsOnSlaveFor($connectionName)
    {
        $this->getWrappedConnection($connectionName)->forceMaster(false);
    }

    /**
     * @When I query :sql with param :param on :connectionName
     */
    public function iQueryWithParamOn(string $sql, string $param, string $connectionName)
    {
        $this->connections[$connectionName]['last-result'] = null;
        $this->connections[$connectionName]['last-error']  = null;
        try {
            $connection = $this->getConnection($connectionName);
            $this->connections[$connectionName]['last-result'] = $connection->executeQuery($sql, [$param]);
        } catch (\Exception $e) {
            $this->connections[$connectionName]['last-error'] = $e;
        }
    }

    /**
     * @When I query :sql on :connectionName
     */
    public function iQueryOn($connectionName, $sql)
    {
        $this->connections[$connectionName]['last-result'] = null;
        $this->connections[$connectionName]['last-error']  = null;
        try {
            $connection = $this->getConnection($connectionName);
            $this->connections[$connectionName]['last-result'] = $connection->query($sql);
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
        $sql = $this->prepareSql($sql);
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
        /** @var \Doctrine\DBAL\Connection $cnx */
        $cnx = $this->getWrappedConnection($connectionName)->getLastConnection();
        assert($cnx->getParams()['user'] === 'slave_user');
    }

    /**
     * @Then :connectionName is on master
     */
    public function isOnMaster($connectionName)
    {
        /** @var \Doctrine\DBAL\Connection $cnx */
        $cnx = $this->getWrappedConnection($connectionName)->getLastConnection();
        assert($cnx->getParams()['user'] === 'master_user');
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
     * @Given a retry connection :connectionName limited to :n retry with username :username
     */
    public function aRetryConnectionLimitedToRetryWithusername($connectionName, $n, $username = null)
    {
        $params = [
            'driverClass' => $this->retryDriverClass(),
            'connectionParams' => $this->masterParams($username),
            'retryStrategy' => $this->retryStrategy($n),
        ];
        $this->connections[$connectionName] = [
            'params' => $params,
            'instance' => null,
            'last-result' => null,
            'last-error' => null,
        ];
    }

    /**
     * @Given a retry master-slaves connection :connectionName with :slaveCount slaves limited to :n retry with db :db
     * @Given a retry master-slaves connection :connectionName with :slaveCount slaves limited to :n retry with db :db and username :username
     */
    public function aRetryMasterSlavesConnectionWithSlavesLimitedToRetryWithDbAndUsername($connectionName, $slaveCount, $n, $db, $username = null)
    {
        $master = $this->masterParams($username);
        $master['dbname'] = $db;

        $slaveCount = (int) $slaveCount;
        $slaves = [];
        while ($slaveCount--) {
            $master['weight'] = 1;
            $slaves[] = $master;
        }

        $params = [
            'driverClass' => $this->retryDriverClass(),
            'connectionParams' => [
                'master' => $master,
                'slaves' => $slaves,
                'driverClass' => $this->masterSlaveDriverClass(),
            ],
            'retryStrategy' => $this->retryStrategy($n),
        ];
        $this->connections[$connectionName] = [
            'params' => $params,
            'instance' => null,
            'last-result' => null,
            'last-error' => null,
        ];
    }

    /**
     * @Given a retry connection :connectionName limited to :n retry with db :db
     * @Given a retry connection :connectionName limited to :n retry with db :db and username :username
     */
    public function aRetryConnectionLimitedToRetryWithDbAndUsername($connectionName, $n, $db, $username = null)
    {
        $master = $this->masterParams($username);
        $params = [
            'driverClass' => $this->retryDriverClass(),
            'connectionParams' => $master,
            'retryStrategy' => $this->retryStrategy($n),
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
     * @Given database has Gone Away on :connectionName
     */
    public function databaseHasGoneAwayOn($connectionName)
    {
        $this->getConnection($connectionName)->exec('SET SESSION WAIT_TIMEOUT=1');
        usleep(1100000);
    }

    /**
     * @Then :connectionName retry limit should be :n
     */
    public function retryLimitShouldBe($connectionName, $n)
    {
        $retryLimit = $this->connections[$connectionName]['params']['retryStrategy']->retryLimit();
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
     * @Then the last query failed on :connectionName
     */
    public function theLastQueryFailedOn($connectionName)
    {
        assert($this->connections[$connectionName]['last-result'] === null);
    }

    /**
     * @Then :connectionName should have :n slave
     * @Then :connectionName should have :n slaves
     */
    public function shouldHaveSlave($connectionName, $n)
    {
        $connection = $this->getWrappedConnection($connectionName);
        if ($connection instanceof \Ez\DbLinker\Driver\Connection\RetryConnection) {
            $connection = $connection->wrappedConnection();
        }
        $slaveCount = count($connection->slaves());
        assert($slaveCount === (int)$n, "Slaves count is $slaveCount, $n expected.");
    }

    /**
     * @Given a connection :connectionName
     * @Given a connection :connectionName with username :username
     */
    public function aConnectionWithusername($connectionName, $username = null)
    {
        $this->connections[$connectionName] = [
            'params' => $this->masterParams($username),
            'instance' => null,
            'last-result' => null,
            'last-error' => null,
        ];
    }

    /**
     * @When I prepare a statement :query on :connectionName
     */
    public function iPrepareAStatementOn($query, $connectionName)
    {
        $connection = $this->getConnection($connectionName);
        $this->statement = $connection->prepare($query);
        $this->statement->connection = $connection;
    }

    /**
     * @When I execute this statement
     */
    public function iExecuteThisStatement()
    {
        $this->lastStatementResult = null;
        $this->lastStatementError = null;
        try {
            $this->lastStatementResult = $this->statement->execute();
        } catch (\Exception $e) {
            $this->lastStatementError = $e;
        }
    }

    /**
     * @Then the last statement succeeded
     */
    public function theLastStatementSucceeded()
    {
        if ($this->lastStatementResult === null) {
            $lastError = $this->lastStatementError;
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
     * @Then the last error should be :errorName on :connectionName
     */
    public function theLastErrorShouldBeOn($errorName, $connectionName)
    {
        $errorCode = $this->errorCode(
            $this->connections[$connectionName]['last-error'] ?:
            $retryStrategy = $this->connections[$connectionName]['params']['retryStrategy']->lastError()
        );
        $this->errorCodeMatchesErrorName($errorCode, $errorName);
    }

    private function errorCodeMatchesErrorName($errorCode, $errorName)
    {
        $errorCodeAssertFailureMessage = "No error found, error $errorName expected";
        $expectedErrorCode = $this->errorToCode($errorName);
        if ($errorCode !== null) {
            $errorCodeAssertFailureMessage = "Error code is $errorCode, error code $expectedErrorCode expected";
        }
        assert((string) $errorCode === (string) $expectedErrorCode, $errorCodeAssertFailureMessage);
    }

    abstract protected function errorToCode($errorName);
    abstract protected function errorCode(Exception $exception);

    private function getWrappedConnection($connectionName)
    {
        return $this->getConnection($connectionName)->getWrappedConnection();
    }

    /**
     * @param $connectionName
     * @return Doctrine\DBAL\Connection
     * @throws \Doctrine\DBAL\DBALException
     */
    private function getConnection($connectionName)
    {
        if ($this->connections[$connectionName]['instance'] === null) {
            $this->connections[$connectionName]['instance'] = DriverManager::getConnection(
                $this->connections[$connectionName]['params']
            );
        }
        return $this->connections[$connectionName]['instance'];
    }

    protected abstract function params(Array $params);

    abstract protected function masterParams($username = null, $password = '');

    /**
     * @Given table :tableName can be created automatically on :connectionName
     */
    public function tableCanBeCreatedAutomaticallyOn($tableName, $connectionName)
    {
        $retryStrategy = $this->connections[$connectionName]['params']['retryStrategy'];
        $retryStrategy->addHandler(function (
            Exception $exception,
            RetryConnection $connection
        ) use ($tableName) {
            if (strpos($exception->getMessage(), $tableName) !== false) {
                $connection->exec("CREATE TABLE {$tableName} (id INT)");
                return true;
            }
        });
    }

    /**
     * @Given the cache is disable on :connectionName
     */
    public function theCacheIsDisable($connectionName)
    {
        $connection = $this->getWrappedConnection($connectionName);
        $connection->disableCache();
    }

    /**
     * @Given slave replication is stopped on :connectionName
     */
    public function slaveReplicationIsStopped($connectionName)
    {
        $connection = $this->getWrappedConnection($connectionName);
        if ($connection instanceof Ez\DbLinker\Driver\Connection\RetryConnection) {
            $connection = $connection->wrappedConnection();
        }
        $connection->connectToSlave();
        $connection->setSlaveStatus(false, 120);
        $connection->isSlaveOk();
    }

    /**
     * @Then there is :n connections established on :connectionName
     */
    public function thereIsConnections($connectionName, $n)
    {
        $connection = $this->getConnection($connectionName);
        $connections = $connection->query("SELECT count(*) as n FROM pg_stat_activity")->fetch()['n'];
        assert($n == $connections, "There is $connections active connection(s) on the test server");
    }

    /**
     * @Then close :arg1
     */
    public function close($arg1)
    {
        $this->getConnection($arg1)->close();
    }

    abstract protected function retryStrategy($n);
}
