<?php

use Doctrine\DBAL\DriverManager;
use Ez\DbLinker\Driver\Connection\RetryConnection;

trait FeatureContext
{
    /**
     * @var array
     */
    private $connections = [];

    /**
     * @var \Doctrine\DBAL\Statement
     */
    private $statement;

    /**
     * @BeforeScenario
     */
    abstract public function clearConnections();

    /**
     * @BeforeScenario
     */
    public function assertNoActiveConnection()
    {
        $n = $this->activeConnectionsCount();
        assert($n === 0, "There is $n active connection(s) on the test server");
    }

    /**
     * @return \Doctrine\DBAL\Connection
     * @throws \Doctrine\DBAL\DBALException
     */
    private function rootConnection(): \Doctrine\DBAL\Connection
    {
        $params = $this->masterParams('root');
        return DriverManager::getConnection($params);
    }

    abstract protected function activeConnectionsCount();

    /**
     * @AfterScenario
     */
    public function closeConnections()
    {
        $this->connections = [];
        gc_collect_cycles();
    }

    /**
     * @Given the server accept :n more connection
     * @Given the server accept :n more connections
     */
    abstract public function theServerAcceptMoreConnections(int $n);

    /**
     * @Given a master-slaves connection :connectionName with :slaveCount slaves
     */
    public function aMasterSlavesConnectionWithSlaves(string $connectionName, int $slaveCount)
    {
        $slaves = [];
        for ($i = 1; $i <= $slaveCount; $i++) {
            $slaves[] = array_merge($this->slaveParams($i), ['weight' => rand(1, 20)]);
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
            $slaves[] = array_merge($this->slaveParams($i, $username), ['weight' => rand(1, 20)]);
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
    public function requestsAreForcedOnMasterFor($connectionName): void
    {
        /** @var RetryConnection $connection */
        $connection = $this->wrpdcnx($connectionName);
        /** @var \Ez\DbLinker\Driver\Connection\MasterSlavesConnection $xnz */
        $xnz = $connection->wrappedConnection();
        $xnz->forceMaster(true);
    }

    /**
     * @When I force requests on slave for :connectionName
     */
    public function iForceRequestsOnSlaveFor($connectionName): void
    {
        $this->wrpdcnx($connectionName)->forceMaster(false);
    }

    /**
     * @When I query :sql with param :param on :connectionName
     */
    public function iQueryWithParamOn(string $sql, string $param, string $connectionName): void
    {
        $this->connections[$connectionName]['last-result'] = null;
        $this->connections[$connectionName]['last-error']  = null;
        try {
            $this->connections[$connectionName]['last-result'] = $this->getConnection($connectionName)->executeQuery($sql, [$param]);
        } catch (\Exception $e) {
            $this->connections[$connectionName]['last-error'] = $e;
        }
    }

    /**
     * @When I query :sql on :connectionName
     */
    public function iQueryOn(string $connectionName, string $sql)
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
    public function iCreateADeadlockOn($connectionName, $connectionNameFork): void
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
     * @When I exec update :sql on :connectionName
     */
    public function iExecUpdateOn($connectionName, $sql)
    {
        $sql = $this->prepareSql($sql);
        $this->connections[$connectionName]['last-result'] = null;
        $this->connections[$connectionName]['last-error']  = null;
        try {
            $this->connections[$connectionName]['last-result'] = $this->getConnection($connectionName)->executeUpdate($sql);
        } catch (\Exception $e) {
            $this->connections[$connectionName]['last-error'] = $e;
        }
    }

    /**
     * @Then :connectionName is on slave
     */
    public function isOnSlave($connectionName)
    {
        assert($this->wrpdcnx($connectionName)->getLastConnection() instanceof \Ez\DbLinker\Slave);
    }

    /**
     * @Then :connectionName is on master
     */
    public function isOnMaster($connectionName)
    {
        assert($this->wrpdcnx($connectionName)->getLastConnection() instanceof \Ez\DbLinker\Master);
    }

    /**
     * @When I start a transaction on :connectionName
     * @Given a transaction is started on :connectionName
     */
    public function aTransactionIsStartedOn($connectionName): void
    {
        $this->getConnection($connectionName)->beginTransaction();
    }

    /**
     * @Given a retry connection :connectionName limited to :n retry
     * @Given a retry connection :connectionName limited to :n retry with username :username
     */
    public function aRetryConnectionLimitedToRetryWithusername(string $connectionName, int $n, string $username = null)
    {
        $this->connections[$connectionName] = [
            'params' => [
                'driverClass' => $this->retryDriverClass(),
                'connectionParams' => $this->masterParams($username),
                'retryStrategy' => $this->retryStrategy($n),
            ],
            'instance' => null,
            'last-result' => null,
            'last-error' => null,
        ];
    }

    /**
     * @Given a retry master-slaves connection :connectionName with :slaveCount slaves limited to :n retry with db :db
     * @Given a retry master-slaves connection :connectionName with :slaveCount slaves limited to :n retry with db :db and username :username
     */
    public function aRetryMasterSlavesConnectionWithSlavesLimitedToRetryWithDbAndUsername(string $connectionName, int $slaveCount, int $n, string $db, string $username = null)
    {
        $master = $this->masterParams($username);
        $master['dbname'] = $db;

        $slaves = [];
        for ($i = 1; $i <= $slaveCount; $i++) {
            $slaves[] = array_merge($this->slaveParams($i, $username), ['dbname' => $db, 'weight' => rand(1, 20)]);
        }

        $this->connections[$connectionName] = [
            'params' => [
                'driverClass' => $this->retryDriverClass(),
                'connectionParams' => [
                    'master' => $master,
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
    public function databaseHasGoneAwayOn($connectionName): void
    {
        $this->getConnection($connectionName)->exec('SET SESSION WAIT_TIMEOUT=1');
        usleep(1100000);
    }

    /**
     * @Then :connectionName retry limit should be :n
     */
    public function retryLimitShouldBe($connectionName, $n): void
    {
        $retryLimit = $this->connections[$connectionName]['params']['retryStrategy']->retryLimit();
        assert($retryLimit === (int) $n, "Retry limit is $retryLimit, $n expected.");
    }

    /**
     * @Given there is a row Lock on table :tableName on :connectionName
     */
    public function thereIsARowLockOn($tableName, $connectionName): void
    {
        $connection = $this->getConnection($connectionName);
        $connection->exec("INSERT INTO $tableName (id, n) VALUES (1, 1)");
        $connection->beginTransaction();
        $connection->exec("UPDATE $tableName SET n = 2");
    }

    /**
     * @When I commit the transaction on :connectionName
     */
    public function iCommitTheTransactionOn($connectionName): void
    {
        $this->getConnection($connectionName)->commit();
    }

    /**
     * @Then the last query succeeded on :connectionName
     */
    public function theLastQuerySucceededOn($connectionName): void
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
    public function theLastQueryFailedOn($connectionName): void
    {
        assert($this->connections[$connectionName]['last-error'] !== null);
    }

    /**
     * @Then :connectionName should have :n slave
     * @Then :connectionName should have :n slaves
     */
    public function shouldHaveSlave($connectionName, $n): void
    {
        $connection = $this->wrpdcnx($connectionName);
        if ($connection instanceof \Ez\DbLinker\Driver\Connection\RetryConnection) {
            $connection = $connection->wrappedConnection();
        }
        $slaveCount = count(array_filter($connection->slaves(), function (\Ez\DbLinker\Slave $slave) {
            return $slave->getWeight() > 0;
        }));
        assert($slaveCount === (int)$n, "Slaves count is $slaveCount, $n expected.");
    }

    /**
     * @Given a connection :connectionName
     * @Given a connection :connectionName with username :username
     */
    public function aConnectionWithusername($connectionName, $username = null): void
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
    public function iPrepareAStatementOn(string $query, string $connectionName): void
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
            var_dump($this->lastStatementResult);
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
     * @Then the last error should be :errorNameExpected on :connectionName
     */
    public function theLastErrorShouldBeOn(string $errorNameExpected, $connectionName)
    {
        $error = $this->connections[$connectionName]['last-error'] ?: $this->connections[$connectionName]['params']['retryStrategy']->lastError();

        $errorCode = $this->errorCode($error);

        $errorCodeAssertFailureMessage = "No error found, error $errorNameExpected expected";
        $expectedErrorCode = $this->errorToCode($errorNameExpected);
        if ($errorCode !== null) {
            $errorCodeAssertFailureMessage = "Error code is $errorCode, error code $expectedErrorCode expected";
        }
        assert((string) $errorCode === (string) $expectedErrorCode, $errorCodeAssertFailureMessage);
    }

    abstract protected function errorToCode($errorName);
    abstract protected function errorCode(Exception $exception);

    /**
     * @param $connectionName
     * @return \Ez\DbLinker\Driver\Connection\MasterSlavesConnection
     * @throws \Doctrine\DBAL\DBALException
     */
    private function wrpdcnx($connectionName)
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
        // drop table first ??
        $retryStrategy = $this->connections[$connectionName]['params']['retryStrategy'];
        $this->wrpdcnx($connectionName)->exec('DROP TABLE IF EXISTS ' . $tableName);
        $retryStrategy->addHandler(function (Exception $exception,RetryConnection $connection) use ($tableName) {
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
        $connection = $this->wrpdcnx($connectionName);
        $connection->disableCache();
    }

    /**
     * @Given slave replication is stopped on :connectionName
     */
    public function slaveReplicationIsStopped($connectionName)
    {
        $connection = $this->wrpdcnx($connectionName);
        if ($connection instanceof Ez\DbLinker\Driver\Connection\RetryConnection) {
            $connection = $connection->wrappedConnection();
        }
        /** @var \Ez\DbLinker\Driver\Connection\MasterSlavesConnection $connection */
        $connection->setSlaveStatus(0, false, 120);
        $connection->isSlaveOk(0);
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
