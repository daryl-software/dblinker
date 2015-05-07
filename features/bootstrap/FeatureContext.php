<?php

use Behat\Behat\Context\Context;
use Behat\Behat\Context\SnippetAcceptingContext;
use Behat\Gherkin\Node\PyStringNode;
use Behat\Gherkin\Node\TableNode;
use Behat\Behat\Tester\Exception\PendingException;
use Doctrine\DBAL\DriverManager;

/**
 * Defines application features from the specific context.
 */
class FeatureContext implements Context, SnippetAcceptingContext
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

        $slaveCount = (int)$slaveCount;
        $slaves = [];
        while($slaveCount--) {
            $master['weight'] = 1;
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
     * @Given requests are forced on master for :connectionName
     * @When I force requests on master for :arg1
     */
    public function requestsAreForcedOnMasterFor($connectionName)
    {
        $this->getWrappedConnection($connectionName)->connectToMaster();
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
    public function shouldHaveRetryQuery($connectionName, $n)
    {
        $retryLimit = $this->getWrappedConnection($connectionName)->retryStrategy()->retryLimit();
        assert($retryLimit === (int)$n, "Retry limit is $retryLimit, $n expected.");
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
     * @Given :permission is denied on :conn
     */
    public function isDenied($permission, $conn)
    {

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
        assert($this->connections[$connectionName]['last-result'] !== null);
    }

    /**
     * @Then the last error code should be :errorCode on :connectionName
     */
    public function theLastErrorCodeShouldBeOn($expectedErrorCode, $connectionName)
    {
        $exception = null;
        $errorCodeAssertFailureMessage = "No error found, error code $expectedErrorCode expected";
        $wrappedConnection = $this->getConnection($connectionName)->getWrappedConnection();
        if ($wrappedConnection instanceof Ez\DbLinker\Driver\Connection\RetryConnection) {
            $exception = $wrappedConnection->retryStrategy()->lastError();
        }
        if ($exception === null && $this->connections[$connectionName]['last-error'] !== null) {
            $previousException = $this->connections[$connectionName]['last-error']->getPrevious();
        }
        $errorCode = null;
        if ($exception !== null) {
            $errorCode = $exception->getErrorCode();
            $errorCodeAssertFailureMessage = "Error code is $errorCode, error code $expectedErrorCode expected";
        }
        assert($errorCode === (int)$expectedErrorCode, $errorCodeAssertFailureMessage);
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

    private function masterParams()
    {
        return [
            'driver'        => 'mysqli',
            'host'          => getenv('DBLINKER_DB_1_PORT_3306_TCP_ADDR'),
            'user'          => getenv('DBLINKER_DB_1_ENV_MYSQL_USER'),
            'password'      => getenv('DBLINKER_DB_1_ENV_MYSQL_PASSWORD'),
            'dbname'        => getenv('DBLINKER_DB_1_ENV_MYSQL_DATABASE'),
            'driver'        => 'pdo_mysql',
            'driverOptions' => [
                // todo move to retry driver
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            ],
        ];
    }
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
