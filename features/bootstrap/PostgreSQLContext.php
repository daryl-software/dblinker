<?php

use Behat\Behat\Tester\Exception\PendingException;
use Ez\DbLinker\Driver\Connection\RetryConnection;

trait PostgreSQLContext
{
    private function masterParams($username = null, $password = '') {
        $params = [
            'host'          => getenv('DBLINKER_POSTGRESQL_MASTER_1_PORT_5432_TCP_ADDR'),
            'user'          => getenv('DBLINKER_POSTGRESQL_MASTER_1_ENV_POSTGRES_USER'),
            'password'      => getenv('DBLINKER_POSTGRESQL_MASTER_1_ENV_POSTGRES_PASSWORD'),
            'dbname'        => $this->defaultDatabaseName(),
        ];
        if ($username !== null && $username !== 'root') {
            if ($username === 'root') {
                $password = getenv('DBLINKER_POSTGRESQL_MASTER_1_ENV_POSTGRES_ROOT_PASSWORD');
                $username = getenv('DBLINKER_POSTGRESQL_MASTER_1_ENV_POSTGRES_ROOT_USER');
            }
            $params['user'] = $username;
            $params['password'] = $password;
        }
        return $this->params($params);
    }

    private function slaveParams(int $number, string $username = null, string $password = '') {
        $params = [
            'host'          => getenv('DBLINKER_POSTGRESQL_SLAVE_'.$number.'_1_PORT_5432_TCP_ADDR'),
            'user'          => getenv('DBLINKER_POSTGRESQL_SLAVE_'.$number.'_1_ENV_POSTGRES_USER'),
            'password'      => getenv('DBLINKER_POSTGRESQL_SLAVE_'.$number.'_1_ENV_POSTGRES_PASSWORD'),
            'dbname'        => $this->defaultDatabaseName(),
        ];
        if ($username !== null && $username !== 'root') {
            if ($username === 'root') {
                $password = getenv('DBLINKER_POSTGRESQL_SLAVE_'.$number.'_1_ENV_POSTGRES_ROOT_PASSWORD');
                $username = getenv('DBLINKER_POSTGRESQL_SLAVE_'.$number.'_1_ENV_POSTGRES_ROOT_USER');
            }
            $params['user'] = $username;
            $params['password'] = $password;
        }
        return $this->params($params);
    }

    private function defaultDatabaseName()
    {
        return getenv('DBLINKER_POSTGRESQL_MASTER_1_ENV_POSTGRES_DB');
    }

    private function activeConnectionsCount()
    {
        return 0;
    }

    /**
     * @Given the server accept :n more connection
     * @Given the server accept :n more connections
     */
    public function theServerAcceptMoreConnections($n)
    {
        throw new PendingException;
    }

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

    private function retryStrategy($n)
    {
        return new PostgreSQLRetryStrategy($n);
    }


    private function errorCode(Exception $exception)
    {
        if(preg_match("/SQLSTATE\[(?<errorCode>[A-Z0-9]*)\]/", $exception->getMessage(), $matches)) {
            return $matches["errorCode"];
        }
    }

    private function retryDriverClass()
    {
        return "Ez\DbLinker\Driver\PostgresqlRetryDriver";
    }

    private function masterSlaveDriverClass()
    {
        return "Ez\DbLinker\Driver\PostgresqlMasterSlavesDriver";
    }

    private function prepareSql($sql)
    {
        if ($sql === "SET @var = 1") {
            return "SELECT 1";
        }
        return $sql;
    }

    private function errorToCode($error)
    {
        if ($error === null) {
            return;
        }
        $errors = [
            "BAD_DB" => "08006",
            "ACCESS_DENIED" => "08006",
            "DBACCESS_DENIED" => "08006",
            "CON_COUNT" => "53300",
            "NO_SUCH_TABLE" => "42P01",
        ];
        if (array_key_exists($error, $errors)) {
            return $errors[$error];
        }
        return "UNKNOWN_ERROR: $error";
    }
}

class PostgreSQLRetryStrategy extends Ez\DbLinker\RetryStrategy\PostgreSQLRetryStrategy
{
    private $lastError = null;
    private $handlers = [];

    public function shouldRetry(
        Exception $exception,
        RetryConnection $connection
    ) {
        $this->lastError = $exception;
        return array_reduce($this->handlers, function($retry, Closure $handler) use ($exception, $connection) {
            return $retry || $handler($exception, $connection);
        }, false) || parent::shouldRetry($exception, $connection);
    }

    public function lastError()
    {
        return $this->lastError;
    }

    public function addHandler(Closure $handler)
    {
        $this->handlers[] = $handler;
    }
}
