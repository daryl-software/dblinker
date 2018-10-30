<?php

use Doctrine\DBAL\DriverManager;

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Driver\DriverException as DDriverException;
use Doctrine\DBAL\Exception\DriverException as EDriverException;
use Ez\DbLinker\Driver\Connection\RetryConnection;

trait MySQLContext
{
    private function masterParams($username = null, $password = '') {
        $params = [
            'host'          => getenv('DBLINKER_MYSQL_MASTER_1_PORT_3306_TCP_ADDR'),
            'user'          => getenv('DBLINKER_MYSQL_MASTER_1_ENV_MYSQL_USER'),
            'password'      => getenv('DBLINKER_MYSQL_MASTER_1_ENV_MYSQL_PASSWORD'),
            'dbname'        => $this->defaultDatabaseName(),
        ];
        if ($username !== null) {
            $params['user'] = $username;
            if ($username === 'root') {
                $password = getenv('DBLINKER_MYSQL_MASTER_1_ENV_MYSQL_ROOT_PASSWORD');
            }
            $params['password'] = $password;
        }
        return $this->params($params);
    }

    private function slaveParams(int $number, $username = null, $password = '') {
        $params = [
            'host'          => getenv('DBLINKER_MYSQL_SLAVE_'.$number.'_1_PORT_3306_TCP_ADDR'),
            'user'          => getenv('DBLINKER_MYSQL_SLAVE_'.$number.'_1_ENV_MYSQL_USER'),
            'password'      => getenv('DBLINKER_MYSQL_SLAVE_'.$number.'_1_ENV_MYSQL_PASSWORD'),
            'dbname'        => $this->defaultDatabaseName(),
        ];
        if ($username !== null && $username !== 'root') {
            if ($username === 'root') {
                $password = getenv('DBLINKER_MYSQL_SLAVE_'.$number.'_1_ENV_MYSQL_ROOT_PASSWORD');
                $username = getenv('DBLINKER_MYSQL_SLAVE_'.$number.'_1_ENV_MYSQL_ROOT_USER');
            }
            $params['user'] = $username;
            $params['password'] = $password;
        }
        return $this->params($params);
    }

    private function defaultDatabaseName()
    {
        return getenv('DBLINKER_MYSQL_MASTER_1_ENV_MYSQL_DATABASE');
    }

    private function activeConnectionsCount()
    {
        $connection = $this->rootConnection();
        gc_collect_cycles();
        $n = (int)$connection->fetchAll("show status like 'Threads_connected'")[0]['Value'];
        $connection->close();
        $connection = null;
        gc_collect_cycles();
        return $n - 1;
    }

    /**
     * @Given the server accept :n more connection
     * @Given the server accept :n more connections
     */
    public function theServerAcceptMoreConnections($n)
    {
        $n += $this->activeConnectionsCount();
        $connection = $this->rootConnection();
        $connection->exec("SET GLOBAL MAX_CONNECTIONS = $n");
        $connection->close();
        $connection = null;
        gc_collect_cycles();
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
        $connection = $this->rootConnection();
        $connection->exec("SET GLOBAL MAX_CONNECTIONS = 50");
        $connection->close();
        $connection = null;
        gc_collect_cycles();
    }

    private function retryStrategy($n)
    {
        return new MysqlRetryStrategy($n);
    }

    private function errorCode(Exception $exception)
    {
        if ($exception instanceof DBALException) {
            $exception = $exception->getPrevious();
        }
        if ($exception instanceof DDriverException || $exception instanceof EDriverException) {
            return $exception->getErrorCode();
        }
    }

    private function retryDriverClass()
    {
        return "Ez\DbLinker\Driver\MysqlRetryDriver";
    }

    private function masterSlaveDriverClass()
    {
        return "Ez\DbLinker\Driver\MysqlMasterSlavesDriver";
    }

    private function prepareSql($sql)
    {
        return $sql;
    }

    private function errorToCode($error)
    {
        $errors = [
            "BAD_DB" => 1049,
            "ACCESS_DENIED" => 1045,
            "GONE_AWAY" => 2006,
            "LOCK_WAIT_TIMEOUT" => 1205,
            "DBACCESS_DENIED" => 1044,
            "DEADLOCK" => 1213,
            "CON_COUNT" => 1040,
            "NO_SUCH_TABLE" => 1146,
        ];
        if (array_key_exists($error, $errors)) {
            return $errors[$error];
        }
        return "UNKNOWN_ERROR: $error";
    }
}

class MysqlRetryStrategy extends Ez\DbLinker\RetryStrategy\MysqlRetryStrategy
{
    private $lastError = null;
    private $handlers = [];

    public function shouldRetry(Exception $exception, RetryConnection $connection) {
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
