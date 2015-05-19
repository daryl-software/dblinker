<?php

namespace Ez\DbLinker\RetryStrategy;

use Ez\DbLinker\RetryStrategy;
use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\DBALException;
use Ez\DbLinker\Driver\Connection\RetryConnection;
use Ez\DbLinker\Driver\Connection\MasterSlavesConnection;
use Doctrine\DBAL\Exception\DriverException;

class MysqlRetryStrategy implements RetryStrategy
{
    private $retryLimit;

    public function __construct($retryLimit = INF)
    {
        $this->retryLimit = $retryLimit;
    }

    public function shouldRetry(
        DBALException $exception,
        RetryConnection $connection,
        Connection $wrappedConnection,
        $method,
        Array $arguments
    ) {
        if ($connection->transactionLevel() > 0) {
            return false;
        }
        while ($exception !== null && !($exception instanceof DriverException)) {
            $exception = $exception->getPrevious();
        }
        if ($exception === null) {
            return false;
        }
        $changeSlave = false;
        $restartOnMaster = true;
        $reconnect = false;
        switch ($exception->getErrorCode()) {
            case 1152: // ER_ABORTING_CONNECTION
            case 1205: // ER_LOCK_WAIT_TIMEOUT
            case 1213: // ER_LOCK_DEADLOCK
                sleep(1); // wait and retry
                break;
            case 1044: // ER_DBACCESS_DENIED_ERROR
            case 1045: // ER_ACCESS_DENIED_ERROR
                 // retry on another server
                $restartOnMaster = false;
                $reconnect = true;
                $changeSlave = true;
                break;
            case 2006: // CR_SERVER_GONE_ERROR
                // force reconnection
                $reconnect = true;
                break;
            default:
                return false;
        }
        $connection = $wrappedConnection->getWrappedConnection();
        if ($connection instanceof MasterSlavesConnection && !$connection->isConnectedToMaster()) {
            if ($changeSlave) {
                $connection->changeSlave();
            } else if ($reconnect) {
                $wrappedConnection->close();
            }
        } else if (!$restartOnMaster) {
            return false;
        } else if ($reconnect) {
            $wrappedConnection->close();
        }
        return $this->retryLimit-- > 0;
    }

    public function retryLimit()
    {
        return $this->retryLimit > 0 ? (int)$this->retryLimit : 0;
    }
}
