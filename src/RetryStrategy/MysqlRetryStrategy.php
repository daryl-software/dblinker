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
        if ($exception === null) {
            return false;
        }
        $restartOnMaster = true;
        switch ($exception->getErrorCode()) {
            case 1152: // ER_ABORTING_CONNECTION
            case 1205: // ER_LOCK_WAIT_TIMEOUT
            case 1213: // ER_LOCK_DEADLOCK
                // $wrappedConnection->close();
                sleep(1); // wait and retry
                break;
            case 1044: // ER_DBACCESS_DENIED_ERROR
            case 1045: // ER_ACCESS_DENIED_ERROR
                $restartOnMaster = false; // retry on another server
                break;
            case 2006: // CR_SERVER_GONE_ERROR
                // force reconnection
                $wrappedConnection->close();
                break;
            default:
                return false;
        }
        $masterSlave = false;
        if ($wrappedConnection instanceof MasterSlavesConnection) {
            if ($wrappedConnection->isConnectedToMaster()) {
                if (!$restartOnMaster) {
                    return false;
                }
            } else {
                $wrappedConnection->detachSlave($wrappedConnection->getCurrentConnection());
            }
        } else if (!$restartOnMaster) {
            return false;
        }

        return $this->retryLimit-- > 0;
    }

    public function retryLimit()
    {
        return $this->retryLimit > 0 ? (int)$this->retryLimit : 0;
    }
}
