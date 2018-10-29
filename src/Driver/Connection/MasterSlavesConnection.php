<?php

namespace Ez\DbLinker\Driver\Connection;

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Driver\Connection;

class MasterSlavesConnection implements Connection
{
    /**
     * configuration
     *
     * @var array
     */
    private $master;

    /**
     * configuration
     *
     * @var array
     */
    private $slaves;

    /**
     * @var \Ez\DbLinker\Cache
     */
    private $cache;

    /**
     * @var bool
     */
    private $forceMaster = false;

    /**
     * @var int
     */
    private $maxSlaveDelay = 30;

    /**
     * @var int
     */
    private $slaveStatusCacheTtl = 10;

    /**
     * Actual connections established to master & slaves
     *
     * @var array
     */
    private $connections = ['master' => null, 'slaves' => []];

    /**
     * @var Connection
     */
    private $lastConnection;

    public function __construct(array $master, array $slaves, $cache = null)
    {
        $this->master = $master;
        $this->slaves = $this->checkSlaves($slaves);
        $this->cache = $cache;
    }

    public function disableCache() {
        $this->cache->disableCache();
    }

    private function checkSlaves(array $slaves)
    {
        // keep slave with weight > 0
        return array_filter(array_map(function ($slave) {
            if ((int) $slave['weight'] > 0) {
                return $slave;
            }
            return null;
        }, $slaves));
    }

    private function masterConnection(bool $forced = null): Connection
    {
        if ($forced !== null) {
            $this->forceMaster = $forced;
        }
        if (!$this->connections['master']) {
            $this->connections['master'] = DriverManager::getConnection($this->master);
        }
        $this->lastConnection = $this->connections['master'];
        return $this->connections['master'];
    }

    private function slaveConnection(int $index = null): Connection
    {
        if (empty($this->slaves)) {
            return $this->masterConnection();
        }

        $this->forceMaster = false;
        $cnx = null;

        if ($index !== null && isset($this->connections['slaves'][$index])) {
            // we want a specific slave and we have it loaded
            $cnx = $this->connections['slaves'][$index];
        } else if ($index == null && !empty($this->connections['slaves'])) {
            // we dont want a particular slave, but we have one loaded
            $cnx = current($this->connections['slaves']);
        } else {
            // we dont have any slave loaded
            $slaveIndex = $index ?? $this->randomSlave();
            if (!isset($this->connections['slaves'][$slaveIndex])) {
                $cnx = $this->connections['slaves'][$slaveIndex] = DriverManager::getConnection($this->slaves[$slaveIndex]);
            } else {
                $cnx = $this->connections['slaves'][$slaveIndex];
            }
        }

        $this->lastConnection = $cnx;
        return $cnx;
    }

    public function getLastConnection(): ?Connection
    {
        return $this->lastConnection;
    }

    /**
     * Get random slave and return configuration array
     *
     * @return int
     */
    private function randomSlave(): int
    {
        $weights = [];
        foreach ($this->slaves as $slaveIndex => $slave) {
            $weights = array_merge($weights, array_fill(0, $slave['weight'], $slaveIndex));
        }
        return $weights[array_rand($weights)];
    }

    public function disableCurrentSlave()
    {
//        if ($this->currentSlave !== null) {
//            array_splice($this->slaves, $this->currentSlave, 1);
//            $this->currentSlave = null;
//        }
//        $this->currentConnectionParams = null;
//        $this->wrappedConnection = null;
    }

    public function slaves()
    {
        return $this->slaves;
    }

    /**
     * Prepares a statement for execution and returns a Statement object.
     *
     * @param string $prepareString
     *
     * @return \Doctrine\DBAL\Driver\Statement
     */
    public function prepare($prepareString)
    {
        $cnx = null;
        if ($this->forceMaster
            || preg_match('/\b(DELETE|UPDATE|INSERT)\b/i', $prepareString)) {
            $cnx = $this->masterConnection();
        } else {
            $cnx = $this->slaveConnection();
        }

        return $cnx->prepare($prepareString);
    }

    public function forceMaster(bool $force)
    {
        $this->forceMaster = $force;
    }

    /**
     * Executes an SQL statement, returning a result set as a Statement object.
     *
     * @return \Doctrine\DBAL\Driver\Statement
     */
    public function query()
    {
        $cnx = $this->forceMaster ? $this->masterConnection() : $this->slaveConnection();
        return call_user_func_array([$cnx, __FUNCTION__], func_get_args());
    }

    /**
     * Quotes a string for use in a query.
     *
     * @param string  $input
     * @param integer $type
     *
     * @return string
     */
    public function quote($input, $type = \PDO::PARAM_STR)
    {
        return $this->slaveConnection()->quote($input, $type);
    }

    /**
     * Executes an SQL statement and return the number of affected rows.
     *
     * @param string $statement
     *
     * @return integer
     */
    public function exec($statement)
    {
        return $this->masterConnection()->exec($statement);
    }

    /**
     * Returns the ID of the last inserted row or sequence value.
     *
     * @param string|null $name
     *
     * @return string
     */
    public function lastInsertId($name = null)
    {
        return $this->masterConnection()->lastInsertId($name);
    }

    /**
     * Initiates a transaction.
     *
     * @return boolean TRUE on success or FALSE on failure.
     */
    public function beginTransaction()
    {
        return $this->masterConnection(true)->beginTransaction();
    }

    /**
     * Commits a transaction.
     *
     * @return boolean TRUE on success or FALSE on failure.
     */
    public function commit()
    {
        return $this->masterConnection(false)->commit();
    }

    /**
     * Rolls back the current transaction, as initiated by beginTransaction().
     *
     * @return boolean TRUE on success or FALSE on failure.
     */
    public function rollBack()
    {
        return $this->masterConnection(false)->rollBack();
    }

    /**
     * Returns the error code associated with the last operation on the database handle.
     *
     * @return string|null The error code, or null if no operation has been run on the database handle.
     */
    public function errorCode()
    {
        return $this->lastConnection->errorCode();
    }

    /**
     * Returns extended error information associated with the last operation on the database handle.
     *
     * @return array
     */
    public function errorInfo()
    {
        return $this->lastConnection->errorInfo();
    }

    public function close()
    {

    }

    private function hasCache() {
        return $this->cache !== null;
    }

    private function getCacheKey() {
        return 'MasterSlavesConnection_' .strtr(serialize($this->currentConnectionParams), '{}()/@:', '______|');
    }

    public function setSlaveStatus(bool $running, ?int $delay) {
        if ($this->hasCache()) {
            $this->cache->setCacheItem($this->getCacheKey(), ['running' => $running, 'delay' => $delay], $this->slaveStatusCacheTtl);
        }
        return ['running' => $running, 'delay' => $delay];
    }

    private function getSlaveStatus() {
        if (stripos($this->wrappedDriver->getName(), 'pgsql') !== false) {
            try {
                $sss = $this->wrappedConnection()->query('SELECT now() - pg_last_xact_replay_timestamp() AS replication_lag')->fetch();
                return $this->setSlaveStatus(true, $sss['replication_lag']);
            } catch (\Exception $e) {
                return $this->setSlaveStatus(false, null);
            }
        } else {
            try {
                $sss = $this->wrappedConnection()->query('SHOW SLAVE STATUS')->fetch();
                if ($sss['Slave_IO_Running'] === 'No' || $sss['Slave_SQL_Running'] === 'No') {
                    // slave is STOPPED
                    return $this->setSlaveStatus(false, null);
                } else {
                    return $this->setSlaveStatus(true, $sss['Seconds_Behind_Master']);
                }
            } catch (\Exception $e) {
                if (stripos($e->getMessage(), 'Access denied') !== false) {
                    return $this->setSlaveStatus(true, 0);
                }
                return $this->setSlaveStatus(false, null);
            }
        }
    }

    public function isSlaveOk($maxdelay = null) {
        if ($maxdelay === null) {
            $maxdelay = $this->maxSlaveDelay;
        }
        if ($this->hasCache()) {
            $status = $this->cache->getCacheItem($this->getCacheKey());
            if ($status === null) {
                $status = $this->getSlaveStatus();
            }
        } else {
            $status = $this->getSlaveStatus();
        }
        if (!$status['running'] || $status['delay'] >= $maxdelay) {
            $this->disableCurrentSlave();
            return false;
        }
        return true;
    }

    public function __destruct()
    {
        // called on connection closes
    }
}
