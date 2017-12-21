<?php

namespace Ez\DbLinker\Driver\Connection;

use Exception;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Driver\PDOConnection;

class MasterSlavesConnection implements Connection, ConnectionWrapper
{
    use ConnectionWrapperTrait;

    private $master;
    private $slaves;
    private $currentConnectionParams;
    private $currentSlave;
    private $cache;
    private $forceMaster;
    private $maxSlaveDelay = 30;
    private $slaveStatusCacheTtl = 10;

    public function __construct(array $master, array $slaves, $cache = null)
    {
        $this->master = $master;
        $this->checkSlaves($slaves);
        $this->slaves = $slaves;
        $this->cache = $cache;
        $this->forceMaster = false;
    }

    public function disableCache() {
        return $this->cache->disableCache();
    }

    private function checkSlaves(array $slaves)
    {
        foreach ($slaves as $slave) {
            if ((int)$slave['weight'] < 0) {
                throw new Exception('Slave weight must be >= 0');
            }
        }
    }

    public function connectToMaster($forced = null)
    {
        if ($forced !== null) {
            $this->forceMaster = $forced;
        }
        if ($this->currentConnectionParams === $this->master) {
            return;
        }
        $this->currentConnectionParams = $this->master;
        $this->currentSlave = null;
        $this->wrappedConnection = null;
    }

    public function connectToSlave()
    {
        $this->forceMaster = false;
        if ($this->currentConnectionParams !== null && $this->currentConnectionParams !== $this->master) {
            return;
        }
        $this->currentConnectionParams = null;
        $this->currentSlave = null;
        $this->wrappedConnection = null;
        $this->wrap();
        while (!$this->isSlaveOk() && $this->currentSlave !== null) {
            $this->wrap();
        }
    }

    public function isConnectedToMaster()
    {
        return $this->currentSlave === null && $this->currentConnectionParams !== null;
    }

    /**
     * @inherit
     */
    public function getCurrentConnection()
    {
        return $this->wrappedConnection();
    }

    protected function wrap()
    {
        if ($this->wrappedConnection !== null) {
            return $this->wrappedConnection;
        }
        if ($this->currentConnectionParams === null) {
            $this->currentSlave = $this->chooseASlave();
            $this->currentConnectionParams = $this->currentSlave !== null ? $this->slaves[$this->currentSlave] : $this->master;
        }
        $connection = DriverManager::getConnection($this->currentConnectionParams);
        $this->wrappedConnection = $connection->getWrappedConnection();
        $this->wrappedDriver = $connection->getDriver();
    }

    private function chooseASlave()
    {
        $totalSlavesWeight = $this->totalSlavesWeight();
        if ($totalSlavesWeight < 1) {
            return null;
        }
        $weightTarget = mt_rand(1, $totalSlavesWeight);
        foreach ($this->slaves as $n => $slave) {
            $weightTarget -= $slave['weight'];
            if ($weightTarget <= 0) {
                return $n;
            }
        }
    }

    private function totalSlavesWeight()
    {
        $weight = 0;
        foreach ($this->slaves as $slave) {
            $weight += $slave['weight'];
        }
        return $weight;
    }

    public function disableCurrentSlave()
    {
        if ($this->currentSlave !== null) {
            array_splice($this->slaves, $this->currentSlave, 1);
            $this->currentSlave = null;
        }
        $this->currentConnectionParams = null;
        $this->wrappedConnection = null;
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
        $this->connectToMaster(true);
        return $this->wrappedConnection()->prepare($prepareString);
    }

    /**
     * Executes an SQL statement, returning a result set as a Statement object.
     *
     * @return \Doctrine\DBAL\Driver\Statement
     */
    public function query()
    {
        if ($this->forceMaster !== true) {
            $this->connectToSlave();
        }
        return call_user_func_array([$this->wrappedConnection(), __FUNCTION__], func_get_args());
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
        return $this->wrappedConnection()->quote($input, $type);
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
        $this->connectToMaster();
        return $this->wrappedConnection()->exec($statement);
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
        $this->forceMaster = true;
        return $this->wrappedConnection()->lastInsertId($name);
    }

    /**
     * Initiates a transaction.
     *
     * @return boolean TRUE on success or FALSE on failure.
     */
    public function beginTransaction()
    {
        $this->connectToMaster(true);
        return $this->wrappedConnection()->beginTransaction();
    }

    /**
     * Commits a transaction.
     *
     * @return boolean TRUE on success or FALSE on failure.
     */
    public function commit()
    {
        $this->connectToMaster(false);
        return $this->wrappedConnection()->commit();
    }

    /**
     * Rolls back the current transaction, as initiated by beginTransaction().
     *
     * @return boolean TRUE on success or FALSE on failure.
     */
    public function rollBack()
    {
        $this->connectToMaster(false);
        return $this->wrappedConnection()->rollBack();
    }

    /**
     * Returns the error code associated with the last operation on the database handle.
     *
     * @return string|null The error code, or null if no operation has been run on the database handle.
     */
    public function errorCode()
    {
        return $this->wrappedConnection()->errorCode();
    }

    /**
     * Returns extended error information associated with the last operation on the database handle.
     *
     * @return array
     */
    public function errorInfo()
    {
        return $this->wrappedConnection()->errorInfo();
    }

    public function close()
    {
        if (!$this->wrappedConnection() instanceof PDOConnection) {
            return $this->wrappedConnection()->getWrappedResourceHandle()->close();
        }
    }

    private function hasCache() {
        return $this->cache !== null;
    }

    private function getCacheKey() {
        return "MasterSlavesConnection_".strtr(serialize($this->currentConnectionParams), '{}()/@:', '______|');
    }

    public function setSlaveStatus(bool $running, ?int $delay) {
        if ($this->hasCache()) {
            $this->cache->setCacheItem($this->getCacheKey(), ["running" => $running, "delay" => $delay], $this->slaveStatusCacheTtl);
        }
        return ['running' => $running, 'delay' => $delay];
    }

    private function getSlaveStatus() {
        try {
            $sss = $this->wrappedConnection()->query("SHOW SLAVE STATUS")->fetch();
            if ($sss['Slave_IO_Running'] === 'No' || $sss['Slave_SQL_Running'] === 'No') {
                // slave is STOPPED
                return $this->setSlaveStatus(false, INF);
            } else {
                return $this->setSlaveStatus(true, $sss['Seconds_Behind_Master']);
            }
        } catch (\Exception $e) {
            return $this->setSlaveStatus(true, 0);
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
            $this->wrap();
            return false;
        }
        return true;
    }
}
