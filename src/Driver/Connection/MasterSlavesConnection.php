<?php

namespace Ez\DbLinker\Driver\Connection;

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Driver\Connection;
use Ez\DbLinker\ExtendedServer;
use Ez\DbLinker\Master;
use Ez\DbLinker\Slave;

class MasterSlavesConnection implements Connection
{
    /**
     * configuration
     *
     * @var Master
     */
    private $master;

    /**
     * configuration
     *
     * @var Slave[]
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
     * @var ExtendedServer
     */
    private $lastConnection;

    /**
     * @var int
     */
    private $lastSlaveIndex;

    public function __construct(array $master, array $slaves, $cache = null)
    {
        $this->master = new Master($master);
        $this->slaves = $this->filteredSlaves($slaves);
        $this->cache = $cache;
    }

    public function disableCache() {
        $this->cache->disableCache();
    }

    private function filteredSlaves(array $slaves)
    {
        // keep slave with weight > 0
        return array_filter(array_map(function ($slave) {
            if ((int) $slave['weight'] > 0) {
                return new Slave($slave);
            }
            return null;
        }, $slaves));
    }

    private function masterConnection(bool $forced = null): Connection
    {
        if ($forced !== null) {
            $this->forceMaster = $forced;
        }
        $this->lastConnection = $this->master;
        return $this->master->connection();
    }

    private function slaveConnection(): Connection
    {
        if (empty($this->slaves)) {
            return $this->masterConnection();
        }

        $this->forceMaster = false;
        $x = null;

        // if we have a connected slave
        foreach ($this->slaves as $slave) {
            if ($slave->isConnected()) {
                $x = $slave;
            }
        }
        if (!$x) {
            $x = Slave::random($this->slaves);
        }

        $this->lastConnection = $x;
        return $x->connection();
    }

    public function getLastConnection(): ?ExtendedServer
    {
        return $this->lastConnection;
    }

    public function disableCurrentSlave(bool $save = false): void
    {
        if ($this->lastConnection instanceof Slave) {
            $this->lastConnection->disable();
        }
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

    public function forceMaster(bool $force): void
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

    private function getCacheKey(array $params) {
        return 'MasterSlavesConnection_' .strtr(serialize($params), '{}()/@:', '______|');
    }

    public function setSlaveStatus(int $slaveIndex, bool $running, ?int $delay) {
        if ($this->hasCache()) {
            $this->cache->setCacheItem($this->getCacheKey($this->slaves[$slaveIndex]), ['running' => $running, 'delay' => $delay], $this->slaveStatusCacheTtl);
        }
        return ['running' => $running, 'delay' => $delay];
    }

    private function getSlaveStatus(int $slaveIndex) {
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

    public function isSlaveOk(int $slaveIndex, bool $cachedValue, int $maxdelay = null) {
        $maxdelay = $maxdelay ?? $this->maxSlaveDelay;
        if ($cachedValue) {
            assert($this->hasCache());
            $status = $this->cache->getCacheItem($this->getCacheKey($this->slaves[$slaveIndex]));
        }
        if ($this->hasCache()) {
            if ($status === null) {
                $status = $this->getSlaveStatus($slaveIndex);
            }
        } else {
            $status = $this->getSlaveStatus($slaveIndex);
        }
        if (!$status['running'] || $status['delay'] >= $maxdelay) {
            $this->disableCurrentSlave();
            return false;
        }
        return true;
    }

    public function lastConnectionType(): ?string {
        return $this->lastConnectionType;
    }

    public function __destruct()
    {
        // called on connection closes
    }
}
