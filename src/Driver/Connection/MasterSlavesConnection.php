<?php

namespace Ez\DbLinker\Driver\Connection;

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

    public function __construct(array $master, array $slaves, $cache = null)
    {
        $this->master = new Master($master);
        $this->slaves = $this->filteredSlaves($slaves);
        $this->cache = $cache;
    }

    public function disableCache() {
        $this->cache->disableCache();
    }

    /**
     * @param array $slaves
     * @return Slave[]
     */
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

    /**
     * @param bool|null $forced
     * @return Connection
     * @throws \Doctrine\DBAL\DBALException
     */
    private function masterConnection(bool $forced = null): Connection
    {
        if ($forced !== null) {
            $this->forceMaster = $forced;
        }
        $this->lastConnection = $this->master;
        return $this->master->connection();
    }

    /**
     * @return Connection
     * @throws \Doctrine\DBAL\DBALException
     */
    private function slaveConnection(): ?Connection
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
            $x = $this->randomSlave($this->slaves);
        }

        $this->lastConnection = $x;
        return $x->connection();
    }

    /**
     * @param Slave[] $slaves
     * @return Slave
     */
    private function randomSlave(array $slaves): Slave
    {
        $weights = [];
        foreach ($slaves as $slaveIndex => $slave) {
            if (!$this->isSlaveOk($slave)) {
                continue;
            }
            $weights = array_merge($weights, array_fill(0, $slave->getWeight(), $slaveIndex));
        }
        return $slaves[$weights[array_rand($weights)]];
    }

    /**
     * @return ExtendedServer|null
     */
    public function getLastConnection(): ?ExtendedServer
    {
        return $this->lastConnection;
    }

    /**
     * @return Slave[]|null
     */
    public function slaves(): ?array
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
        $caller = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 2)[1]['function'];

        if ($this->forceMaster || $caller === 'executeUpdate') {
            $cnx = $this->masterConnection();
        } else if ($caller === 'executeQuery') {
            if (preg_match('/\b(DELETE|UPDATE|INSERT|REPLACE)\b/i', $prepareString)) {
                $cnx = $this->masterConnection();
                error_log('should call executeUpdate()');
            } else {
                $cnx = $this->slaveConnection();
            }
        } else if (preg_match('/\b(DELETE|UPDATE|INSERT|REPLACE)\b/i', $prepareString)) {
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
        return $this->lastConnection->connection()->errorCode();
    }

    /**
     * Returns extended error information associated with the last operation on the database handle.
     *
     * @return array
     */
    public function errorInfo()
    {
        return $this->lastConnection->connection()->errorInfo();
    }

    private function hasCache() {
        return $this->cache !== null;
    }

    private function getCacheKey(Slave $slave) {
        return 'Slave_' . md5(serialize($slave->dbalConfig()));
    }

    public function setSlaveStatus(Slave $slave, bool $running, int $delay = null) {
        if ($this->hasCache()) {
            $this->cache->setCacheItem($this->getCacheKey($slave), ['running' => $running, 'delay' => $delay], $this->slaveStatusCacheTtl);
        }
        return ['running' => $running, 'delay' => $delay];
    }

    private function isSlaveOK(Slave $slave): bool {
        if ($this->hasCache()) {
            $data = $this->cache->getCacheItem($this->getCacheKey($slave));
            if (is_array($data)) {
                if ($data['runnning'] === false || $data['delay'] > $this->maxSlaveDelay) {
                    return false;
                }
            }
        }
        return true;
    }
}
