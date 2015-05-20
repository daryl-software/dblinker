<?php

namespace Ez\DbLinker\Driver\Connection;

use Doctrine\DBAL\Driver\Connection;
use SplObjectStorage;
use Exception;

class MasterSlavesConnection implements Connection
{
    private $master;
    private $slavesWeights;
    private $connection;

    public function __construct(Connection $master, SplObjectStorage $slavesWeights)
    {
        $this->master  = $master;
        $this->slavesWeights  = $slavesWeights;
        foreach ($slavesWeights as $slave) {
            $weight = $slavesWeights[$slave];
            if (!($slave instanceof Connection)) {
                throw new Exception(
                    'All objects attached to $slavesWeights must implements Doctrine\DBAL\Driver\Connection'
                );
            }
            if (!is_numeric($weight)) {
                throw new Exception('Slave weight must be a numeric');
            }
            if ($weight < 0) {
                throw new Exception('Slave weight must be >= 0');
            }
        }
    }

    public function connectToMaster()
    {
        $this->connection = $this->master;
    }

    public function connectToSlave()
    {
        $this->connection = null;
    }

    public function isConnectedToMaster()
    {
        return $this->connection === $this->master;
    }

    private function connection()
    {
        if ($this->connection === null) {
            $this->connection = $this->chooseASlave() ?: $this->master;
        }
        return $this->connection;
    }

    public function getCurrentConnection()
    {
        return $this->connection();
    }

    private function chooseASlave()
    {
        $totalSlavesWeight = 0;
        foreach ($this->slavesWeights as $slave) {
            $totalSlavesWeight += $this->slavesWeights[$slave];
        }
        if ($totalSlavesWeight < 1) {
            return;
        }
        $weightTarget = mt_rand(1, $totalSlavesWeight);
        foreach ($this->slavesWeights as $slave) {
            $weightTarget -= $this->slavesWeights[$slave];
            if ($weightTarget <= 0) {
                return $slave;
            }
        }
    }

    public function changeSlave()
    {
        $currentConnection = $this->connection();
        $this->connection = null;
        $totalSlavesWeight = 0;
        foreach ($this->slavesWeights as $slave) {
            if ($currentConnection !== $slave) {
                $totalSlavesWeight += $this->slavesWeights[$slave];
            }
        }
        if ($totalSlavesWeight < 1) {
            return;
        }
        $weightTarget = mt_rand(1, $totalSlavesWeight);
        foreach ($this->slavesWeights as $slave) {
            if ($currentConnection === $slave) {
                continue;
            }
            $weightTarget -= $this->slavesWeights[$slave];
            if ($weightTarget <= 0) {
                $this->connection = $slave;
                return;
            }
        }
    }

    public function slaves()
    {
        return $this->slavesWeights;
    }

    /**
     * Prepares a statement for execution and returns a Statement object.
     *
     * @param string $prepareString
     *
     * @return \Doctrine\DBAL\Driver\Statement
     */
    public function prepare($prepareString) {
        $this->connectToMaster();
        return $this->connection()->prepare($prepareString);
    }

    /**
     * Executes an SQL statement, returning a result set as a Statement object.
     *
     * @return \Doctrine\DBAL\Driver\Statement
     */
    public function query() {
        return call_user_func_array([$this->connection(), __FUNCTION__], func_get_args());
    }

    /**
     * Quotes a string for use in a query.
     *
     * @param string  $input
     * @param integer $type
     *
     * @return string
     */
    public function quote($input, $type=\PDO::PARAM_STR) {
        return $this->connection()->quote($input, $type);
    }

    /**
     * Executes an SQL statement and return the number of affected rows.
     *
     * @param string $statement
     *
     * @return integer
     */
    public function exec($statement) {
        $this->connectToMaster();
        return $this->connection()->exec($statement);
    }

    /**
     * Returns the ID of the last inserted row or sequence value.
     *
     * @param string|null $name
     *
     * @return string
     */
    public function lastInsertId($name = null) {
        return $this->connection()->lastInsertId($name);
    }

    /**
     * Initiates a transaction.
     *
     * @return boolean TRUE on success or FALSE on failure.
     */
    public function beginTransaction() {
        $this->connectToMaster();
        return $this->connection()->beginTransaction();
    }

    /**
     * Commits a transaction.
     *
     * @return boolean TRUE on success or FALSE on failure.
     */
    public function commit() {
        $this->connectToMaster();
        return $this->connection()->commit();
    }

    /**
     * Rolls back the current transaction, as initiated by beginTransaction().
     *
     * @return boolean TRUE on success or FALSE on failure.
     */
    public function rollBack() {
        $this->connectToMaster();
        return $this->connection()->rollBack();
    }

    /**
     * Returns the error code associated with the last operation on the database handle.
     *
     * @return string|null The error code, or null if no operation has been run on the database handle.
     */
    public function errorCode() {
        return $this->connection()->errorCode();
    }

    /**
     * Returns extended error information associated with the last operation on the database handle.
     *
     * @return array
     */
    public function errorInfo() {
        return $this->connection()->errorInfo();
    }
}
