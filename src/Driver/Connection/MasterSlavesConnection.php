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
    private $currentConnectionFactory;

    public function __construct($master, SplObjectStorage $slavesWeights)
    {
        $this->master = $master;
        $this->checkSlavesWeights($slavesWeights);
        $this->slavesWeights = $slavesWeights;
    }

    private function checkSlavesWeights(SplObjectStorage $slavesWeights)
    {
        foreach ($slavesWeights as $slave) {
            if ((int)$slavesWeights[$slave] < 0) {
                throw new Exception('Slave weight must be >= 0');
            }
        }
    }

    public function connectToMaster()
    {
        $this->currentConnectionFactory = $this->master;
        $this->connection = null;
    }

    public function connectToSlave()
    {
        $this->currentConnectionFactory = null;
        $this->connection = null;
    }

    public function isConnectedToMaster()
    {
        return $this->currentConnectionFactory === $this->master;
    }

    private function connection()
    {
        if ($this->connection === null) {
            if ($this->currentConnectionFactory === null) {
                $this->currentConnectionFactory = $this->chooseASlave() ?: $this->master;
            }
            $factory = $this->currentConnectionFactory;
            $this->connection = $factory();
        }
        return $this->connection;
    }

    public function getCurrentConnection()
    {
        return $this->connection();
    }

    private function chooseASlave()
    {
        $totalSlavesWeight = $this->totalSlavesWeight();
        if ($totalSlavesWeight < 1) {
            return null;
        }
        $weightTarget = mt_rand(1, $totalSlavesWeight);
        foreach ($this->slavesWeights as $slave) {
            $weightTarget -= $this->slavesWeights[$slave];
            if ($weightTarget <= 0) {
                return $slave;
            }
        }
    }

    private function totalSlavesWeight()
    {
        $weight = 0;
        foreach ($this->slavesWeights as $slave) {
            $weight += $this->slavesWeights[$slave];
        }
        return $weight;
    }

    public function disableCurrentSlave()
    {
        $this->slavesWeights->detach($this->currentConnectionFactory);
        $this->currentConnectionFactory = null;
        $this->connection = null;
    }

    public function slaves()
    {
        return clone $this->slavesWeights;
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
        $this->connectToMaster();
        return $this->connection()->prepare($prepareString);
    }

    /**
     * Executes an SQL statement, returning a result set as a Statement object.
     *
     * @return \Doctrine\DBAL\Driver\Statement
     */
    public function query()
    {
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
    public function quote($input, $type = \PDO::PARAM_STR)
    {
        return $this->connection()->quote($input, $type);
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
        return $this->connection()->exec($statement);
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
        return $this->connection()->lastInsertId($name);
    }

    /**
     * Initiates a transaction.
     *
     * @return boolean TRUE on success or FALSE on failure.
     */
    public function beginTransaction()
    {
        $this->connectToMaster();
        return $this->connection()->beginTransaction();
    }

    /**
     * Commits a transaction.
     *
     * @return boolean TRUE on success or FALSE on failure.
     */
    public function commit()
    {
        $this->connectToMaster();
        return $this->connection()->commit();
    }

    /**
     * Rolls back the current transaction, as initiated by beginTransaction().
     *
     * @return boolean TRUE on success or FALSE on failure.
     */
    public function rollBack()
    {
        $this->connectToMaster();
        return $this->connection()->rollBack();
    }

    /**
     * Returns the error code associated with the last operation on the database handle.
     *
     * @return string|null The error code, or null if no operation has been run on the database handle.
     */
    public function errorCode()
    {
        return $this->connection()->errorCode();
    }

    /**
     * Returns extended error information associated with the last operation on the database handle.
     *
     * @return array
     */
    public function errorInfo()
    {
        return $this->connection()->errorInfo();
    }
}
