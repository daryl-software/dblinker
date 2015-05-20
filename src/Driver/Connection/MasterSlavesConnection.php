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
        $this->master = $master;
        $this->checkSlavesWeights($slavesWeights);
        $this->slavesWeights = $slavesWeights;
    }

    private function checkSlavesWeights(SplObjectStorage $slavesWeights)
    {
        foreach ($slavesWeights as $slave) {
            $weight = $slavesWeights[$slave];
            if (!$slave instanceof Connection) {
                throw new Exception(
                    'All objects attached to $slavesWeights must implements Doctrine\DBAL\Driver\Connection'
                );
            }
            if ((int)$weight < 0) {
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
            $this->connection = $this->chooseASlave();
        }
        return $this->connection;
    }

    public function getCurrentConnection()
    {
        return $this->connection();
    }

    private function chooseASlave(Array $slavesBlacklist = [])
    {
        $slavesWeights = $this->slaves($slavesBlacklist);
        $totalSlavesWeight = $this->totalSlavesWeight($slavesWeights);
        if ($totalSlavesWeight < 1) {
            return $this->master;
        }
        $weightTarget = mt_rand(1, $totalSlavesWeight);
        foreach ($slavesWeights as $slave) {
            $weightTarget -= $slavesWeights[$slave];
            if ($weightTarget <= 0) {
                return $slave;
            }
        }
    }

    private function totalSlavesWeight(SplObjectStorage $slaveWeights)
    {
        $weight = 0;
        foreach ($slaveWeights as $slave) {
            $weight += $this->slavesWeights[$slave];
        }
        return $weight;
    }

    public function changeSlave()
    {
        $this->connection = $this->chooseASlave([$this->connection]);
    }

    public function slaves(Array $slavesBlacklist = [])
    {
        $slaves = new SplObjectStorage;
        foreach ($this->slavesWeights as $slave) {
            if (!in_array($slave, $slavesBlacklist, true)) {
                $slaves->attach($slave, $this->slavesWeights[$slave]);
            }
        }
        return $slaves;
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
