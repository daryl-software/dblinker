<?php

namespace Ez\DbLinker\Driver\Connection;

use IteratorAggregate;
use Doctrine\DBAL\Driver\Statement;
use Ez\DbLinker\Driver\Connection\RetryConnection;
use Ez\DbLinker\RetryStrategy;

class RetryStatement implements IteratorAggregate, Statement
{
    private $statement;
    private $retryConnection;
    private $retryStrategy;

    use CallAndRetry;

    /**
     * @param Statement $statement
     */
    public function __construct(Statement $statement, RetryConnection $retryConnection, RetryStrategy $retryStrategy)
    {
        $this->statement       = $statement;
        $this->retryConnection = $retryConnection;
        $this->retryStrategy   = $retryStrategy;
    }

    /**
     * {@inheritdoc}
     */
    public function execute($params = NULL)
    {
        return (bool)$this->callAndRetry(__FUNCTION__, func_get_args());
    }

    /**
     * {@inheritdoc}
     */
    private function wrappedObject()
    {
        return $this->statement;
    }

    /**
     * {@inheritdoc}
     */
    private function retryConnection()
    {
        return $this->retryConnection;
    }

    /**
     * {@inheritdoc}
     */
    private function retryStrategy()
    {
        return $this->retryStrategy;
    }

    /**
     * {@inheritdoc}
     */
    public function bindParam($column, &$variable, $type = null, $length = null)
    {
        return $this->statement->bindParam($column, $variable, $type, $length);
    }

    /**
     * {@inheritdoc}
     */
    public function bindValue($param, $value, $type = null)
    {
        return $this->statement->bindValue($param, $value, $type);
    }

    /**
     * {@inheritdoc}
     */
    public function fetch($fetchMode = null)
    {
        return $this->statement->fetch($fetchMode);
    }

    /**
     * {@inheritdoc}
     */
    public function fetchAll($fetchMode = null)
    {
        return $this->statement->fetchAll($fetchMode);
    }

    /**
     * {@inheritdoc}
     */
    public function fetchColumn($columnIndex = 0)
    {
        return $this->statement->fetchColumn($columnIndex);
    }

    /**
     * {@inheritdoc}
     */
    public function errorCode()
    {
        return $this->statement->errorCode();
    }

    /**
     * {@inheritdoc}
     */
    public function errorInfo()
    {
        return $this->statement->errorInfo();
    }

    /**
     * {@inheritdoc}
     */
    public function closeCursor()
    {
        return $this->statement->closeCursor();
    }

    /**
     * {@inheritdoc}
     */
    public function rowCount()
    {
        return $this->statement->rowCount();
    }

    /**
     * {@inheritdoc}
     */
    public function columnCount()
    {
        return $this->statement->columnCount();
    }

    /**
     * {@inheritdoc}
     */
    public function setFetchMode($fetchMode, $arg2 = null, $arg3 = null)
    {
        return $this->statement->setFetchMode($fetchMode, $arg2, $arg3);
    }

    /**
     * {@inheritdoc}
     */
    public function getIterator()
    {
        return $this->statement;
    }
}
