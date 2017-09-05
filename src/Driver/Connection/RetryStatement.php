<?php

namespace Ez\DbLinker\Driver\Connection;

use IteratorAggregate;
use Doctrine\DBAL\Driver\Statement;
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
        return (bool)$this->callAndRetry(function () use ($params) {
            return $this->statement->execute($params);
        }, $this->retryStrategy, $this->retryConnection);
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
    public function fetch($fetchMode = null, $cursorOrientation = \PDO::FETCH_ORI_NEXT, $cursorOffset = 0)
    {
        return $this->statement->fetch($fetchMode, $cursorOrientation, $cursorOffset);
    }

    /**
     * {@inheritdoc}
     */
    public function fetchAll($fetchMode = null, $fetchArgument = null, $ctorArgs = null)
    {
        return $this->statement->fetchAll($fetchMode, $fetchArgument, $ctorArgs);
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
