<?php

namespace Ez\DbLinker;


use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;

abstract class ExtendedServer
{
    protected $host;
    protected $user;
    protected $password;
    protected $dbname;
    protected $extraParams = [];
    protected $driver;
    protected $driverOptions;

    /**
     * @var Connection
     */
    protected $connection;

    public function __construct(array $params)
    {
        foreach ($params as $key => $value) {
            switch ($key) {
                case 'host':
                case 'user':
                case 'password':
                case 'dbname':
                case 'weight':
                case 'driver':
                case 'driverOptions':
                    $this->$key = $value;
                break;
            default:
                throw new \Exception($key . ' = ' . $value);
                $extraParams[$key] = $value;
            }
        }
    }

    private function dbalConfig(): array {
        return [
            'host' => $this->host,
            'user' => $this->user,
            'password' => $this->password,
            'dbname' => $this->dbname,
            'driver' => $this->driver,
            'driverOptions' => $this->driverOptions,
        ];
    }

    public function isConnected(): bool {
        return $this->connection !== null;
    }

    public function __toString()
    {
        return $this->user . '@' . $this->host . ':' . $this->dbname;
    }

    public function close() {
        if ($this->isConnected()) {
            echo \get_class($this) . ' ' . $this . PHP_EOL;
            $this->connection->close();
        }
    }

    /**
     * @return \Doctrine\DBAL\Connection
     * @throws \Doctrine\DBAL\DBALException
     */
    public function connection(): Connection {
        if (!$this->connection) {
            $this->connection = DriverManager::getConnection($this->dbalConfig());
        }
        return $this->connection;
    }

    public function __destruct()
    {
        $this->close();
    }
}