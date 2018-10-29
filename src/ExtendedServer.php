<?php

namespace Ez\DbLinker;


use Doctrine\DBAL\DriverManager;
use mysql_xdevapi\Exception;

abstract class ExtendedServer
{
    private $host;
    private $user;
    private $password;
    private $dbname;
    private $drivername;
    private $extraParams = [];
    private $driver;
    private $driverOptions;

    protected static $connection;

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

    public function isConnected() :bool {
        return self::$connection !== null;
    }

    public function connection() {
        if (!self::$connection) {
            self::$connection = DriverManager::getConnection($this->dbalConfig());
        }
        return self::$connection;
    }
}