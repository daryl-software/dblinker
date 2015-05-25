# DB Linker

Database connection for master/slaves setup. This package provides drivers for [DBAL](https://github.com/doctrine/dbal). `MasterSlavesDriver` for auto switching between a master and slaves servers & `RetryDriver` for retrying query when some errors occurs.

[![Build Status](https://img.shields.io/travis/ezweb/dblinker.svg)](https://travis-ci.org/ezweb/dblinker)
[![Code Status](https://img.shields.io/scrutinizer/g/ezweb/dblinker.svg)](https://scrutinizer-ci.com/g/ezweb/dblinker/build-status/master)
[![Latest Version](https://img.shields.io/packagist/v/ezweb/dblinker.svg)](https://packagist.org/packages/ezweb/dblinker)
[![License](https://img.shields.io/packagist/l/ezweb/dblinker.svg)](https://packagist.org/packages/ezweb/dblinker)

## Installation

run `composer require ezweb/dblinker`

## Configuration

### Master/Slaves connection

A `MysqlMasterSlavesConnection` wraps a mysql master connection & one or more mysql slaves connection. It execute "READ" queries on one of the slaves connections and "WRITE" queries on the master connection.

```php

// master and slaves configuration
$master = [
    'driver' => 'mysqli',
    'host' => $masterHostname,
    'user' => $masterUsername,
    'password' => $masterPassword,
    'dbname' => $masterDb,
];

$slaves = [
    [
        'driver' => 'mysqli',
        'host' => $slave1Hostname,
        'user' => $slave1Username,
        'password' => $slave1Password,
        'dbname' => $slave1Db,
        'weight' => $slave1Weight,
    ],
    /** other slaves params **/
];

// connection configuration
$params = [
    'master' => $master,
    'slaves' => $slaves,
    'driverClass' => 'Ez\DbLinker\Driver\MysqlMasterSlavesDriver',
];

// Doctrine\DBAL\Configuration
$connection = Doctrine\DBAL\DriverManager::getConnection($params);

var_dump($connection->fetchColumn('SELECT 1')); // slave
var_dump($connection->exec('INSERT INTO…')); // master
```

### Retry connection

A `MysqlRetryConnection` wraps another mysql connection. Its goal is to transparently re-execute queries that provokes erros when it's possible to recover automatically.

```php

// master and slaves configuration
$params = [
    'connectionParams' => [
        /** mysql master/slaves, mysqli or pdo_mysql parameters **/
    ],
    'driverClass' => 'Ez\DbLinker\Driver\MysqlRetryDriver',
    'retryStrategy' => new Ez\DbLinker\RetryStrategy\MysqlRetryStrategy,
]

// Doctrine\DBAL\Configuration
$connection = Doctrine\DBAL\DriverManager::getConnection($params);

var_dump($connection->fetchColumn('SELECT 1')); // nothing special
var_dump($connection->exec('SET SESSION WAIT_TIMEOUT = 1'));
sleep(2);
var_dump($connection->fetchColumn('SELECT 1')); // Connection will catch "MySQL has gone away", re-execute the query and return the results as if nothing happened
```

## Usage

When using `Doctrine\DBAL\DriverManager`, the `$connection` it returns is an instance of `Doctrine\DBAL\Connection`, that wraps one or more `*Connection`.
Take a look at [its documentation](http://docs.doctrine-project.org/projects/doctrine-dbal/en/latest/reference/data-retrieval-and-manipulation.html) for more information.

Note: You can nest a `MysqlMasterSlavesConnection` in a `MysqlRetryConnection`.

## Tests

To run the test suite, you need Composer and Behat:

```bash
$ composer install
$ behat
```

If you have Docker & Docker-Compose you can run `docker-compose run behat` instead
