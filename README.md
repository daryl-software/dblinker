# DB Linker

Database connection for master/slaves setup. This package provides drivers for [DBAL](https://github.com/doctrine/dbal). `MysqlMasterSlavesDriver` for auto switching between a master and slaves MySQL servers & `MysqlRetryDriver` for retrying query when some errors occurs.

## Usage

### Master/Slaves connection

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

A RetryConnection wraps another, catches some kind of errors and re-execute the query.

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
