<?php

namespace Ez\DbLinker\Driver;

use Ez\DbLinker\Driver\Connection\MasterSlavesConnection;
use Cache\Adapter\Apcu\ApcuCachePool;
use Ez\DbLinker\Cache;

trait MasterSlavesDriver
{
    private $cache;
    private $cacheDefaultTtl = 60;

    /**
     * Attempts to create a connection with the database.
     *
     * @param array       $params        All connection parameters passed by the user.
     * @param string|null $username      The username to use when connecting.
     * @param string|null $password      The password to use when connecting.
     * @param array       $driverOptions The driver options to use when connecting.
     *
     * @return \Doctrine\DBAL\Driver\Connection The database connection.
     */
    public function connect(array $params, $username = null, $password = null, array $driverOptions = []) {
        $cacheDriver = array_key_exists("config_cache", $driverOptions) ? $driverOptions["config_cache"] : null;

        $configParams = array_intersect_key($params, array_flip(["master", "slaves"]));
        $key = "dblinker.master-slave-config.".hash("sha256", serialize($configParams));

        $config = null;

        // default cache, disable it with $cache = false
        $cache = new Cache($cacheDriver);

        if ($cache->hasCache()) {
            $config = $cache->getCacheItem($key);
        }

        if ($config === null) {
            $config = $this->config($configParams, $driverOptions);
        }

        if ($cache->hasCache()) {
            $cache->setCacheItem($key, $config, 60);
        }

        return new MasterSlavesConnection($config["master"], $config["slaves"], $cache);
    }

    private function config(array $params, array $driverOptions)
    {
        $driverKey = array_key_exists('driverClass', $params['master']) ? 'driverClass' : 'driver';

        $driverValue = $params['master'][$driverKey];

        $slaves = [];
        foreach ($params['slaves'] as $slaveParams) {
            $slaveParams[$driverKey] = $driverValue;
            $slaves[] = $slaveParams;
        }

        $config = [
            "master" => $params["master"],
            "slaves" => $slaves,
        ];

        if (!array_key_exists("config_transform", $driverOptions)) {
            return $config;
        }

        assert(is_callable($driverOptions["config_transform"]));
        return $driverOptions["config_transform"]($config);
    }

}
