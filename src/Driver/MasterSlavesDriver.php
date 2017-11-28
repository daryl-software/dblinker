<?php

namespace Ez\DbLinker\Driver;

use Ez\DbLinker\Driver\Connection\MasterSlavesConnection;
use Cache\Adapter\Apcu\ApcuCachePool;

trait MasterSlavesDriver
{
    use WrapperDriver;

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
    public function connect(Array $params, $username = null, $password = null, Array $driverOptions = []) {
        $cache = array_key_exists("config_cache", $driverOptions) ? $driverOptions["config_cache"] : null;

        $configParams = array_intersect_key($params, array_flip(["master", "slaves"]));
        $key = "dblinker.master-slave-config.".hash("sha256", serialize($configParams));

        $config = null;

        // default cache, disable it with $cache = false
        if ($cache !== false && $cache === null) {
            $cache = new ApcuCachePool();
            $driverOptions["config_cache"] = $cache;
        }

        if ($cache !== null) {
            assert($driverOptions["config_cache"] instanceof \Psr\Cache\CacheItemPoolInterface);
            $cacheItem = $cache->getItem($key);
            $config = $cacheItem->get();
        }

        if ($config === null) {
            $config = $this->config($configParams, $driverOptions);
        }

        if ($cache !== null) {
            $cacheItem->set($config);
            $cacheItem->expiresAt(time()+60);
            $cache->save($cacheItem);
        }

        return new MasterSlavesConnection($config["master"], $config["slaves"]);
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
