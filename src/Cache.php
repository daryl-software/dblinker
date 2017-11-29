<?php

namespace Ez\DbLinker;

use Exception;
use stdClass;
use Cache\Adapter\Apcu\ApcuCachePool;

class Cache
{
    private $cache;
    private $defaultCache = "Cache\Adapter\Apcu\ApcuCachePool";

    public function __construct($cache = null)
    {
        $this->setCache($cache);
    }

    // null = set to default cache (Apcu)
    // false = disable cache
    // \Psr\Cache\CacheItemPoolInterface class
    private function setCache($cache = null) {
        if ($cache !== false && $cache === null) {
            $cache = new $this->defaultCache();
        }

        if ($cache) {
            assert($cache instanceof \Psr\Cache\CacheItemPoolInterface);
        }
        $this->cache = $cache;
    }

    public function getCache() {
        return $this->cache;
    }

    public function hasCache() {
        return (bool)$this->cache;
    }

    public function getCacheItem($key) {
        if ($this->hasCache()) {
            $cacheItem = $this->getCache()->getItem($key);
            return $cacheItem->get();
        }
        return null;
    }

    public function setCacheItem($key, $data, $ttl = 0) {
        if ($this->hasCache()) {
            $cacheItem = $this->getCache()->getItem($key);
            $cacheItem->set($data);
            if ($ttl > 0) {
                $cacheItem->expiresAt(time()+$ttl);
            }
            return $this->getCache()->save($cacheItem);
        }
        return null;
    }

    public function disableCache() {
        return $this->setCache(false);
    }
}
