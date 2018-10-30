<?php

namespace Ez\DbLinker;


class Slave extends ExtendedServer
{
    /**
     * @var int
     */
    protected $weight;

    /**
     * @return int
     */
    public function getWeight(): int
    {
        return $this->weight;
    }

    /**
     * @param Slave[] $slaves
     * @return Slave
     */
    public static function random(array $slaves): Slave
    {
        $weights = [];
        foreach ($slaves as $slaveIndex => $slave) {
//            if (!$this->isSlaveOk($slaveIndex)) {
//                continue;
//            }
            $weights = array_merge($weights, array_fill(0, $slave->getWeight(), $slaveIndex));
        }
        return $slaves[$weights[array_rand($weights)]];
    }

    public function status() {
        if (stripos($this->driver, 'pgsql') !== false) {
            try {
                $sss = $this->connection()->query('SELECT now() - pg_last_xact_replay_timestamp() AS replication_lag')->fetch();
                return $this->setSlaveStatus(true, $sss['replication_lag']);
            } catch (\Exception $e) {
                return $this->setSlaveStatus(false, null);
            }
        } else {
            try {
                $sss = $this->connection()->query('SHOW SLAVE STATUS')->fetch();
                if ($sss['Slave_IO_Running'] === 'No' || $sss['Slave_SQL_Running'] === 'No') {
                    // slave is STOPPED
                    return $this->setSlaveStatus(false, null);
                } else {
                    return $this->setSlaveStatus(true, $sss['Seconds_Behind_Master']);
                }
            } catch (\Exception $e) {
                if (stripos($e->getMessage(), 'Access denied') !== false) {
                    return $this->setSlaveStatus(true, 0);
                }
                return $this->setSlaveStatus(false, null);
            }
        }
    }

    public function isSlaveOk(bool $cachedValue, int $maxdelay = null) {
//        $maxdelay = $maxdelay ?? $this->maxSlaveDelay;
//            $status = $this->getSlaveStatus($slaveIndex);
//            }
//        } else {
//            $status = $this->getSlaveStatus($slaveIndex);
//        }
//        if (!$status['running'] || $status['delay'] >= $maxdelay) {
//            $this->disableCurrentSlave();
//            return false;
//        }
        return true;
    }

    public function disable() {
        $this->weight = 0;
        $this->connection = null;
    }
}