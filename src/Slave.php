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

    public function disable() {
        $this->weight = 0;
        static::$connection = null;
    }
}