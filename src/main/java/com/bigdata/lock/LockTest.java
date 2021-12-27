package com.bigdata.lock;

import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

/**
 * @author 1110734@cecdat.com
 * @version 1.0.0
 */
public class LockTest {

    public static void main(String[] args) {

        Config config = new Config();
        config.useSingleServer().setAddress("redis://bigData04:6379");

        RedissonClient client = Redisson.create(config);
        RLock lock = client.getLock("lock");

        try {
            lock.lock();
        }finally {
            lock.unlock();
        }
    }
}
