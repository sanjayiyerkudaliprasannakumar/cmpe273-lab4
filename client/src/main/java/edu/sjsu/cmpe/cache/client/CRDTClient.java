package edu.sjsu.cmpe.cache.client;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by K P Sanjay Iyer on 5/23/15.
 */
public class CRDTClient {
    public List<CacheServiceInterface> caches;
    public int writeSuccessCount;

    public static CountDownLatch writeLatch;
    public static CountDownLatch readLatch;
    public static AtomicInteger writeSuccesses;
    public static HashMap<String, Integer> readCounts;

    public CRDTClient(List<CacheServiceInterface> caches, int writeSuccessCount) {
        this.caches = caches;
        this.writeSuccessCount = writeSuccessCount;
    }

    public boolean put(long key, String value) {
        writeLatch = new CountDownLatch(caches.size());
        writeSuccesses = new AtomicInteger();

        for(CacheServiceInterface cache : caches) {
            cache.put(key, value);
        }

        try {
            writeLatch.await();
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        }
        if(writeSuccesses.get() < writeSuccessCount) {
            System.out.println("Write failed");
            for(CacheServiceInterface cache : caches) {
                cache.delete(key);
            }
            return false;
        }
        return true;
    }

    public String get(long key) {
        readLatch = new CountDownLatch(caches.size());
        readCounts = new HashMap<String, Integer>();

        for(CacheServiceInterface cache : caches) {
            cache.get(key);
        }

        try {
            readLatch.await();
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        }

        String correctValue = "";
        int maxCount = 0;
        for(String value : readCounts.keySet()) {
            if(readCounts.get(value) > maxCount) {
                correctValue = value;
                maxCount = readCounts.get(value);
            }
        }
        this.put(key, correctValue);
        return correctValue;
    }
}
