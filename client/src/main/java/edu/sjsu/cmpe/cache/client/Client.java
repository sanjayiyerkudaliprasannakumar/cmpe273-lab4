package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;


public class Client {
    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");
        CacheServiceInterface cache_A = new DistributedCacheService(
                "http://localhost:3000");
        CacheServiceInterface cache_B = new DistributedCacheService(
                "http://localhost:3001");
        CacheServiceInterface cache_C = new DistributedCacheService(
                "http://localhost:3002");
        ArrayList<CacheServiceInterface> caches = new ArrayList<CacheServiceInterface>();
        caches.add(cache_A);
        caches.add(cache_B);
        caches.add(cache_C);

        CRDTClient client = new CRDTClient(caches, 2);

        System.out.println("Putting 1=>a");
        client.put(1, "a");
        System.out.println("Stop Cache A");
        TimeUnit.SECONDS.sleep(30);
        System.out.println("Putting 1=>b");
        client.put(1, "b");
        System.out.println("Start Cache A");
        TimeUnit.SECONDS.sleep(30);
        System.out.println("Getting key=1");
        String value = client.get(1);
        System.out.println("Found 1=>" + value);
    }
}
