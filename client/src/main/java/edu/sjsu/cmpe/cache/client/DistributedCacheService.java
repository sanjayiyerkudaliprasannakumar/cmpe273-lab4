package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

import java.util.concurrent.Future;

/**
 * Distributed cache service
 *
 */
public class DistributedCacheService implements CacheServiceInterface {
    private final String cacheServerUrl;

    public DistributedCacheService(String serverUrl) {
        this.cacheServerUrl = serverUrl;
    }

    /**
     * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#get(long)
     */
    @Override
    public String get(long key) {
        Future<HttpResponse<JsonNode>> future = Unirest.get(cacheServerUrl + "/cache/{key}")
                .header("accept", "application/json")
                .routeParam("key", Long.toString(key))
                .asJsonAsync(new Callback<JsonNode>() {
                    public void failed(UnirestException e) {
                        System.out.println(e.getMessage());
                        CRDTClient.readLatch.countDown();
                    }

                    public void completed(HttpResponse<JsonNode> response) {
                        if(response.getCode() == 200) {
                            String value = response.getBody().getObject().getString("value");
                            Integer count = 1;
                            if (CRDTClient.readCounts.containsKey(value)) {
                                count = CRDTClient.readCounts.get(value) + 1;
                            }
                            CRDTClient.readCounts.put(value, count);
                            CRDTClient.readLatch.countDown();
                        } else {
                            CRDTClient.readLatch.countDown();
                        }
                    }

                    public void cancelled() {
                        System.out.println("The request has been cancelled");
                        CRDTClient.readLatch.countDown();
                    }

                });
        return "";
    }

    /**
     * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#put(long,
     *      java.lang.String)
     */
    @Override
    public void put(long key, String value) {
        Future<HttpResponse<JsonNode>> future = Unirest.put(cacheServerUrl + "/cache/{key}/{value}")
                .header("accept", "application/json")
                .routeParam("key", Long.toString(key))
                .routeParam("value", value)
                .asJsonAsync(new Callback<JsonNode>() {

                    public void failed(UnirestException e) {
                        System.out.println(e.getMessage());
                        CRDTClient.writeLatch.countDown();
                    }

                    public void completed(HttpResponse<JsonNode> response) {
                        CRDTClient.writeSuccesses.incrementAndGet();
                        CRDTClient.writeLatch.countDown();
                    }

                    public void cancelled() {
                        System.out.println("The request has been cancelled");
                        CRDTClient.writeLatch.countDown();
                    }

                });
    }

    public void delete(long key) {
        Future<HttpResponse<JsonNode>> future = Unirest.delete(cacheServerUrl + "/cache/{key}")
                .routeParam("key", Long.toString(key))
                .asJsonAsync(new Callback<JsonNode>() {

                    public void failed(UnirestException e) {
                        System.out.println(e.getMessage());
                    }

                    public void completed(HttpResponse<JsonNode> response) {
                    }

                    public void cancelled() {
                        System.out.println("The request has been cancelled!");
                    }

                });
    }
}
