package com.migratorydata.extensions.authorization;

import com.migratorydata.extensions.authorization.stubs.ClientCredentials;
import com.migratorydata.extensions.authorization.stubs.PublishRequest;
import com.migratorydata.extensions.authorization.stubs.SubscribeRequest;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class AuthorizationManagerTest {

    @Test
    public void testKeyAdd() throws InterruptedException {
        AuthorizationManager authorizationManager = new AuthorizationManager();
        new Thread(authorizationManager).start();

        Thread.sleep(1000);

        JSONObject key = new JSONObject();
        key.put("op", "add_key");
        key.put("topic", "t-topic1");
        key.put("key", "t-topic1:xxx:s.p");
        key.put("publish", true);
        key.put("subscribe", true);

        authorizationManager.onMessage(key.toString());

        String subject = "/t-topic1";
        ClientCredentials clientCredentials = new ClientCredentials("t-topic1:xxx:s.p", "192.168.1.1:5000");
        SubscribeRequest subscribeRequest = new SubscribeRequest(clientCredentials, subject);
        authorizationManager.handleSubscribeCheck(subscribeRequest);

        Thread.sleep(200);
        Assert.assertTrue(subscribeRequest.allowed);

        PublishRequest request = new PublishRequest(clientCredentials, subject);
        authorizationManager.handlePublishCheck(request);
        Thread.sleep(200);
        Assert.assertTrue(request.allowed);
    }

    @Test
    public void testKeyDelete() throws InterruptedException {
        AuthorizationManager authorizationManager = new AuthorizationManager();
        new Thread(authorizationManager).start();

        Thread.sleep(1000);

        JSONObject key = new JSONObject();
        key.put("op", "add_key");
        key.put("topic", "t-topic1");
        key.put("key", "t-topic1:xxx:s.p");
        key.put("publish", true);
        key.put("subscribe", true);

        authorizationManager.onMessage(key.toString());

        JSONObject keyDelete = new JSONObject();
        keyDelete.put("op", "delete_key");
        keyDelete.put("topic", "t-topic1");
        keyDelete.put("key", "t-topic1:xxx:s.p");

        authorizationManager.onMessage(keyDelete.toString());

        String subject = "/t-topic1";
        ClientCredentials clientCredentials = new ClientCredentials("t-topic1:xxx:s.p", "192.168.1.1:5000");
        SubscribeRequest subscribeRequest = new SubscribeRequest(clientCredentials, subject);
        authorizationManager.handleSubscribeCheck(subscribeRequest);

        Thread.sleep(200);
        Assert.assertFalse(subscribeRequest.allowed);

        PublishRequest request = new PublishRequest(clientCredentials, subject);
        authorizationManager.handlePublishCheck(request);
        Thread.sleep(200);
        Assert.assertFalse(request.allowed);
    }
}
