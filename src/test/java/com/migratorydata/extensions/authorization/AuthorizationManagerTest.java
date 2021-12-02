package com.migratorydata.extensions.authorization;

import com.migratorydata.extensions.authorization.stubs.ClientCredentials;
import com.migratorydata.extensions.authorization.stubs.PublishRequest;
import com.migratorydata.extensions.authorization.stubs.SubscribeRequest;
import com.migratorydata.extensions.util.Metric;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class AuthorizationManagerTest {

    @Test
    public void testKeyAdd() throws InterruptedException {
        AuthorizationManager authorizationManager = new AuthorizationManager();
        new Thread(authorizationManager).start();

        Thread.sleep(1000);

        JSONObject key = new JSONObject();
        key.put("op", "add_key");
        key.put("topic", "s");
        key.put("application", "t-topic1");
        key.put("key", "s:t-topic1:xxx:s.p");
        key.put("publish", true);
        key.put("subscribe", true);

        authorizationManager.onMessage(key.toString());

        String subject = "/s/t-topic1";
        ClientCredentials clientCredentials = new ClientCredentials("s:t-topic1:xxx:s.p", "192.168.1.1:5000");
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
    public void testTwoKeys() throws InterruptedException {
        AuthorizationManager authorizationManager = new AuthorizationManager();
        new Thread(authorizationManager).start();

        Thread.sleep(1000);

        JSONObject key1 = new JSONObject();
        key1.put("op", "add_key");
        key1.put("topic", "s");
        key1.put("application", "t-topic1");
        key1.put("key", "s:t-topic1:xxx:s.p");
        key1.put("publish", true);
        key1.put("subscribe", true);

        authorizationManager.onMessage(key1.toString());

        JSONObject key2 = new JSONObject();
        key2.put("op", "add_key");
        key2.put("topic", "s");
        key2.put("application", "t-topic2");
        key2.put("key", "s:t-topic2:xxx:s.p");
        key2.put("publish", true);
        key2.put("subscribe", true);

        authorizationManager.onMessage(key2.toString());

        String subject = "/s/t-topic1";
        ClientCredentials clientCredentials = new ClientCredentials("s:t-topic2:xxx:s.p", "192.168.1.1:5000");
        SubscribeRequest subscribeRequest = new SubscribeRequest(clientCredentials, subject);
        authorizationManager.handleSubscribeCheck(subscribeRequest);

        Thread.sleep(200);
        Assert.assertFalse(subscribeRequest.allowed);

        PublishRequest request = new PublishRequest(clientCredentials, subject);
        authorizationManager.handlePublishCheck(request);
        Thread.sleep(200);
        Assert.assertFalse(request.allowed);

        ClientCredentials clientCredentials2 = new ClientCredentials("s:t-topic1:xxx:s.p", "192.168.1.1:5000");
        SubscribeRequest subscribeRequest2 = new SubscribeRequest(clientCredentials2, subject);
        authorizationManager.handleSubscribeCheck(subscribeRequest2);

        Thread.sleep(200);
        Assert.assertTrue(subscribeRequest2.allowed);
    }

    @Test
    public void testKeyDelete() throws InterruptedException {
        AuthorizationManager authorizationManager = new AuthorizationManager();
        new Thread(authorizationManager).start();

        Thread.sleep(1000);

        JSONObject key = new JSONObject();
        key.put("op", "add_key");
        key.put("topic", "s");
        key.put("application", "t-topic1");
        key.put("key", "s:t-topic1:xxx:s.p");
        key.put("publish", true);
        key.put("subscribe", true);

        authorizationManager.onMessage(key.toString());

        JSONObject keyDelete = new JSONObject();
        keyDelete.put("op", "delete_key");
        keyDelete.put("topic", "s");
        keyDelete.put("application", "t-topic1");
        keyDelete.put("key", "s:t-topic1:xxx:s.p");

        authorizationManager.onMessage(keyDelete.toString());

        String subject = "/s/t-topic1";
        ClientCredentials clientCredentials = new ClientCredentials("s:t-topic1:xxx:s.p", "192.168.1.1:5000");
        SubscribeRequest subscribeRequest = new SubscribeRequest(clientCredentials, subject);
        authorizationManager.handleSubscribeCheck(subscribeRequest);

        Thread.sleep(200);
        Assert.assertFalse(subscribeRequest.allowed);

        PublishRequest request = new PublishRequest(clientCredentials, subject);
        authorizationManager.handlePublishCheck(request);
        Thread.sleep(200);
        Assert.assertFalse(request.allowed);
    }

    @Test
    public void testConnectionsLimit() throws InterruptedException {
        AuthorizationManager authorizationManager = new AuthorizationManager();
        new Thread(authorizationManager).start();

        Thread.sleep(1000);

        JSONObject key = new JSONObject();
        key.put("op", "add_key");
        key.put("topic", "s");
        key.put("application", "t-topic1");
        key.put("key", "s:t-topic1:xxx:s.p");
        key.put("publish", true);
        key.put("subscribe", true);

        authorizationManager.onMessage(key.toString());

        Map<String, Metric> conServer1 = new HashMap<>();
        conServer1.put("t-topic1", new Metric(null, "t-topic1", 99));

        authorizationManager.updateConnections(conServer1, "server1");

        String subject = "/s/t-topic1";
        ClientCredentials clientCredentials = new ClientCredentials("s:t-topic1:xxx:s.p", "192.168.1.1:5000");
        SubscribeRequest subscribeRequest = new SubscribeRequest(clientCredentials, subject);
        authorizationManager.handleSubscribeCheck(subscribeRequest);

        Thread.sleep(200);
        Assert.assertTrue(subscribeRequest.allowed);

        Map<String, Metric> conServer2 = new HashMap<>();
        conServer2.put("t-topic1",new Metric(null, "t-topic1", 2));

        authorizationManager.updateConnections(conServer2, "server2");

        SubscribeRequest subscribeRequest2 = new SubscribeRequest(clientCredentials, subject);
        authorizationManager.handleSubscribeCheck(subscribeRequest2);

        Thread.sleep(200);
        Assert.assertFalse(subscribeRequest2.allowed);
    }

    @Test
    public void testMessagesLimit() throws InterruptedException {
        AuthorizationManager authorizationManager = new AuthorizationManager();
        new Thread(authorizationManager).start();

        Thread.sleep(1000);

        JSONObject key = new JSONObject();
        key.put("op", "add_key");
        key.put("topic", "s");
        key.put("application", "t-topic1");
        key.put("key", "s:t-topic1:xxx:s.p");
        key.put("publish", true);
        key.put("subscribe", true);

        authorizationManager.onMessage(key.toString());

        Map<String, Metric> messagesServer1 = new HashMap<>();
        messagesServer1.put("t-topic1", new Metric("s", "t-topic1", 4999));

        authorizationManager.updateMessages(messagesServer1, "server1");

        String subject = "/s/t-topic1";
        ClientCredentials clientCredentials = new ClientCredentials("s:t-topic1:xxx:s.p", "192.168.1.1:5000");
        PublishRequest publishRequest = new PublishRequest(clientCredentials, subject);
        authorizationManager.handlePublishCheck(publishRequest);
        Thread.sleep(200);
        Assert.assertTrue(publishRequest.allowed);

        Map<String, Metric> conServer2 = new HashMap<>();
        conServer2.put("t-topic1", new Metric("s", "t-topic1", 2));

        authorizationManager.updateMessages(conServer2, "server2");

        PublishRequest publishRequest2 = new PublishRequest(clientCredentials, subject);
        authorizationManager.handlePublishCheck(publishRequest2);
        Thread.sleep(200);
        Assert.assertFalse(publishRequest2.allowed);
    }

    @Test
    public void testConsumerJSonTransform() {
        Map<String, Integer> topicsToConnections = new HashMap<>();
        topicsToConnections.put("t-topic1", 2);
        topicsToConnections.put("t-topic2", 3);
        topicsToConnections.put("t-topic3", 4);


        JSONObject connectionsStats = new JSONObject();
        connectionsStats.put("op", "connections");
        connectionsStats.put("server", "s1");
        connectionsStats.put("connections", new JSONObject(topicsToConnections));

        System.out.println(connectionsStats.toString());
        byte[] data = connectionsStats.toString().getBytes();

        JSONObject result = new JSONObject(new String(data));
        System.out.println(result);
        String op = result.getString("op");
        String serverName = result.getString("server");

        Assert.assertEquals(op, "connections");
        Assert.assertEquals(serverName, "s1");

        Map connections = result.getJSONObject("connections").toMap();

        Assert.assertTrue(topicsToConnections.equals(connections));
    }
}
