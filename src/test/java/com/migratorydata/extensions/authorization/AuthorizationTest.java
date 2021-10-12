//package com.migratorydata.extensions.authorization;
//
//import com.migratorydata.extensions.audit.AccessLimit;
//import com.migratorydata.extensions.audit.MigratoryDataAccessListener;
//import com.migratorydata.extensions.audit.PublishLimit;
//import com.migratorydata.extensions.authorization.stubs.*;
//import org.junit.Assert;
//import org.junit.Test;
//
//public class AuthorizationTest {
//
//    @Test
//    public void testPublishLimit() throws InterruptedException {
//        AuthorizationListener listener = new AuthorizationListener();
//
//        PublishLimit publishLimit = new PublishLimit();
//        Thread.sleep(5000);
//
//        String subject = "/laurentiu/sbj_private2";
//
//        ClientCredentials clientCredentials = new ClientCredentials("9gTVgEPnpc:O4yEnxrSHmYLRXRo", "192.168.1.1:5000");
//
//        while (true) {
//            PublishRequest request = new PublishRequest(clientCredentials, subject );
//            listener.onPublish(request);
//            Thread.sleep(200);
//            Assert.assertTrue(request.allowed);
//
//            publishLimit.onMessage(new MessageEventStub(subject));
//        }
//    }
//
//    @Test
//    public void testConnectionsLimit() throws InterruptedException {
//        AuthorizationListener listener = new AuthorizationListener();
//
//        AccessLimit accessLimit = new AccessLimit();
//        Thread.sleep(5000);
//
//        String subject = "/laurentiu/sbj_private2";
//
//        ClientCredentials clientCredentials = new ClientCredentials("9gTVgEPnpc:od2fE2Rk4EQIg0cZ", "192.168.1.1:5000");
//
//        int countport = 10000;
//        while (true) {
//            countport++;
//            ConnectEventStub connectEventStub = new ConnectEventStub("9gTVgEPnpc:od2fE2Rk4EQIg0cZ", "192.168.1.1" + String.valueOf(countport));
//            accessLimit.onConnect(connectEventStub);
//
//            SubscribeRequest subscribeRequest = new SubscribeRequest(clientCredentials, subject);
//            listener.onSubscribe(subscribeRequest);
//            Thread.sleep(200);
//            Assert.assertTrue(subscribeRequest.allowed);
//
//            //accessLimit.onDisconnect(new DisconnectEventStub("9gTVgEPnpc:od2fE2Rk4EQIg0cZ", "192.168.1.1" + String.valueOf(countport)));
//        }
//    }
//
//}
