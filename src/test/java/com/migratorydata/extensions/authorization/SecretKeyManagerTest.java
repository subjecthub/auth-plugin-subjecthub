package com.migratorydata.extensions.authorization;

import org.junit.Test;

public class SecretKeyManagerTest {

    @Test
    public void testSecretKeyManager() throws InterruptedException {
        SecretKeyManager secretKeyManager = new SecretKeyManager();
        new Thread(secretKeyManager).start();

        Thread.sleep(5000);
    }
}
