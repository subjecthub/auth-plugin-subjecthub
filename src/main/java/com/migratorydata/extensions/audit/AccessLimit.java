package com.migratorydata.extensions.audit;

import com.migratorydata.extensions.authorization.AuthorizationListener;

import java.util.HashMap;
import java.util.Map;

public class AccessLimit implements MigratoryDataAccessListener {

    private AuthorizationListener authorizationListener;

    private Map<String, String> ipToAppId = new HashMap<>();
    private Map<String, Integer> appCountClients = new HashMap<>();

    private long lastUpdateOfAccessLimit = System.currentTimeMillis();

    public AccessLimit() {
        System.out.println("@@@@@INSTANCE AUDIT ACCESS@@@@");
        authorizationListener = AuthorizationListener.getInstance();
    }

    // Audit.Access
    @Override
    public void onConnect(ConnectEvent connectEvent) {
        System.out.println("onConnect = " + connectEvent);

        String clientAddress = connectEvent.getClientAddress();
        String appId = getAppId(connectEvent.getToken());

        if (appId == null) {
            return;
        }

        ipToAppId.put(clientAddress, appId);
        Integer count = appCountClients.get(appId);
        if (count == null) {
            appCountClients.put(appId, Integer.valueOf(1));
        } else {
            appCountClients.put(appId, Integer.valueOf(count.intValue() + 1));
        }

        checkLastUpdate();
    }

    @Override
    public void onDisconnect(DisconnectEvent disconnectEvent) {
        System.out.println("onDisconnect = " + disconnectEvent);

        String appId = ipToAppId.remove(disconnectEvent.getClientAddress());
        if (appId == null) {
            return;
        }

        Integer count = appCountClients.get(appId);
        if (count != null && count.intValue() > 0) {
            appCountClients.put(appId, Integer.valueOf(count.intValue() - 1));
        }

        checkLastUpdate();
    }

    private void checkLastUpdate() {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastUpdateOfAccessLimit >= 5000) {
            Map<String, Integer> copyAppCountClients = new HashMap<>(appCountClients);
            authorizationListener.updateAccessLimit(copyAppCountClients);

            lastUpdateOfAccessLimit = currentTime;
        }
    }

    private String getAppId(String token) {
        if (token == null) {
            return null;
        }

        int index = token.indexOf(":");
        if (index == -1) {
            return null;
        }
        return token.substring(0, index);
    }

    @Override
    public void onSubscribe(SubscribeEvent subscribeEvent) {
    }

    @Override
    public void onSubscribeWithHistory(SubscribeWithHistoryEvent subscribeWithHistoryEvent) {
    }

    @Override
    public void onSubscribeWithRecovery(SubscribeWithRecoveryEvent subscribeWithRecoveryEvent) {
    }

    @Override
    public void onUnsubscribe(UnsubscribeEvent unsubscribeEvent) {
    }
}
