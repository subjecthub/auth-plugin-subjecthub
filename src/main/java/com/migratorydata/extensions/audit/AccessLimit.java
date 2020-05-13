package com.migratorydata.extensions.audit;

import com.migratorydata.extensions.authorization.AuthorizationListener;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AccessLimit implements MigratoryDataAccessListener {

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SZ");

    private AuthorizationListener authorizationListener;

    private Map<String, Integer> appCountClients = new HashMap<>(); // appId => connectionsCount

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public AccessLimit() {
        log("@@@@@ AUDIT ACCESS EXTENSION @@@@");
        authorizationListener = AuthorizationListener.getInstance();

        executor.scheduleAtFixedRate(() -> {
            checkLastUpdate();
        }, 0, 5, TimeUnit.SECONDS);
    }

    @Override
    public void onConnect(ConnectEvent connectEvent) {
        executor.execute(() -> {
            log("onConnect = " + connectEvent);

            String appId = getAppId(connectEvent.getToken());

            if (appId == null) {
                return;
            }

            Integer count = appCountClients.get(appId);
            if (count == null) {
                appCountClients.put(appId, Integer.valueOf(1));
            } else {
                appCountClients.put(appId, Integer.valueOf(count.intValue() + 1));
            }
        });
    }

    @Override
    public void onDisconnect(DisconnectEvent disconnectEvent) {
        executor.execute(() -> {
            log("onDisconnect = " + disconnectEvent);

            ConnectEvent castConnectEvent = (ConnectEvent) disconnectEvent;

            String appId = getAppId(castConnectEvent.getToken());
            if (appId == null) {
                return;
            }

            Integer count = appCountClients.get(appId);
            if (count != null && count.intValue() > 0) {
                appCountClients.put(appId, Integer.valueOf(count.intValue() - 1));
            }
        });
    }

    private void checkLastUpdate() {
        Map<String, Integer> copyAppCountClients = new HashMap<>(appCountClients);
        authorizationListener.updateAccessLimit(copyAppCountClients);
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

    private void log(String info) {
        String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
        System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "ACCESS", info));
    }
}
