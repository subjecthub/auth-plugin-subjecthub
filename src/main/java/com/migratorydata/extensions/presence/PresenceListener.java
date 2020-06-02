package com.migratorydata.extensions.presence;

import com.migratorydata.extensions.authorization.AuthorizationListener;

import java.text.SimpleDateFormat;
import java.util.Date;

public class PresenceListener implements MigratoryDataPresenceListener {

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SZ");

    private AuthorizationListener authorizationListener;

    public PresenceListener() {
        log("@@@@@ PRESENCE EXTENSION @@@@");

        authorizationListener = AuthorizationListener.getInstance();
    }

    @Override
    public void onClusterMessage(MigratoryDataPresenceListener.Message message) {
        log("onClusterMessage" + message.toString());

        String closure = (String) message.getAdditionalInfo().get("closure");
        if (closure != null && closure.startsWith("request-")) {
            authorizationListener.onConnectorRequest(message);
        }
    }

    @Override
    public void onUserPresence(MigratoryDataPresenceListener.User event) {
        // ignore
    }

    private void log(String info) {
        String isoDateTime = sdf.format(new Date(System.currentTimeMillis()));
        System.out.println(String.format("[%1$s] [%2$s] %3$s", isoDateTime, "PRESENCE", info));
    }

}