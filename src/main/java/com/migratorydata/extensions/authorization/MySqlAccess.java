package com.migratorydata.extensions.authorization;

import com.migratorydata.extensions.user.Application;
import com.migratorydata.extensions.user.Key;
import com.migratorydata.extensions.user.User;
import com.migratorydata.extensions.user.Users;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class MySqlAccess {

    private Connection connect = null;
    private Statement statement = null;
    private ResultSet resultSet = null;

    public void loadUsers(String dbConnector, String dbIp, String dbName, String user, String password, Users users) throws Exception {
        try {
            String jdbcConnector = "jdbc:mysql://";
            // This will load the MySQL driver, each DB has its own driver
            if (dbConnector.equals("mysql")) {
                Class.forName("com.mysql.cj.jdbc.Driver");
            } else {
                Class.forName("org.mariadb.jdbc.Driver");
                jdbcConnector = "jdbc:mariadb://";
            }

            String url = jdbcConnector + dbIp +"/" + dbName;

            // Setup the connection with the DB
            connect = DriverManager.getConnection(url, user, password);

            // Statements allow to issue SQL queries to the database
            statement = connect.createStatement();

            // Result set get the result of the SQL query

            resultSet = statement.executeQuery("select * from `users` INNER JOIN `limits` ON limits.id=users.limit_id");
            while (resultSet.next()) {
                String subjecthubId = resultSet.getString("users.subjecthub_id");
                int connectionsLimit = resultSet.getInt("limits.connections_limit");
                int publishLimit = resultSet.getInt("limits.publish_limit");

                User u = users.getUser(subjecthubId);
                if (u == null) {
                    u = new User(subjecthubId);
                    users.addUser(subjecthubId, u);
                }
                u.updateLimits(connectionsLimit, publishLimit);
            }

            resultSet = statement.executeQuery("select * from `applications` INNER JOIN `users` ON users.id=applications.user_id");
            while (resultSet.next()) {
                String appId = resultSet.getString("applications.app_id");
                String subjecthubId = resultSet.getString("users.subjecthub_id");

                users.addApplication(subjecthubId, appId);
            }

            resultSet = statement.executeQuery("select * from `keys` INNER JOIN `applications` ON applications.id=keys.application_id INNER JOIN users ON users.id=applications.user_id");
            while (resultSet.next()) {

                String appId = resultSet.getString("applications.app_id");

                String publish_key = resultSet.getString("publish_key");
                String subscribe_key = resultSet.getString("subscribe_key");
                String pub_sub_key = resultSet.getString("pub_sub_key");

                Key key = users.getKey(appId);
                key.addKey(publish_key, Key.KeyType.PUBLISH);
                key.addKey(subscribe_key, Key.KeyType.SUBSCRIBE);
                key.addKey(pub_sub_key, Key.KeyType.PUB_SUB);
            }

            resultSet = statement.executeQuery("select * from subjects INNER JOIN `applications` ON applications.id=subjects.application_id INNER JOIN `users` ON users.id=applications.user_id");
            while (resultSet.next()) {
                String subjecthub_id = resultSet.getString("users.subjecthub_id");
                String appId = resultSet.getString("applications.app_id");
                String subject = resultSet.getString("subjects.subject");
                String subjectType = resultSet.getString("subjects.type");

                String completeSubject = "/" + subjecthub_id + "/" + subject;

                Application.SubjectType appSubjectType = Application.SubjectType.PRIVATE;
                if ("public".equals(subjectType)) {
                    users.addPublicSubject(completeSubject);

                    appSubjectType = Application.SubjectType.PUBLIC;
                }

                Application app = users.getApplication (appId);
                app.addSubject(completeSubject, appSubjectType);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            close();
        }
    }

    // You need to close the resultSet
    private void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }

            if (statement != null) {
                statement.close();
            }

            if (connect != null) {
                connect.close();
            }
        } catch (Exception e) {
        }
    }
}
