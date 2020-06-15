package com.migratorydata.extensions.authorization;

import com.migratorydata.extensions.user.*;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class MySqlAccess {

    private Connection connect = null;
    private Statement statement = null;
    private Statement batchStatement = null;
    private PreparedStatement preparedStatement = null;
    private ResultSet resultSet = null;

    private String dbConnector;
    private String dbIp;
    private String dbName;
    private String user;
    private String password;

    private String url;

    public MySqlAccess(String dbConnector, String dbIp, String dbName, String user, String password) throws Exception {
        this.dbConnector = dbConnector;
        this.dbIp = dbIp;
        this.dbName = dbName;
        this.user = user;
        this.password = password;

        String jdbcConnector = "jdbc:mysql://";
        // This will load the MySQL driver, each DB has its own driver
        if (dbConnector.equals("mysql")) {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } else {
            Class.forName("org.mariadb.jdbc.Driver");
            jdbcConnector = "jdbc:mariadb://";
        }

        this.url = jdbcConnector + dbIp + "/" + dbName;
    }

    public void loadUsers(Users users) {
        try {

            // Setup the connection with the DB
            connect = DriverManager.getConnection(url, user, password);

            // Statements allow to issue SQL queries to the database
            statement = connect.createStatement();

            // Result set get the result of the SQL query

            resultSet = statement.executeQuery("select * from `users` INNER JOIN `limits` ON limits.id=users.limit_id");
            while (resultSet.next()) {
                int id = resultSet.getInt("users.id");
                String subjecthubId = resultSet.getString("users.subjecthub_id");
                int connectionsLimit = resultSet.getInt("limits.connections_limit");
                int publishLimit = resultSet.getInt("limits.publish_limit");

                User u = users.getUser(subjecthubId);
                if (u == null) {
                    u = new User(id, subjecthubId);
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
                switch (subjectType) {
                    case "private":
                        appSubjectType = Application.SubjectType.PRIVATE;
                        break;
                    case "public":
                        users.addPublicSubject(completeSubject);
                        appSubjectType = Application.SubjectType.PUBLIC;
                        break;
                    case "connector":
                        appSubjectType = Application.SubjectType.CONNECTOR;
                        break;
                }

                Application app = users.getApplication(appId);
                app.addSubject(completeSubject, appSubjectType);
            }

            resultSet = statement.executeQuery("select * from sources INNER JOIN `subjects` as sub on sources.subject_id=sub.id " +
                    "INNER JOIN `configurations` on sources.configuration_id=configurations.id INNER JOIN `users` on configurations.user_id=users.id " +
                    "INNER JOIN `subjects` as configSub on configurations.subject_id=configSub.id " +
                    "WHERE protocol='Kafka'");
            while (resultSet.next()) {
                users.addSource(resultSet.getInt("sources.id"),
                        new KafkaConnector(resultSet.getString("users.subjecthub_id"),
                                resultSet.getString("configSub.subject"),
                                resultSet.getString("sources.endpoint"),
                                resultSet.getString("sub.subject"),
                                resultSet.getString("sources.status")));
            }

            resultSet = statement.executeQuery("select * from subscriptions INNER JOIN `subjects` as sub on subscriptions.subject_id=sub.id " +
                    "INNER JOIN `configurations` on subscriptions.configuration_id=configurations.id INNER JOIN `users` on configurations.user_id=users.id " +
                    "INNER JOIN `subjects` as configSub on configurations.subject_id=configSub.id " +
                    "WHERE protocol='Kafka'");
            while (resultSet.next()) {
                users.addSubscription(resultSet.getInt("subscriptions.id"),
                        new KafkaConnector(resultSet.getString("users.subjecthub_id"),
                                resultSet.getString("configSub.subject"),
                                resultSet.getString("subscriptions.endpoint"),
                                resultSet.getString("sub.subject"),
                                resultSet.getString("subscriptions.status")));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    public void updateSourceStatusById(Integer id, String status) {
        try {
            connect = DriverManager.getConnection(url, user, password);

            preparedStatement = connect.prepareStatement("UPDATE sources SET status = ? WHERE id = ?");

            preparedStatement.setString(1, status);
            preparedStatement.setInt(2, id);

            preparedStatement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    public void updateSubscriptionsStatusById(Integer id, String status) {
        try {
            connect = DriverManager.getConnection(url, user, password);

            preparedStatement = connect.prepareStatement("UPDATE subscriptions SET status = ? WHERE id = ?");

            preparedStatement.setString(1, status);
            preparedStatement.setInt(2, id);

            preparedStatement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    public void updateStats(Map<String, User> u) {
        try {

            HashMap<String, User> users = new HashMap<>(u);

            connect = DriverManager.getConnection(url, user, password);

            // get current values for today.
            // Statements allow to issue SQL queries to the database
            statement = connect.createStatement();
            batchStatement = connect.createStatement();

            // Result set get the result of the SQL query
            resultSet = statement.executeQuery("select * from `home_stats` INNER JOIN `users` ON users.id=home_stats.user_id WHERE home_stats.created_at >= CURDATE() \n" +
                    "  AND home_stats.created_at < CURDATE() + INTERVAL 1 DAY");
            while (resultSet.next()) {
                int id = resultSet.getInt("home_stats.id");
                int max_concurrent_users = resultSet.getInt("home_stats.max_concurrent_users");
                int total_messages = resultSet.getInt("home_stats.total_messages");
                String subjecthub_id = resultSet.getString("users.subjecthub_id");
                // update entry
                User user = users.remove(subjecthub_id);
                if (user == null) {
                    continue;
                }
                int newMessages = user.getAndResetNumberOfNewReceivedMessage();
                int concurrentUsers = user.getMaxConcurrentUsers();

                boolean updateRow = false;

                if (max_concurrent_users < concurrentUsers) {
                    max_concurrent_users = concurrentUsers;
                    updateRow = true;
                }

                if (newMessages > 0) {
                    total_messages += newMessages;
                    updateRow = true;
                }

                if (updateRow) {
                    batchStatement.addBatch(String.format("UPDATE home_stats SET max_concurrent_users = %d, total_messages = %d, updated_at = now() WHERE id = %d", max_concurrent_users, total_messages, id));
                }
            }

            for (User user : users.values()) {
                // insert
                int newMessages = user.getAndResetNumberOfNewReceivedMessage();
                int concurrentUsers = user.getMaxConcurrentUsers();
                int user_id = user.getId();

                batchStatement.addBatch(String.format("INSERT INTO home_stats (user_id, max_concurrent_users, total_messages, created_at) VALUES (%d, %d, %d, now())", user_id, concurrentUsers, newMessages));
            }

            batchStatement.executeBatch();
        } catch (Exception e) {
            e.printStackTrace();
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

            if (preparedStatement != null) {
                preparedStatement.close();
            }

            if (batchStatement != null) {
                batchStatement.close();
            }

            if (connect != null) {
                connect.close();
            }
        } catch (Exception e) {
        }
    }
}
