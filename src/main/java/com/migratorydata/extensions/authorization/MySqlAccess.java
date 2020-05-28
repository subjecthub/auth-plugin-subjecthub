package com.migratorydata.extensions.authorization;

import com.migratorydata.extensions.user.*;

import java.sql.*;

public class MySqlAccess {

    private Connection connect = null;
    private Statement statement = null;
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

        this.url = jdbcConnector + dbIp +"/" + dbName;
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
                    case "source":
                        appSubjectType = Application.SubjectType.SOURCE;
                        break;
                    case "subscription":
                        appSubjectType = Application.SubjectType.SUBSCRIPTION;
                        break;
                    case "connector":
                        appSubjectType = Application.SubjectType.CONNECTOR;
                        break;
                }

                Application app = users.getApplication(appId);
                app.addSubject(completeSubject, appSubjectType);
            }

            resultSet = statement.executeQuery("select * from sources INNER JOIN `subjects` on sources.subject_id=subjects.id " +
                    "INNER JOIN `applications` on subjects.application_id=applications.id INNER JOIN `users` on applications.user_id=users.id " +
                    "INNER JOIN `configurations` on sources.configuration_id=configurations.id " +
                    "WHERE protocol='Kafka'");
            while (resultSet.next()) {
                users.addSource(resultSet.getInt("sources.id"),
                        new KafkaConnector(resultSet.getString("users.subjecthub_id"),
                                resultSet.getString("configurations.metadata"),
                                resultSet.getString("sources.endpoint"),
                                resultSet.getString("subjects.subject")));
            }

            resultSet = statement.executeQuery("select * from subscriptions INNER JOIN `subjects` on subscriptions.subject_id=subjects.id " +
                    "INNER JOIN `applications` on subjects.application_id=applications.id INNER JOIN `users` on applications.user_id=users.id " +
                    "INNER JOIN `configurations` on subscriptions.configuration_id=configurations.id " +
                    "WHERE protocol='Kafka'");
            while (resultSet.next()) {
                users.addSubscription(resultSet.getInt("subscriptions.id"),
                        new KafkaConnector(resultSet.getString("users.subjecthub_id"),
                                resultSet.getString("configurations.metadata"),
                                resultSet.getString("subscriptions.endpoint"),
                                resultSet.getString("subjects.subject")));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    public void saveConnectionsStats(Users users) {
        try {
            connect = DriverManager.getConnection(url, user, password);

            for (User user : users.getUsers().values()) {
                int connections = user.getConnectionsCount();
                int userId = user.getId();

                preparedStatement = connect.prepareStatement("INSERT INTO connections_stats (user_id, connections) VALUES (?,?)");

                preparedStatement.setInt(1, userId);
                preparedStatement.setInt(2, connections);

                int row = preparedStatement.executeUpdate();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    public void saveMessagesStats(Users users) {
        try {
            connect = DriverManager.getConnection(url, user, password);

            for (User user : users.getUsers().values()) {
                int messages = user.getMessagesCount();
                int userId = user.getId();

                preparedStatement = connect.prepareStatement("INSERT INTO messages_stats (user_id, messages) VALUES (?,?)");

                preparedStatement.setInt(1, userId);
                preparedStatement.setInt(2, messages);

                int row = preparedStatement.executeUpdate();
            }
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

            if (connect != null) {
                connect.close();
            }
        } catch (Exception e) {
        }
    }
}
