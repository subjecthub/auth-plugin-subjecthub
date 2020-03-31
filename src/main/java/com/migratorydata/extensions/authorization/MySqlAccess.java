package com.migratorydata.extensions.authorization;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class MySqlAccess {

    private Connection connect = null;
    private Statement statement = null;
    private PreparedStatement preparedStatement = null;
    private ResultSet resultSet = null;

    public Map<String, Key> readKeysFromDataBase(String ip, String db, String username, String password) throws Exception {
        String url = "jdbc:mysql://" + ip +"/" + db;

        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.cj.jdbc.Driver");

            // Setup the connection with the DB
            connect = DriverManager.getConnection(url, username, password);

            // Statements allow to issue SQL queries to the database
            statement = connect.createStatement();

            // Result set get the result of the SQL query
            resultSet = statement.executeQuery("select * from `keys` INNER JOIN `groups` ON groups.id=keys.group_id");
            return writeResultSet(resultSet);
        } catch (Exception e) {
            throw e;
        } finally {
            close();
        }
    }

    public Map<String, Boolean> readPublicSubjectsFromDataBase(String ip, String db, String username, String password) throws Exception {

        Map<String, Boolean> publicSubjects = new HashMap<>();

        String url = "jdbc:mysql://" + ip +"/" + db;

        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.cj.jdbc.Driver");

            // Setup the connection with the DB
            connect = DriverManager.getConnection(url, username, password);

            PreparedStatement statement = connect.prepareStatement("select * from subjects INNER JOIN `groups` ON groups.id=subjects.group_id WHERE subjects.type = ?");
            statement.setString(1, "public");
            ResultSet resultSet = statement.executeQuery();

            System.out.println("Load from database:");
            while (resultSet.next()) {
                String group = resultSet.getString("groups.name");

                String subject = resultSet.getString("subjects.subject");

                System.out.println("group=" + group + ", subject=" + subject);

                publicSubjects.put("/" + group + "/" + subject, Boolean.TRUE);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            close();
        }

        return publicSubjects;
    }

    private Map<String, Key> writeResultSet(ResultSet resultSet) throws SQLException {
        Map<String, Key> groupToKey = new HashMap<>();
        // ResultSet is initially before the first data set
        System.out.println("Load from database:");
        while (resultSet.next()) {
            String group = resultSet.getString("groups.name");

            String publish_key = resultSet.getString("publish_key");
            String subscribe_key = resultSet.getString("subscribe_key");
            String pub_sub_key = resultSet.getString("pub_sub_key");

            System.out.println("group=" + group + ", publish_key=" + publish_key);

            Key key = groupToKey.get(group);
            if (key == null) {
                key = new Key();
                groupToKey.put(group, key);
            }
            key.addKey(publish_key, Key.KeyType.PUBLISH);
            key.addKey(subscribe_key, Key.KeyType.SUBSCRIBE);
            key.addKey(pub_sub_key, Key.KeyType.PUB_SUB);
        }

        return groupToKey;
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
