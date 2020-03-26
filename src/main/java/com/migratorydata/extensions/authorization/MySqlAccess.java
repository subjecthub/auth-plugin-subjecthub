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

    public Map<String, Key> readDataBase(String ip, String db, String username, String password) throws Exception {
        String url = "jdbc:mysql://" + ip +"/" + db;

        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.jdbc.Driver");

            // Setup the connection with the DB
            connect = DriverManager.getConnection(url, username, password);

            // Statements allow to issue SQL queries to the database
            statement = connect.createStatement();

            // Result set get the result of the SQL query
            resultSet = statement.executeQuery("select * from subjects INNER JOIN `keys` ON keys.id=subjects.key_id INNER JOIN users ON users.id=subjects.user_id");
            return writeResultSet(resultSet);
        } catch (Exception e) {
            throw e;
        } finally {
            close();
        }
    }

    private Map<String, Key> writeResultSet(ResultSet resultSet) throws SQLException {
        Map<String, Key> subjectToKey = new HashMap<>();
        // ResultSet is initially before the first data set
        System.out.println("Load from database:");
        while (resultSet.next()) {
            String subject = resultSet.getString("subject");
            String username = resultSet.getString("username");

            String publish_key = resultSet.getString("publish_key");
            String subscribe_key = resultSet.getString("subscribe_key");
            String pub_sub_key = resultSet.getString("pub_sub_key");
            String type = resultSet.getString("type");

            System.out.println("subject=" + subject + ", publish_key=" + publish_key + ", username=" + username);

            subjectToKey.put("/" + username + "/" + subject, new Key("private".equals(type), publish_key, subscribe_key, pub_sub_key));
        }

        return subjectToKey;
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
