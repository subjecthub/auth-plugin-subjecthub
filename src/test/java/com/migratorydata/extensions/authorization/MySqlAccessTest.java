package com.migratorydata.extensions.authorization;

import com.migratorydata.extensions.user.User;
import com.migratorydata.extensions.user.Users;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MySqlAccessTest {

    @Test
    public void testDB() throws Exception {
        Users users = new Users();
        MySqlAccess sql = new MySqlAccess("mysql","192.168.10.10:3306", "subjecthub", "homestead", "secret");
        sql.loadUsers(users);

        System.out.println(users);
    }

    @Test
    public void testStatsUpdate() throws Exception {
        Map<String, User> users = new HashMap<>();
        User user = new User(1, "bestscore");
        user.updateLimits(100, 100);
        user.addNewReceivedMessages(15);
        user.countConnections("s1", 30);

        users.put("bestscore", user);

        MySqlAccess sql = new MySqlAccess("mysql","192.168.10.10:3306", "subjecthub", "homestead", "secret");
        sql.updateStats(users);
    }

    @Test
    public void testStatsUpdate2() throws Exception {
        Map<String, User> users = new HashMap<>();
        User user = new User(2, "stocks");
        user.updateLimits(100, 100);
        user.addNewReceivedMessages(2);
        user.countConnections("s1", 40);

        users.put("stocks", user);

        MySqlAccess sql = new MySqlAccess("mysql","192.168.10.10:3306", "subjecthub", "homestead", "secret");
        sql.updateStats(users);
    }

}
