package com.migratorydata.extensions.authorization;

import com.migratorydata.extensions.user.User;
import com.migratorydata.extensions.user.Users;
import org.junit.Test;

import java.util.Collections;

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
        Users users = new Users();
        users.addUser("bestscore", new User(1, "bestscore"));
        MySqlAccess sql = new MySqlAccess("mysql","192.168.10.10:3306", "subjecthub", "homestead", "secret");
        sql.saveMessagesStats(users, Collections.emptyMap());
        sql.saveConnectionsStats(users);
    }

}
