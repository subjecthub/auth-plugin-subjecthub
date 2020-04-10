package com.migratorydata.extensions.authorization;

import com.migratorydata.extensions.user.Users;
import org.junit.Test;

public class MySqlAccessTest {

    @Test
    public void testDB() throws Exception {
        Users users = new Users();
        MySqlAccess sql = new MySqlAccess();
        sql.loadUsers("mysql","192.168.10.10:3306", "subjecthub", "homestead", "secret", users);

        System.out.println(users);
    }

}
