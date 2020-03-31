package com.migratorydata.extensions.authorization;

import org.junit.Test;

public class MySqlAccessTest {

    @Test
    public void testDB() throws Exception {
        MySqlAccess sql = new MySqlAccess();
        sql.readKeysFromDataBase("192.168.10.10:3306", "subjecthub", "homestead", "secret");

        sql.readPublicSubjectsFromDataBase("192.168.10.10:3306", "subjecthub", "homestead", "secret");
    }

}
