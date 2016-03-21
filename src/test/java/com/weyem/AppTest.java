package com.weyem;

import com.weyem.utils.jdbc.JdbcUtils;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    @Test
    public void method_1() throws SQLException
    {
        JdbcUtils jdbcUtils = new JdbcUtils("jdbc.properties");
        List<Map<String, Object>> maps = jdbcUtils.queryForMapList("SELECT * FROM students");
        System.out.println(maps);
    }
}
