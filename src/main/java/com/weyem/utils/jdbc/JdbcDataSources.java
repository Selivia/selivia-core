package com.weyem.utils.jdbc;

import org.apache.commons.dbcp.BasicDataSourceFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author Selivia
 * @version 1.0
 */
public class JdbcDataSources
{

    private DataSource dataSource;
    private Connection con;
    /**
     * 构造初始化 根据配置文件手动创建
     * @param propPath
     */
    public JdbcDataSources(String propPath)
    {

        Properties prop = new Properties();
        InputStream inputStream = JdbcDataSources.class.getClassLoader().getResourceAsStream(propPath);
        try
        {
            prop.load(inputStream);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (inputStream != null)
            {
                try
                {
                    inputStream.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
        initProp(prop);
    }

    /**
     * 构造初始化 根据配置文件手动创建
     * @param prop
     */
    public JdbcDataSources(Properties prop)
    {
        initProp(prop);
    }

    /**
     * 根据property创建 DataSources
     * @param prop
     */
    private void initProp(Properties prop)
    {
        try
        {
            this.dataSource = BasicDataSourceFactory.createDataSource(prop);
        }
        catch (Exception e)
        {
            throw new RuntimeException("创建连接池失败！请检查配置信息");
        }
    }

    public DataSource getDataSource()
    {
        return dataSource;
    }

    public Connection getConnection() throws SQLException
    {
        return con = dataSource.getConnection();
    }
}
