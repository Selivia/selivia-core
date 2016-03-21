package com.weyem.utils.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 加载数据库连接工具类
 * 
 * @author Selivia
 * @version 1.0
 */
public class JdbcConnection
{
	private String driver = null;
    private String url = null;
    private String username = null;
    private String password = null;

    public JdbcConnection(){}
    
	public JdbcConnection(String driver, String url, String username,String password)
	{
		this.driver = driver;
		this.url = url;
		this.username = username;
		this.password = password;
		init();
	}

	/**
	 * 初始化 加载驱动
	 */
	public void init()
	{
		try
		{
			Class.forName(driver);
		}
		catch (ClassNotFoundException e)
		{
			throw new RuntimeException("加载驱动失败！！");
		}
	}
    /**
     * @Method: release
     * @Description: 释放资源，
     *     要释放的资源包括Connection数据库连接对象，负责执行SQL命令的Statement对象，存储查询结果的ResultSet对象
     *
     * @param con
     * @param stm
     * @param rs
     */ 
     public void release(Connection con,Statement stm,ResultSet rs)
     {
         if(rs!=null)
         {
             try
             {
                 //关闭存储查询结果的ResultSet对象
            	 rs.close();
             }
             catch (Exception e) 
             {
                 e.printStackTrace();
             }
         }
         
         if(stm!=null)
         {
             try
             {
                 //关闭负责执行SQL命令的Statement对象
                 stm.close();
             }
             catch (Exception e)
             {
                 e.printStackTrace();
             }
         }
         
         if(con!=null)
         {
             try
             {
                 //关闭Connection数据库连接对象
                 con.close();
             }
             catch (Exception e)
             {
                 e.printStackTrace();
             }
         }
     }
     
     /**
      * @Method: setConnection
      * @Description:设置连接属性
      * 
      * @param driver 驱动
      * @param url 数据库地址
      * @param username 用户名
      * @param password 密码
      */
     public void setConnection(String driver, String url, String username,String password)
     {
    	 this.driver = driver;
    	 this.url = url;
    	 this.username = username;
    	 this.password = password;
     }
	/**
     * @Method: getConnection
     * @Description: 获取数据库连接对象
     *
     * @return Connection数据库连接对象
     * @throws SQLException
     */ 
    public Connection getConnection() throws SQLException
    {
        return DriverManager.getConnection(url, username,password);
    }
}
