package com.weyem.utils.jdbc;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.*;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Jdbc 工具类  
 * 内部调用Apache Dbutils框架的 QueryRunner对象进行CRUD
 * 
 * @author Selivia
 * @version 1.1
 */
public class JdbcUtils
{
    private DataSource dataSource;
    private JdbcDataSources jdbcDataSources;

	/**
	 * 构造初始化 
	 * @param dataSource
	 */
	public JdbcUtils(DataSource dataSource)
	{
		this.dataSource = dataSource;
	}

	/**
	 * 构造初始化 根据配置文件手动创建
	 * @param propPath
	 */
	public JdbcUtils(String propPath)
	{
        jdbcDataSources = new JdbcDataSources(propPath);
        this.dataSource = jdbcDataSources.getDataSource();
    }

	/**
	 * 构造初始化 根据配置文件手动创建
	 * @param prop
	 */
	public JdbcUtils(Properties prop)
	{
        jdbcDataSources = new JdbcDataSources(prop);
        this.dataSource = jdbcDataSources.getDataSource();
    }

    /**
     * 获得 DataSource
     * @return
     */
    public DataSource getDataSource()
    {
        return dataSource;
    }

    /**
     * 获得 Connection
     * @return
     */
    public Connection getConnection() throws SQLException
    {
        return dataSource.getConnection();
    }
    /**
	 *  
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public int executeUpdate(String sql) throws SQLException
	{
        QueryRunner qr = new QueryRunner(dataSource);
		return qr.update(sql);
	}

	/**
	 *
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public int executeUpdate(String sql,Object... params) throws SQLException
	{
        QueryRunner qr = new QueryRunner();
		return qr.update(sql,params);
	}

    /**
    * @Method: batch
    * @Description:sql语句 批处理
    *
    * @throws SQLException
    */
    public void batch(String sql,Object[][] params) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
        qr.batch(sql, params);
    }

    /**
     * 查询  返回Bean对象
     * @param sql
     * @param clazz Bean对象字节码
     * @return
     * @throws SQLException
     */
    public <T> T queryForBean(Class<T> clazz,String sql) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new BeanHandler<T>(clazz));
    }

    /**
     * 查询  返回Bean对象
     * @param sql
     * @param params
     * @param clazz Bean对象字节码
     * @return
     * @throws SQLException
     */
    public <T> T queryForBean(Class<T> clazz,String sql,Object... params) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new BeanHandler<T>(clazz),params);
    }

    /**
     * 返回查询全部结果 返回List集合的Bean对象
     * @param clazz
     * @param sql
     * @return
     * @throws SQLException
     */
    public <T> List<T> queryForBeanList(Class<T> clazz,String sql) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new BeanListHandler<T>(clazz));
    }

    /**
     * 返回查询全部结果 返回List集合的Bean对象
     * @param clazz
     * @param sql
     * @param params
     * @return
     * @throws SQLException
     */
    public <T> List<T> queryForBeanList(Class<T> clazz,String sql,Object... params) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new BeanListHandler<T>(clazz),params);
    }

    /**
     * 查询  返回List<Map<String, Object>>
     * @param sql
     * @return
     * @throws SQLException
     */
    public List<Map<String, Object>> queryForMapList(String sql) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new MapListHandler());
    }

    /**
     * 查询  返回List<Map<String, Object>>
     * @param sql
     * @param params
     * @return
     * @throws SQLException
     */
    public List<Map<String, Object>> queryForMapList(String sql,Object... params) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new MapListHandler(),params);
    }

    /**
     * 返回查询结果第一行记录。。返回Object数组
     * @param sql
     * @return
     * @throws SQLException
     */
    public Object[] queryForArray(String sql) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new ArrayHandler());
    }

    /**
     * 返回查询结果第一行记录。。返回Object数组
     * @param sql
     * @param params
     * @return
     * @throws SQLException
     */
    public Object[] queryForArray(String sql,Object... params) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new ArrayHandler(),params);
    }

    /**
     * 返回 所有的查询结果。。返回List<Object[]> 集合
     * @param sql
     * @return
     * @throws SQLException
     */
    public List<Object[]>  queryForArrayList(String sql) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new ArrayListHandler());
    }

    /**
     * 返回 所有的查询结果。。返回List<Object[]> 集合
     * @param sql
     * @param params
     * @return
     * @throws SQLException
     */
    public List<Object[]>  queryForArrayList(String sql,Object... params) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new ArrayListHandler(),params);
    }

    /**
     * 返回查询结果第一行记录。。返回Map<String, Object>
     * @param sql
     * @return
     * @throws SQLException
     */
    public Map<String, Object> queryForMap(String sql) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new MapHandler());
    }

    /**
     * 返回查询结果第一行记录。。返回Map<String, Object>
     * @param sql
     * @param params
     * @return
     * @throws SQLException
     */
    public Map<String, Object> queryForMap(String sql,Object... params) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new MapHandler(),params);
    }

    /**
     * 返回全部查询结果  。。。返回 Map<String, T> 以 主键=T 形式展现
     * @param clazz
     * @param sql
     * @return
     * @throws SQLException
     */
    public <T> Map<String, T>  queryForBeanMap(Class<T> clazz,String sql) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new BeanMapHandler<String, T>(clazz));
    }

    /**
     * 返回全部查询结果  。。。返回 Map<String, T> 以 主键=T 形式展现
     * @param clazz
     * @param sql
     * @param params
     * @return
     * @throws SQLException
     */
    public <T> Map<String, T>  queryForBeanMap(Class<T> clazz,String sql,Object... params) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new BeanMapHandler<String, T>(clazz),params);
    }

    /**
     * 返回查询结果集。。 某列的结果List
     * @param sql
     * @param columnIndex 列索引从1开始
     * @return
     * @throws SQLException
     */
    public List<String> queryForColumnList(String sql,int columnIndex) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new ColumnListHandler<String>(columnIndex));
    }

    /**
     * 返回查询结果集。。 某列的结果List
     * @param sql
     * @param columnIndex 列索引从1开始
     * @param params
     * @return
     * @throws SQLException
     */
    public List<String> queryForColumnList(String sql,int columnIndex,Object... params) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new ColumnListHandler<String>(columnIndex),params);
    }

    /**
     * 返回查询结果集。。 某列的结果List
     * @param sql
     * @param columnName 列名称
     * @return
     * @throws SQLException
     */
    public List<String> queryForColumnList(String sql,String columnName) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new ColumnListHandler<String>(columnName));
    }

    /**
     * 返回查询结果集。。 某列的结果List
     * @param sql
     * @param columnName 列名称
     * @param params
     * @return
     * @throws SQLException
     */
    public List<String> queryForColumnList(String sql,String columnName,Object... params) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new ColumnListHandler<String>(columnName),params);
    }

    /**
     * 获取结果集中第一行数据指定列的值
     * @param sql
     * @param columnIndex 列索引从1开始
     * @return
     * @throws SQLException
     */
    public Object queryForScalar(String sql,int columnIndex) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new ScalarHandler<Object>(columnIndex));
    }

    /**
     * 获取结果集中第一行数据指定列的值
     * @param sql
     * @param columnIndex 列索引从1开始
     * @param params
     * @return
     * @throws SQLException
     */
    public Object queryForScalar(String sql,int columnIndex,Object... params) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new ScalarHandler<Object>(columnIndex),params);
    }

    /**
     * 获取结果集中第一行数据指定列的值
     * @param sql
     * @param columnName 列名称
     * @return
     * @throws SQLException
     */
    public Object queryForScalar(String sql,String columnName) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new ScalarHandler<Object>(columnName));
    }

    /**
     * 获取结果集中第一行数据指定列的值
     * @param sql
     * @param columnName 列名称
     * @param params
     * @return
     * @throws SQLException
     */
    public Object queryForScalar(String sql,String columnName,Object... params) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new ScalarHandler<Object>(columnName),params);
    }

    /**
     * 返回全部查询结果  。。。返回 Map<columnName,Map<String, Object>> 以 主键=T 形式展现
     * @param sql
     * @param columnIndex 列索引从1开始
     * @return
     * @throws SQLException
     */
    public Map<String, Map<String, Object>> queryForKeyMap(String sql,int columnIndex) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new KeyedHandler<String>(columnIndex));
    }

    /**
     * 返回全部查询结果  。。。返回 Map<columnName,Map<String, Object>> 以 主键=T 形式展现
     * @param sql
     * @param columnIndex 列索引从1开始
     * @param params
     * @return
     * @throws SQLException
     */
    public Map<String, Map<String, Object>> queryForKeyMap(String sql,int columnIndex,Object... params) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new KeyedHandler<String>(columnIndex),params);
    }

    /**
     * 返回全部查询结果  。。。返回 Map<columnName,Map<String, Object>> 以 主键=T 形式展现
     * @param sql
     * @param columnName 列名
     * @return
     * @throws SQLException
     */
    public Map<String, Map<String, Object>> queryForKeyMap(String sql,String columnName) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new KeyedHandler<String>(columnName));
    }

    /**
     * 返回全部查询结果  。。。返回 Map<columnName,Map<String, Object>> 以 主键=T 形式展现
     * @param sql
     * @param columnName 列名
     * @param params
     * @return
     * @throws SQLException
     */
    public Map<String, Map<String, Object>> queryForKeyMap(String sql,String columnName,Object... params) throws SQLException
    {
        QueryRunner qr = new QueryRunner(dataSource);
    	return qr.query(sql, new KeyedHandler<String>(columnName),params);
    }
  
}
