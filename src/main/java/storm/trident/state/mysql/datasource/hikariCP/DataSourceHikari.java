package storm.trident.state.mysql.datasource.hikariCP;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import storm.trident.state.mysql.MysqlStateConfig;

import com.zaxxer.hikari.HikariDataSource;

public class DataSourceHikari {
	
	
	public static volatile DataSourceHikari datasourceInstance;
	private HikariDataSource ds;
	
	protected static Properties properties = new Properties();

	/**
	 * Load properties file for hikari configuration lookup
	 */
	
	static {
		InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("hikari.properties");
		try {
			if (inputStream == null) {
				throw new FileNotFoundException("property file '" + "hikari.properties" + "' not found in the classpath");
			}
			
			properties.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	
	private DataSourceHikari(MysqlStateConfig mysqlConfig){
		ds = new HikariDataSource();
		ds.setJdbcUrl(mysqlConfig.getPureUrl());
		ds.setUsername(mysqlConfig.getUsername());
		ds.setPassword(mysqlConfig.getPassword());
		//ds.setDataSourceClassName(className);
		ds.setDriverClassName(properties.getProperty("driver.class.name"));
		ds.setMaximumPoolSize(Integer.valueOf(properties.getProperty("maxPoolSize")));
		ds.setMaxLifetime(Long.valueOf(properties.getProperty("maxLifeTime")));
		ds.setMinimumIdle(Integer.valueOf(properties.getProperty("minIdleConnection")));
		ds.setPoolName("hikari");

	}
	
	public static DataSourceHikari getInstance(MysqlStateConfig mysqlConfig){
		if(datasourceInstance == null){
			synchronized (DataSourceHikari.class) {
				if(datasourceInstance == null){
					datasourceInstance = new DataSourceHikari(mysqlConfig);
				}
			}
		}
		return datasourceInstance;
	}
	
	public Connection getConnection() throws SQLException{
		return this.ds.getConnection();
	}
	

}
