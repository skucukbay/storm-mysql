package storm.trident.state.mysql.datasource;


import java.beans.PropertyVetoException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import storm.trident.state.mysql.MysqlStateConfig;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

public class TridentMysqlStateDataSource {

	//written as volatile for double check synchronization
    private static volatile TridentMysqlStateDataSource  dataSource;
    private BoneCP connectionPool;
	protected static Properties properties = new Properties();

	/**
	 * Load properties file for bonecp configuration lookup
	 */
	
	static {
		InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("bonecp.properties");
		try {
			if (inputStream == null) {
				throw new FileNotFoundException("property file '" + "bonecp.properties" + "' not found in the classpath");
			}
			
			properties.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
    private TridentMysqlStateDataSource(MysqlStateConfig config) throws IOException, SQLException, PropertyVetoException {
        try {
            // load the database driver (make sure this is in your classpath!)
            Class.forName("com.mysql.jdbc.Driver");
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage(),e);
        }

        try {
            // setup the connection pool using BoneCP Configuration
           BoneCPConfig boneCpConfig = new BoneCPConfig();
           boneCpConfig.setPoolName("Trident-MySQL-State Connection Pool");
           boneCpConfig.setJdbcUrl(config.getPureUrl());
           boneCpConfig.setUsername(config.getUsername());
           boneCpConfig.setPassword(config.getPassword());
           boneCpConfig.setMinConnectionsPerPartition(Integer.parseInt(properties.getProperty("minConnectionsPerPartition")));
           boneCpConfig.setMaxConnectionsPerPartition(Integer.parseInt(properties.getProperty("maxConnectionsPerPartition")));
           boneCpConfig.setPartitionCount(Integer.parseInt(properties.getProperty("partitionCount")));
   
           // setup the connection pool
           connectionPool = new BoneCP(boneCpConfig);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

    }

    public static TridentMysqlStateDataSource getInstance(MysqlStateConfig config) throws IOException, SQLException, PropertyVetoException {
		if (dataSource == null) {
			synchronized (TridentMysqlStateDataSource.class) {
				if (dataSource == null) {
					dataSource = new TridentMysqlStateDataSource(config);
				}
			}
		}
		return dataSource;
    }

    public Connection getConnection() throws SQLException {
        return dataSource.connectionPool.getConnection();
    }


}