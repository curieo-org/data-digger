package org.curieo.consumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
/**
 * This class encapsulates the database connection to PostgreSQL
 * server using JDBC.
 * @author M Doornenbal for Curieo Technologies BV
 */
public class PostgreSQLClient implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLClient.class);
    public static final String LOCALDB = "jdbc:postgresql://localhost:5432/postgres";
    private final Connection connection;
    private final String connectionString;
    
    public static enum CreateFlags {
    	OnExistFail,
    	OnExistSilentNoOp,
    	OnExistSilentOverride
    }
    
    public PostgreSQLClient(
    		String dbUrl, String user, String password) throws SQLException {
        // Connect method #2
        if (dbUrl == null) {
        	dbUrl = LOCALDB;
        }

        Properties parameters = new Properties();
        parameters.put("user", user);
        parameters.put("password", password);

        connection = DriverManager.getConnection(dbUrl, parameters);
        if (connection != null) {
            LOGGER.info("Connected to database {}", dbUrl);
        }
        connectionString = dbUrl;
    }

    /**
     * Create a database. 
     * @param statement
     * @return true if the first result is a ResultSet object; false if it is an update count or there are no results
     * @throws SQLException
     */
    public boolean executeSqlStatement(String statement) throws SQLException {
    	Statement stmt = connection.createStatement();
    	return stmt.execute(statement);
    }
    
    /**
     * Create a database. 
     * @param databaseName
     * @param flags
     * @return connection string for the database.
     * @throws SQLException
     */
    public String createDatabase(
    		String databaseName, 
    		CreateFlags flags) throws SQLException {
    	Statement stmt = null;
    	int i = connectionString.lastIndexOf('/');
    	String retval;
    	if (i == -1) {
    		retval = null;
    	}
    	else {
    		return connectionString.substring(0, i + 1) + databaseName;
    	}
    	
        try {
        	stmt = connection.createStatement();
            String testIfExists = String.format("SELECT * FROM pg_database WHERE datname='%s'", 
            		escapeSingleQuotes(databaseName));
            ResultSet resultSet = stmt.executeQuery(testIfExists);
            boolean exists = resultSet.next();
            resultSet.close();
            if (exists) {
            	// there is already a database with this name
            	switch (flags) {
				case OnExistFail:
	        		stmt.close();
					throw new RuntimeException(String.format("Database %s already exists", databaseName));
				case OnExistSilentNoOp:
					return retval;
				case OnExistSilentOverride:
					dropDatabase(databaseName);
					break;
				default:
					break;
            	}
            }

            String sql = String.format("CREATE DATABASE %s", databaseName);
            int result = stmt.executeUpdate(sql);
            LOGGER.info("Executed create database with success {}", result);
        }
        finally {
        	if (stmt != null) {
        		stmt.close();
        	}
        }
        return retval;
    }
    

    public void dropDatabase(String databaseName) throws SQLException {
        // create three connections to three different databases on localhost
        String sql = String.format("DROP DATABASE %s", databaseName);
        Statement stmt = connection.createStatement();
        int result = stmt.executeUpdate(sql);
        LOGGER.info("Dropped database with success {}", result);
        stmt.close();
    }
    
    public static String escapeSingleQuotes(String s) {
    	StringBuilder sb= new StringBuilder();
    	for (char c : s.toCharArray()) {
    		if (c == '\'') {
    			sb.append("''");
    		}
    		else {
    			sb.append(c);
    		}
    	}
    	return sb.toString();
    }

	@Override
	public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
        	connection.close();
        }
	}

	public Connection getConnection() {
		return connection;
	}
}