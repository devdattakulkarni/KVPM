package com.dev.kvpm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class ProvenanceDaoImpl implements ProvenanceDao {
	
	private static ComboPooledDataSource cpds;
    private static final Logger log = LoggerFactory
        .getLogger(ProvenanceDaoImpl.class);
    private Configuration config;

    public ProvenanceDaoImpl(Configuration config) {
        this.config = config;
    }

	public void insert(String resourceKey, String accessor, String operation, String data) throws Exception {
		log.info("Storing provenance data for " + resourceKey + " Data:"
	            + accessor + " " + operation);
	        init();

	        String query = "insert into provenance_data (resource, accessor, operation, data) VALUES (?, ?, ?, ?)";

	        PreparedStatement pstmt = null;
	        Connection conn = null;
	        try {
	            conn = cpds.getConnection();
	            pstmt = conn.prepareStatement(query);
	            pstmt.setString(1, resourceKey);
	            pstmt.setString(2, accessor);
	            pstmt.setString(3, operation);
	            pstmt.setString(4, data);
	            log.debug("Executing SQL query");
	            pstmt.executeUpdate();
	            log.debug("Completed SQL query");
	        } catch (SQLException sqlException) {
	            sqlException.printStackTrace();
	            log
	                .error(
	                    "Encountered exception while inserting provenance information for " + resourceKey,
	                    sqlException.toString());
	        } finally {
	            if (pstmt != null) {
	                pstmt.close();
	            }
	            if (conn != null) {
	                conn.close();
	            }
	        }
	}

	/* ------- Private method -------- */
    private void init() throws Exception {
        if (cpds == null) {
            String url = config.getString("jdbc.url");
            String username = config.getString("jdbc.username");
            String password = config.getString("jdbc.password");
            cpds = new ComboPooledDataSource();
            cpds.setDriverClass(config.getString("jdbc.driver"));
            cpds.setJdbcUrl(url);
            cpds.setUser(username);
            cpds.setPassword(password);
            cpds.setMaxStatements(20);
        }
    }
}