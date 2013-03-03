package com.dev.kvpm;

import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

public class CassandraAccessor {

	private static Logger logger = LoggerFactory
			.getLogger(CassandraAccessor.class);
	private Map<Node, Node> resourcePolicyMap;
	private String user;
	private CassandraUtil cassandraUtil;
	private ProvenanceDao provenanceDao;
	private Configuration config;

	public CassandraAccessor(String configFilePath, String user,
			String password, String keyspace, String server, int port)
			throws Exception {
		this.user = user;
		this.cassandraUtil = new CassandraUtil(user, password, keyspace);
		this.cassandraUtil.connect(server, port);
		this.config = new PropertiesConfiguration(configFilePath);
		provenanceDao = new ProvenanceDaoImpl(config);
	}

	public Map<Node, Node> getResourcePolicyMap() {
		return resourcePolicyMap;
	}

	public CassandraUtil getCassandraUtil() {
		return cassandraUtil;
	}

	public synchronized Object get(String keyspace, String columnFamily,
			String rowKey, String columnKey, long timestamp) throws Exception {

		String resource = "/" + keyspace + "/" + columnFamily + "/" + columnKey;
		logger.info("Accessing " + resource);

		byte[] val = (byte[]) cassandraUtil
				.get(columnFamily, rowKey, columnKey);

		collectReadProvenanceInformation(resource, user);

		return new String(val);
	}

	private void collectReadProvenanceInformation(String resource, String user) {
		try {
			logger.info("About to save provenance information.");
			provenanceDao.insert(resource, user, "read");
			logger.info("Done saving provenance information.");
		} catch (Exception exp) {
			logger.error(
					"Exception while storing read provenance information.",
					exp.toString());
		}
	}

	public String getRow(String columnFamily, String rowKey) throws Exception {
		String retVal = cassandraUtil.getRow(columnFamily, rowKey);
		return retVal;
	}

	public String direct_get(String keyspace, String columnFamily,
			String rowKey, String columnKey, long timestamp) throws Exception {
		byte[] obj = (byte[]) cassandraUtil
				.get(columnFamily, rowKey, columnKey);
		return new String(obj);
	}

	public Map<String, String> get_versions(String keyspace,
			String columnFamily, String rowKey, String columnKey, long timestamp)
			throws Exception {
		Map<String, String> vmap = cassandraUtil.get_super_col(columnFamily,
				rowKey, columnKey);
		return vmap;
	}

	public synchronized void put(String keyspace, String columnFamily,
			String rowKey, String columnKey, Object value, long timestamp)
			throws Exception {
		cassandraUtil.add(keyspace, columnFamily, rowKey, columnKey, value,
				timestamp);
	}

	public synchronized void put_with_super_col(String keyspace,
			String columnFamily, String rowKey, String columnKey,
			String supercolumn, Object value, long timestamp) throws Exception {
		cassandraUtil.add_with_super_col(keyspace, columnFamily, rowKey,
				columnKey, supercolumn, value, timestamp);
	}

	public void delete(String columnFamily, String rowKey, String column)
			throws Exception {
		cassandraUtil.delete(columnFamily, rowKey, column);
	}

	public synchronized void dropColumnFamily(String columnFamily)
			throws Exception {
		cassandraUtil.dropColumnFamily(columnFamily);
	}

	public synchronized void addColumnFamily(String keyspace,
			String columnFamily) throws Exception {
		cassandraUtil.addColumnFamily(keyspace, columnFamily);
	}

	public String getUser() {
		return user;
	}

	public static void main(String args[]) throws Exception {
		logger.info("Cassandra Client");

		String user = "devdatta";
		String password = "devdatta";
		String keyspace = "PatientInfoSystem";
		String server = "localhost";
		int port = 9160;

		String policyFilePath = "src/main/resources/Policy.xml";
		CassandraAccessor accessor = new CassandraAccessor(policyFilePath,
				user, password, keyspace, server, port);

		String columnFamily = "Patient";
		String rowKey = "john";
		String columnKey = "name";

		String colValue = (String) accessor.get(keyspace, columnFamily, rowKey,
				columnKey, System.currentTimeMillis());
		logger.info("Column Value: {}", colValue);
	}
}