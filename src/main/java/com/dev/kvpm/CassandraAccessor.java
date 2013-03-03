package com.dev.kvpm;

import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import com.dev.kvpm.CassandraUtil;

public class CassandraAccessor {

	private static Logger logger = LoggerFactory
			.getLogger(CassandraAccessor.class);
	private Map<Node, Node> resourcePolicyMap;
	private String user;
	private CassandraUtil cassandraUtil;
	private ProvenanceDaoImpl provenanceDao;
	private Configuration config;
	private boolean collectProvenance;

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

	public void setProvenanceFlag(boolean flag) {
		this.collectProvenance = flag;
	}
	
	public synchronized Object get_kvpm_prov_in_Cassandra(String keyspace, String columnFamily,
			String rowKey, String columnKey, long timestamp) throws Exception {

		String resource = "/" + keyspace + "/" + columnFamily + "/(id=" + rowKey + ")/" + columnKey;
		logger.info("Accessing " + resource);

		byte[] val = (byte[]) cassandraUtil
				.get(columnFamily, rowKey, columnKey);

		if (collectProvenance) {
			collectProvenanceInformationInCassandra(keyspace, resource, user, "read", null);
		}

		return new String(val);
	}
	
	private void collectProvenanceInformationInCassandra(String keyspace, String resource, String user,
			String operation, String data) {
		try {
		String columnFamily = "Provenance";
		String columnKey = String.valueOf(System.currentTimeMillis());
		String value = "accessor:" + user + " operation:" + operation + " data:" + data;
		this.direct_put(keyspace, columnFamily, resource, columnKey, value, System.currentTimeMillis());
		} catch (Exception exp) {
			logger.error("Error while storing provenance information in the Provenance column family. " + exp.toString());
		}		
	}

	public synchronized Object get(String keyspace, String columnFamily,
			String rowKey, String columnKey, long timestamp) throws Exception {

		String resource = "/" + keyspace + "/" + columnFamily + "/(id=" + rowKey + ")/" + columnKey;
		logger.info("Accessing " + resource);

		byte[] val = (byte[]) cassandraUtil
				.get(columnFamily, rowKey, columnKey);

		if (collectProvenance) {
			collectProvenanceInformation(resource, user, "read", null);
		}

		return new String(val);
	}

	private void collectProvenanceInformation(String resource, String user,
			String operation, String data) {
		try {
			logger.info("About to save provenance information.");
			long start = System.currentTimeMillis();
			provenanceDao.insert(resource, user, operation, data);
			long end = System.currentTimeMillis();
			logger.info("Done saving provenance information. Time taken:"
					+ (end - start));
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
		if (obj != null) {
			return new String(obj);
		} else {
			return null;
		}
	}

	public Map<String, String> get_versions(String keyspace,
			String columnFamily, String rowKey, String columnKey, long timestamp)
			throws Exception {
		Map<String, String> vmap = cassandraUtil.get_super_col(columnFamily,
				rowKey, columnKey);
		return vmap;
	}
	
	public synchronized void direct_put(String keyspace, String columnFamily,
			String rowKey, String columnKey, Object value, long timestamp)
			throws Exception {

		cassandraUtil.add(keyspace, columnFamily, rowKey, columnKey, value,
				timestamp);
	}
	
	public synchronized void put_kvpm_prov_in_Cassandra(String keyspace, String columnFamily,
			String rowKey, String columnKey, Object value, long timestamp)
			throws Exception {

		String resource = "/" + keyspace + "/" + columnFamily + "/(id=" + rowKey + ")/" + columnKey;
		logger.info("Accessing " + resource);

		String currentValue = null;
		if (collectProvenance) {
			currentValue = direct_get(keyspace, columnFamily, rowKey,
					columnKey, timestamp);
		}

		cassandraUtil.add(keyspace, columnFamily, rowKey, columnKey, value,
				timestamp);

		if (collectProvenance) {
			this.collectProvenanceInformationInCassandra(keyspace, resource, user, "write", currentValue);
		}
	}

	public synchronized void put(String keyspace, String columnFamily,
			String rowKey, String columnKey, Object value, long timestamp)
			throws Exception {

		String resource = "/" + keyspace + "/" + columnFamily + "/(id=" + rowKey + ")/" + columnKey;
		logger.info("Accessing " + resource);

		String currentValue = null;
		if (collectProvenance) {
			currentValue = direct_get(keyspace, columnFamily, rowKey,
					columnKey, timestamp);
		}

		cassandraUtil.add(keyspace, columnFamily, rowKey, columnKey, value,
				timestamp);

		if (collectProvenance) {
			collectProvenanceInformation(resource, user, "write", currentValue);
		}
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