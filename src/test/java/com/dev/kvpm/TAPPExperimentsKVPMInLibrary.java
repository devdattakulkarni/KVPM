package com.dev.kvpm;

import java.util.Map;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TAPPExperimentsKVPMInLibrary {

	private CassandraAccessor accessor;
	private final Logger log = LoggerFactory
			.getLogger(TAPPExperimentsKVPMInLibrary.class);
	String user = "devdatta";
	String password = "devdatta";
	String keyspace = "PatientInfoSystem";
	String server = "localhost";
	int port = 9170;

	String configFilePath = "src/main/resources/config/config.properties";

	@Before
	public void setup() throws Exception {
		accessor = new CassandraAccessor(configFilePath, user, password,
				keyspace, server, port);
	}
	
	@Test
	public void tgetpOffDataInCassandra() throws Exception {
		String columnFamily = "Patient";
		String rowKey = "VVS";
		String columnKey = "medication";
		long timestamp = 0;
		int dataSize = 1;
		for (int i = 0; i < dataSize; i++) {
			accessor.setProvenanceFlag(false);
			long start = System.currentTimeMillis();
			String value = (String)accessor.get_kvpm_prov_in_Cassandra(keyspace, columnFamily, rowKey, columnKey, timestamp);
			long end = System.currentTimeMillis();
			long totalTime = end - start;
			log.info("Got value:" + value + " Time take:" + totalTime);
		}
	}
	
	@Test
	public void tgetpONDataInCassandra() throws Exception {
		String columnFamily = "Patient";
		String rowKey = "VVS";
		String columnKey = "medication";
		long timestamp = 0;
		int dataSize = 1;
		for (int i = 0; i < dataSize; i++) {
			accessor.setProvenanceFlag(true);
			long start = System.currentTimeMillis();
			String value = (String)accessor.get_kvpm_prov_in_Cassandra(keyspace, columnFamily, rowKey, columnKey, timestamp);
			long end = System.currentTimeMillis();
			long totalTime = end - start;
			log.info("Got value:" + value + " Time take:" + totalTime);
		}
	}
	
	@Test
	public void testGetWithProvenanceTurnedOff() throws Exception {
		String columnFamily = "Patient";
		String rowKey = "VVS";
		String columnKey = "medication";
		long timestamp = 0;
		int dataSize = 1;
		for (int i = 0; i < dataSize; i++) {
			accessor.setProvenanceFlag(false);
			long start = System.currentTimeMillis();
			String value = (String)accessor.get(keyspace, columnFamily, rowKey, columnKey, timestamp);
			long end = System.currentTimeMillis();
			long totalTime = end - start;
			log.info("Got value:" + value + " Time take:" + totalTime);
		}
	}
	
	
	
	@Test
	public void testGetWithProvenanceTurnedOn() throws Exception {
		String columnFamily = "Patient";
		String rowKey = "VVS";
		String columnKey = "medication";
		long timestamp = 0;
		int dataSize = 1;
		for (int i = 0; i < dataSize; i++) {
			accessor.setProvenanceFlag(true);
			long start = System.currentTimeMillis();
			String value = (String)accessor.get(keyspace, columnFamily, rowKey, columnKey, timestamp);
			long end = System.currentTimeMillis();
			long totalTime = end - start;
			log.info("Got value:" + value + " Time take:" + totalTime);
		}
	}
		
	@Test
	public void testPutWithProvenanceTurnedOn() throws Exception {
		String columnFamily = "Patient";
		String rowKey = "VVS";
		String columnKey = "medication";
		String value = "Ibuprofane";
		long timestamp = 0;
		int dataSize = 1;
		for (int i = 0; i < dataSize; i++) {
			accessor.setProvenanceFlag(true);
			long start = System.currentTimeMillis();
			accessor.put(keyspace, columnFamily, rowKey, columnKey, value, timestamp);
			long end = System.currentTimeMillis();
			long totalTime = end - start;
			log.info("Time take:" + totalTime);
		}
	}
	
	@Test
	public void testPutWithProvenanceTurnedOff() throws Exception {
		String columnFamily = "Patient";
		String rowKey = "VVS";
		String columnKey = "medication";
		String value = "Ibuprofane";
		long timestamp = 0;
		int dataSize = 1;
		for (int i = 0; i < dataSize; i++) {
			accessor.setProvenanceFlag(false);
			long start = System.currentTimeMillis();
			accessor.put(keyspace, columnFamily, rowKey, columnKey, value, timestamp);
			long end = System.currentTimeMillis();
			long totalTime = end - start;
			log.info("Time take:" + totalTime);
		}
	}
	
	@Test
	public void tputpOffDataInCassandra() throws Exception {
		String columnFamily = "Patient";
		String rowKey = "VVS";
		String columnKey = "medication";
		String value = "Ibuprofane";
		long timestamp = 0;
		int dataSize = 1;
		for (int i = 0; i < dataSize; i++) {
			accessor.setProvenanceFlag(false);
			long start = System.currentTimeMillis();
			accessor.put_kvpm_prov_in_Cassandra(keyspace, columnFamily, rowKey, columnKey, value, timestamp);
			long end = System.currentTimeMillis();
			long totalTime = end - start;
			log.info("Time take:" + totalTime);
		}
	}
	
	@Test
	public void tputpOnDataInCassandra() throws Exception {
		String columnFamily = "Patient";
		String rowKey = "VVS";
		String columnKey = "medication";
		String value = "Ibuprofane";
		long timestamp = 0;
		int dataSize = 1;
		for (int i = 0; i < dataSize; i++) {
			accessor.setProvenanceFlag(true);
			long start = System.currentTimeMillis();
			accessor.put_kvpm_prov_in_Cassandra(keyspace, columnFamily, rowKey, columnKey, value, timestamp);
			long end = System.currentTimeMillis();
			long totalTime = end - start;
			log.info("Time take:" + totalTime);
		}
	}
	
	@Test
	public void insertData() throws Exception {
		String columnFamily = "Provenance";
		String rowKey = "Kumble";
		String colKey = "";
		String value = "Advil";
		for(long i=0; i<10; i++) {
			colKey = String.valueOf(i);
			value = "Advil " + i;
			accessor.direct_put(keyspace, columnFamily, rowKey, colKey, value, i);
		}
	}
	
	@Test
	public void testQueryALL() throws Exception {
		String columnFamily = "Provenance";
		String rowKey = "Kumble";		
		accessor.query_all(columnFamily, rowKey);
	}
	
	@Test
	public void testQueryLAST() throws Exception {
		String columnFamily = "Provenance";
		String rowKey = "Kumble";		
		accessor.query_last(columnFamily, rowKey);
	}
	

	@Test
	public void insert_multiple_versions_for_a_column() throws Exception {
		String columnFamily = "Nurse";
		String rowKey = "JodhaAkbar";
		String supercolumn = "medication";
		long timestamp = 22;
		String value = "Aspro 22";

		long start = System.currentTimeMillis();
		int dataSize = 1;
		for (int i = 0; i < dataSize; i++) {
			String columnKey = Long.toString(timestamp);
			accessor.put_with_super_col(keyspace, columnFamily, rowKey,
					columnKey, supercolumn, value, timestamp);
		}
	}
	
	@Test
	public void get_multiple_versions_for_a_column() throws Exception {
		String columnFamily = "Nurse";
		String rowKey = "JodhaAkbar";
		String supercolumn = "medication";
		long timestamp = 21;
		
		Map<String,String> vmap = accessor.get_versions(keyspace, columnFamily, rowKey, supercolumn, timestamp);
		
		for(Entry<String,String> version : vmap.entrySet()) {
			System.out.println(version.getKey() + " " + version.getValue());
		}		
	}

	@Test
	public void insertKey() throws Exception {
		String columnFamily = "Patient";
		String rowKey = "JodhaAkbar";
		String columnKey = "medication";
		long timestamp = 23;
		String value = "Aspro 23";
		
		int dataSize = 1;
		for (int i = 0; i < dataSize; i++) {
			accessor.put(keyspace, columnFamily, rowKey, columnKey, value + " "
					+ i, timestamp);
		}
	}

	@Test
	public void testReadProvenance() throws Exception {
		String columnFamily = "Patient";
		String rowKey = "JodhaAkbar";
		String columnKey = "medication";
		long timestamp = 23;

		long start = System.currentTimeMillis();
		int dataSize = 1;
		for (int i = 0; i < dataSize; i++) {
			String result = accessor.direct_get(keyspace, columnFamily, rowKey,
					columnKey, timestamp);
			log.debug("Got value:" + result);
		}

		long end = System.currentTimeMillis();
		long totalTime = end - start;
		double avgInsertTimePerRecord = totalTime / dataSize;
		log.debug("Total read time:" + totalTime);
		log.debug("Per record read time:" + avgInsertTimePerRecord);
	}

	@Test
	public void provenanceQuery_ALL() throws Exception {

		String columnFamily = "Provenance";
		String rowKey = "/PatientInfoSystem/Patient(id=Jodha)/medication:enable_dataprov_write";

		// accessor.get(keyspace, columnFamily, rowKey, columnKey, timestamp,
		// runtimeParams);

	}
}