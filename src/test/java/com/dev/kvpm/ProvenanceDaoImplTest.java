package com.dev.kvpm;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProvenanceDaoImplTest {

	ProvenanceDao provenanceDao;
	private static final Logger log = LoggerFactory
			.getLogger(ProvenanceDaoImplTest.class);

	@Before
	public void setup() throws ConfigurationException {
		String configFile = "src/main/resources/config/config.properties";
		Configuration config = new PropertiesConfiguration(configFile);
		provenanceDao = new ProvenanceDaoImpl(config);
	}

	@Test
	public void testInsert() throws Exception {
		String resourceKey = "testRowKey";
		String accessor = "testAccessor";
		String operation = "read";
		provenanceDao.insert(resourceKey, accessor, operation);
		log.info("Done saving provenance information.");
	}
}