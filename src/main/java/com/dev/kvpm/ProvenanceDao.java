package com.dev.kvpm;

public interface ProvenanceDao {
	
	void insert(String resourceKey, String accessor, String operation, String data) throws Exception ;

}
