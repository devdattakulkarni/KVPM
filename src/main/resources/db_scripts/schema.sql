create database provenance;

create table provenance_data (resource VARCHAR(100) CHARACTER SET utf8, accessor VARCHAR(200) CHARACTER SET utf8, access_timestamp TIMESTAMP, operation VARCHAR(10) CHARACTER SET UTF8, data VARCHAR(1024) CHARACTER SET UTF8);

create index resource_idx USING HASH on provenance_data (resource);

create index accessor_idx USING HASH on provenance_data (accessor);

create index timestamp_idx USING HASH on provenance_data (access_timestamp);

create index operation_idx USING HASH on provenance_data (operation);



