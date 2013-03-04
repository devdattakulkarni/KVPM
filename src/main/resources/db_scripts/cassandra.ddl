CREATE COLUMN FAMILY Provenance
WITH comparator = UTF8Type
AND key_validation_class=UTF8Type
;

assume Provenance VALIDATOR AS UTF8;

assume Provenance COMPARATOR AS UTF8; 

use PatientInfoSystem devdatta 'devdatta';

connect localhost/9170 devdatta 'devdatta';


CREATE COLUMN FAMILY blog_entry
WITH comparator = TimeUUIDType
AND key_validation_class=UTF8Type
AND default_validation_class = UTF8Type;
