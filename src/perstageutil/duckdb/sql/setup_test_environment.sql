-- test_data.main.loan__land definition

__pstage_

CREATE or replace TABLE loan__land(loan_number VARCHAR,
loan_amount DECIMAL(14, 2),
loan_officer VARCHAR,
create_timestamp TIMESTAMP,
update_timestamp TIMESTAMP,
__pstage_load_dts TIMESTAMP);


CREATE or replace TABLE loan(loan_number VARCHAR not null,
loan_amount DECIMAL(14,2),
loan_officer VARCHAR,
create_timestamp TIMESTAMP,
update_timestamp TIMESTAMP,
__pstage_synced_timestamp TIMESTAMP not null,
__pstage_deleted_indicator boolean not null,
__pstage_hash_diff varchar(32),
PRIMARY KEY(loan_number));


CREATE or replace TABLE loan__hist(loan_number VARCHAR not null,
loan_amount DECIMAL(14,2),
loan_officer VARCHAR,
create_timestamp TIMESTAMP,
update_timestamp TIMESTAMP,
__pstage_start_timestamp TIMESTAMP not null,
__pstage_end_timestamp TIMESTAMP,
__pstage_active_indicator boolean,
__pstage_synced_timestamp TIMESTAMP not null,
__pstage_deleted_indicator boolean not null,
__pstage_hash_diff varchar(32),
PRIMARY KEY(loan_number, __pstage_start_timestamp));


CREATE OR REPLACE TABLE loan__stage(loan_number VARCHAR,
loan_amount DECIMAL(14,2),
loan_officer VARCHAR,
create_timestamp TIMESTAMP,
update_timestamp TIMESTAMP);

CREATE or replace TABLE loan__cks(loan_number VARCHAR not null,
__pstage_synced_timestamp TIMESTAMP not null,
PRIMARY KEY(loan_number));

INSERT INTO test_persisted_stage.main.loan__stage
(loan_number, loan_amount, loan_officer, create_timestamp, update_timestamp)
VALUES('', ?, '', '', '');

truncate table main.loan__stage;

INSERT INTO main.loan__stage
(loan_number, loan_amount, loan_officer, create_timestamp, update_timestamp)
VALUES('1', 100.00, 'john smith', '1992-09-20 11:30:00.123456789'::TIMESTAMP, NULL),
('2', 110.00, NULL, '1992-09-20 11:31:00.123456789'::TIMESTAMP, '1992-09-23 11:30:00.000000000'::TIMESTAMP),
('3', 130.00, 'bob stober', '1992-09-20 11:32:00.123456789'::TIMESTAMP, '1992-09-24 11:30:00.000000000'::TIMESTAMP);

--ADD dupe.
INSERT INTO main.loan__stage
(loan_number, loan_amount, loan_officer, create_timestamp, update_timestamp)
VALUES('3', 130.00, 'bob stober', '1992-09-20 11:32:00.123456789'::TIMESTAMP, '1992-09-25 11:30:00.000000000'::TIMESTAMP);

--test teardown and setup
TRUNCATE TABLE main.loan__land;
TRUNCATE TABLE main.loan;
TRUNCATE TABLE main.loan__hist;
INSERT INTO main.loan__land
(loan_number, loan_amount, loan_officer, create_timestamp, update_timestamp, __pstage_load_dts)
SELECT loan_number, loan_amount, loan_officer, create_timestamp, update_timestamp, current_localtimestamp()
FROM test_persisted_stage.main.loan__stage;

select * from loan__land;
SELECT * FROM loan__stage;

select * from loan;
select * from loan__hist;

"__pstage_load_dts"
"__pstage_synced_timestamp"
"__pstage_deleted_indicator"
"__pstage_hash_diff"

"__pstage_start_timestamp"
"__pstage_end_timestamp"
"__pstage_active_indicator"

SELECT TIMESTAMP '1992-09-20 11:30:00.123456789';
SELECT '1992-09-20 11:30:00.123456789'::TIMESTAMP;




