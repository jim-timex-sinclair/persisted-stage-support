--clean everything out

truncate table loan;
truncate table loan__hist;


--CREATE TEMP table
CREATE or replace TEMP TABLE temp__cte_loan__land
AS
WITH
CTE_loan__land AS
(
	SELECT TRIM(loan_number) AS loan_number, 
	loan_amount AS loan_amount,
	TRIM(loan_officer) AS loan_officer,
	create_timestamp AS create_timestamp, 
	update_timestamp AS update_timestamp,
	__pstage_load_dts AS __pstage_load_dts,
	coalesce(update_timestamp, create_timestamp) AS __pstage_source_change_timestamp,
	CURRENT_LOCALTIMESTAMP() AS __pstage_synced_timestamp,
	FALSE AS __pstage_deleted_indicator,
    md5(
    	IFNULL(CAST(create_timestamp AS VARCHAR(128)), '')
    	|| '-' || IFNULL(CAST(loan_amount AS VARCHAR(128)), '')
    	|| '-' || IFNULL(TRIM(loan_officer), '')
    	|| '-' || IFNULL(CAST(update_timestamp AS VARCHAR(128)), '')
    ) AS __pstage_hash_diff,
    ROW_NUMBER() OVER (PARTITION BY loan_number ORDER BY __pstage_source_change_timestamp DESC, __pstage_load_dts DESC) AS __pstage_load_order
    FROM test_persisted_stage.main.loan__land
)
SELECT * FROM CTE_loan__land;

SELECT DISTINCT __pstage_load_order AS __pstage_load_order
FROM temp__cte_loan__land
ORDER BY __pstage_load_order DESC;


--FIRST BATCH
--MERGE 1
MERGE INTO loan AS T
    USING(
        SELECT * FROM temp__cte_loan__land
        WHERE __pstage_load_order = 2
    ) AS S
ON S.loan_number = T.loan_number
    WHEN MATCHED
        AND S.__pstage_hash_diff <> T.__pstage_hash_diff
        THEN UPDATE SET 
        loan_amount = S.loan_amount,
        loan_officer = S.loan_officer,
        create_timestamp = S.create_timestamp,
        update_timestamp = S.update_timestamp,
        __pstage_synced_timestamp = CURRENT_LOCALTIMESTAMP(), --S.__pstage_synced_timestamp,
        __pstage_deleted_indicator = false,
        __pstage_hash_diff = S.__pstage_hash_diff
    WHEN NOT MATCHED
        THEN INSERT (
        loan_number, 
        loan_amount,
        loan_officer,
        create_timestamp,
        update_timestamp,
        __pstage_synced_timestamp, 
        __pstage_deleted_indicator,
        __pstage_hash_diff
        )
        VALUES (
        S.loan_number, 
        S.loan_amount,
        S.loan_officer,
        S.create_timestamp,
        S.update_timestamp,
        CURRENT_LOCALTIMESTAMP(), --S.__pstage_synced_timestamp, 
        S.__pstage_deleted_indicator,
        S.__pstage_hash_diff
        );
       
--INSERT 1   
INSERT INTO loan__hist by name
WITH --Get the data from CTEs
CTE_SEED AS --Get previous max load date from historical table
(
    SELECT '1900-01-01'::TIMESTAMP AS __pstage_synced_timestamp
    UNION
    SELECT MAX(__pstage_synced_timestamp) AS __pstage_synced_timestamp
    FROM loan__hist
),
CTE_MAX_LOADED AS
(
    SELECT MAX(__pstage_synced_timestamp) AS max__pstage_synced_timestamp
    FROM CTE_SEED
),
CTE_CURRENT AS
(
    SELECT
        loan_number, loan_amount, loan_officer, create_timestamp, update_timestamp, __pstage_deleted_indicator, __pstage_hash_diff,
		1 as __pstage_active_indicator,
        __pstage_synced_timestamp AS __pstage_start_timestamp
    FROM loan
    INNER JOIN CTE_MAX_LOADED
    ON loan.__pstage_synced_timestamp > CTE_MAX_LOADED.max__pstage_synced_timestamp -- record if __pstage_synced_timestamp is greater than previous max __pstage_synced_timestamp from historical table
)
SELECT
	loan_number, loan_amount, loan_officer, create_timestamp, update_timestamp, __pstage_deleted_indicator, __pstage_hash_diff,
	__pstage_active_indicator,
	__pstage_start_timestamp,
	'9999-12-31 23:59:59.999999'::TIMESTAMP AS __pstage_end_timestamp,
	CURRENT_LOCALTIMESTAMP() AS __pstage_synced_timestamp
FROM CTE_CURRENT;

--UPDATE 1   
UPDATE loan__hist
SET __pstage_end_timestamp = SUB_HIST_UP.__pstage_end_timestamp,
__pstage_synced_timestamp = SUB_HIST_UP.__pstage_synced_timestamp,
__pstage_active_indicator = SUB_HIST_UP.__pstage_active_indicator
FROM
(
    WITH
    CTE_UPDATE_NEEDED AS
    (
        SELECT loan_number, COUNT(*) AS record_count
        FROM loan__hist
        WHERE __pstage_active_indicator = true
        GROUP BY loan_number
        HAVING COUNT(*) > 1
    ),
    CTE_HIST_UP_1 AS
    (
        SELECT TARGET.loan_number, TARGET.__pstage_start_timestamp,
        LAST_VALUE(TARGET.__pstage_start_timestamp) OVER (PARTITION BY TARGET.loan_number ORDER BY TARGET.__pstage_start_timestamp ROWS BETWEEN 0 PRECEDING AND 1 FOLLOWING) AS __pstage_end_timestamp
        FROM loan__hist TARGET
        INNER JOIN CTE_UPDATE_NEEDED
        ON TARGET.loan_number = CTE_UPDATE_NEEDED.loan_number
        WHERE TARGET.__pstage_active_indicator = 1
    ),
    CTE_HIST_UP_2 AS
    (
        SELECT loan_number, __pstage_start_timestamp,
        CASE
            WHEN __pstage_start_timestamp = __pstage_end_timestamp THEN '9999-12-31 23:59:59.999999'::TIMESTAMP
            ELSE __pstage_end_timestamp
        END AS __pstage_end_timestamp,
        CASE
            WHEN __pstage_start_timestamp = __pstage_end_timestamp THEN 1
            ELSE 0
        END __pstage_active_indicator
        FROM CTE_HIST_UP_1
    )
    SELECT loan_number, __pstage_start_timestamp, __pstage_end_timestamp, CURRENT_LOCALTIMESTAMP() AS __pstage_synced_timestamp, __pstage_active_indicator
    FROM CTE_HIST_UP_2
    ORDER BY __pstage_start_timestamp
) SUB_HIST_UP
WHERE loan__hist.loan_number = SUB_HIST_UP.loan_number
AND loan__hist.__pstage_start_timestamp = SUB_HIST_UP.__pstage_start_timestamp;
 
select * from loan;
--MERGE
--MERGE

--SECOND BATCH
--MERGE 2
MERGE INTO loan AS T
    USING(
        SELECT * FROM temp__cte_loan__land
        WHERE __pstage_load_order = 1
    ) AS S
ON S.loan_number = T.loan_number
    WHEN MATCHED
        AND S.__pstage_hash_diff <> T.__pstage_hash_diff
        THEN UPDATE SET 
        loan_amount = S.loan_amount,
        loan_officer = S.loan_officer,
        create_timestamp = S.create_timestamp,
        update_timestamp = S.update_timestamp,
        __pstage_synced_timestamp = CURRENT_LOCALTIMESTAMP(), --S.__pstage_synced_timestamp,
        __pstage_deleted_indicator = false,
        __pstage_hash_diff = S.__pstage_hash_diff
    WHEN NOT MATCHED
        THEN INSERT (
        loan_number, 
        loan_amount,
        loan_officer,
        create_timestamp,
        update_timestamp,
        __pstage_synced_timestamp, 
        __pstage_deleted_indicator,
        __pstage_hash_diff
        )
        VALUES (
        S.loan_number, 
        S.loan_amount,
        S.loan_officer,
        S.create_timestamp,
        S.update_timestamp,
        CURRENT_LOCALTIMESTAMP(), --S.__pstage_synced_timestamp, 
        S.__pstage_deleted_indicator,
        S.__pstage_hash_diff
        );
       
--INSERT 2
INSERT INTO loan__hist by name
WITH --Get the data from CTEs
CTE_SEED AS --Get previous max load date from historical table
(
    SELECT '1900-01-01'::TIMESTAMP AS __pstage_synced_timestamp
    UNION
    SELECT MAX(__pstage_synced_timestamp) AS __pstage_synced_timestamp
    FROM loan__hist
),
CTE_MAX_LOADED AS
(
    SELECT MAX(__pstage_synced_timestamp) AS max__pstage_synced_timestamp
    FROM CTE_SEED
),
CTE_CURRENT AS
(
    SELECT
        loan_number, loan_amount, loan_officer, create_timestamp, update_timestamp, __pstage_deleted_indicator, __pstage_hash_diff,
		1 as __pstage_active_indicator,
        __pstage_synced_timestamp AS __pstage_start_timestamp
    FROM loan
    INNER JOIN CTE_MAX_LOADED
    ON loan.__pstage_synced_timestamp > CTE_MAX_LOADED.max__pstage_synced_timestamp -- record if __pstage_synced_timestamp is greater than previous max __pstage_synced_timestamp from historical table
)
SELECT
	loan_number, loan_amount, loan_officer, create_timestamp, update_timestamp, __pstage_deleted_indicator, __pstage_hash_diff,
	__pstage_active_indicator,
	__pstage_start_timestamp,
	'9999-12-31 23:59:59.999999'::TIMESTAMP AS __pstage_end_timestamp,
	CURRENT_LOCALTIMESTAMP() AS __pstage_synced_timestamp
FROM CTE_CURRENT;

--UPDATE 2    
UPDATE loan__hist
SET __pstage_end_timestamp = SUB_HIST_UP.__pstage_end_timestamp,
__pstage_synced_timestamp = SUB_HIST_UP.__pstage_synced_timestamp,
__pstage_active_indicator = SUB_HIST_UP.__pstage_active_indicator
FROM
(
    WITH
    CTE_UPDATE_NEEDED AS
    (
        SELECT loan_number, COUNT(*) AS record_count
        FROM loan__hist
        WHERE __pstage_active_indicator = true
        GROUP BY loan_number
        HAVING COUNT(*) > 1
    ),
    CTE_HIST_UP_1 AS
    (
        SELECT TARGET.loan_number, TARGET.__pstage_start_timestamp,
        LAST_VALUE(TARGET.__pstage_start_timestamp) OVER (PARTITION BY TARGET.loan_number ORDER BY TARGET.__pstage_start_timestamp ROWS BETWEEN 0 PRECEDING AND 1 FOLLOWING) AS __pstage_end_timestamp
        FROM loan__hist TARGET
        INNER JOIN CTE_UPDATE_NEEDED
        ON TARGET.loan_number = CTE_UPDATE_NEEDED.loan_number
        WHERE TARGET.__pstage_active_indicator = 1
    ),
    CTE_HIST_UP_2 AS
    (
        SELECT loan_number, __pstage_start_timestamp,
        CASE
            WHEN __pstage_start_timestamp = __pstage_end_timestamp THEN '9999-12-31 23:59:59.999999'::TIMESTAMP
            ELSE __pstage_end_timestamp
        END AS __pstage_end_timestamp,
        CASE
            WHEN __pstage_start_timestamp = __pstage_end_timestamp THEN 1
            ELSE 0
        END __pstage_active_indicator
        FROM CTE_HIST_UP_1
    )
    SELECT loan_number, __pstage_start_timestamp, __pstage_end_timestamp, CURRENT_LOCALTIMESTAMP() AS __pstage_synced_timestamp, __pstage_active_indicator
    FROM CTE_HIST_UP_2
    ORDER BY __pstage_start_timestamp
) SUB_HIST_UP
WHERE loan__hist.loan_number = SUB_HIST_UP.loan_number
AND loan__hist.__pstage_start_timestamp = SUB_HIST_UP.__pstage_start_timestamp;


SELECT * FROM temp__cte_loan__land;
--DELETE FROM LAND
DELETE FROM loan__land
USING temp__cte_loan__land
WHERE (loan__land.loan_number = temp__cte_loan__land.loan_number
AND loan__land.__pstage_load_dts = temp__cte_loan__land.__pstage_load_dts
);

DROP TABLE temp__cte_loan__land;

select * from 
test_persisted_stage.main.loan__hist;