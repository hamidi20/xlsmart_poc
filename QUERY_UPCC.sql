insert into POC_XL_1.L1.UPCC
select
	  CASE 
		WHEN col_1 IS NULL OR TRIM(col_1) = '' THEN DATE '1900-01-01'
		ELSE TO_DATE(SUBSTR(col_1, 1, 10), 'YYYY-MM-DD')
	  END	as date_id
	
	, CASE 
		WHEN col_1 IS NULL OR TRIM(col_1) = '' THEN TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH12:MI:SS AM')
		ELSE TO_CHAR(TO_TIMESTAMP(col_1), 'YYYY-MM-DD HH12:MI:SS AM') --TO_TIMESTAMP(col_1, 'YYYY-MM-DD HH24:MI:SS')
	  END	as start_time
	
	, '-1' 		as event_type_id
	, 'CONTEN'	as service_filter
	
	, NULLIF(TRIM(col_2), '')	as subscriber_no
	, NULLIF(TRIM(col_4), '')	as msisdn
	
	, CASE 
		WHEN col_3 = '0' THEN 'PRE'
		WHEN col_3 = '1' THEN 'POST'
		ELSE 'UNK'
	  END	as payment_category
	
	, NULLIF(TRIM(col_11), '')	as apn
	
	, CASE 
		WHEN col_3 = '0' THEN 'PRE'
		WHEN col_3 = '1' THEN 'POST'
		ELSE 'UNK'
	  END	as bts_type
	
	-- , CASE 
		-- WHEN TRIM(USED_ECGI_FINAL) IS NULL OR TRIM(USED_ECGI_FINAL) = '' THEN NULL
		-- ELSE USED_ECGI_FINAL
	  -- END	as ecgi
	
	, '' as ecgi
	
	, CASE 
		WHEN COALESCE(NULLIF(TRIM(col_5), ''), NULLIF(TRIM(col_22), '')) IS NOT NULL and COALESCE(NULLIF(TRIM(col_5), ''), NULLIF(TRIM(col_22), '')) <> '' then SUBSTRING(COALESCE(NULLIF(TRIM(NULLIF(TRIM(col_6), '')), ''), COALESCE(NULLIF(TRIM(col_5), ''), NULLIF(TRIM(col_22), ''))),1,3) + '.' + SUBSTRING(COALESCE(NULLIF(TRIM(NULLIF(TRIM(col_6), '')), ''), COALESCE(NULLIF(TRIM(col_5), ''), NULLIF(TRIM(col_22), ''))),4,2) + ',' + TO_VARCHAR(SUBSTRING(COALESCE(NULLIF(TRIM(SUBSTRING(COALESCE(NULLIF(TRIM(col_5), ''), NULLIF(TRIM(col_22), '')), 11, 5)), ''), NULLIF(TRIM(col_22), '')), 6, 5)) + '.' + TO_VARCHAR(SUBSTRING(COALESCE(NULLIF(TRIM(col_5), ''), NULLIF(TRIM(col_22), '')), 11, 5))
		ELSE NULL
	  END 													as cgi
	
	, NULLIF(TRIM(col_7), '')								as home_poc
	, NULL 													as physical_poc
	
	, REGEXP_REPLACE(NULLIF(TRIM(col_8), ''), '[^0-9]', '')	as price_plan_id
	, 0														as rated_amount
	
	, CASE 
		WHEN b.ro_id IS NOT NULL THEN b.ro_id
		WHEN LEFT(TRIM(col_9), 1) BETWEEN '0' AND '9' THEN 
			 CASE 
				WHEN CHARINDEX(TRIM(col_9), '_') > 0 THEN SUBSTR(TRIM(col_9), 1, CHARINDEX(TRIM(col_9), '_') - 1)
				ELSE TRIM(col_9)
			 END
		ELSE '440271'
	  END 													as rated_offer_id
	
	, NULLIF(TRIM(col_10), '')								as roam_zone
	, NULLIF(TRIM(col_12), '')								as origin_country_name
	, NULLIF(TRIM(col_13), '')								as session_id
	
	, CASE 
		WHEN col_14 IS NULL OR TRIM(col_14) = '' OR NOT REGEXP_LIKE(col_14, '^[0-9]+$') THEN -1
		ELSE CAST(col_14 AS DECIMAL(38, 2))
	  END													as trigger_type
	
	, NULLIF(TRIM(col_15), '')								as quota_name
	, NVL(NULLIF(TRIM(col_16), ''), '0')					as quota_unit
	
	-- , CASE 
		-- WHEN NVL(NULLIF(TRIM(col_16), ''), '0') = '2' THEN QUOTA_USAGE * 10 
		-- ELSE 0
	  -- end 				as duration
	  
	, 0 as duration
		
	-- , CASE 
		-- WHEN NVL(NULLIF(TRIM(col_16), ''), '0') = '0' THEN QUOTA_USAGE * 1024 
		-- ELSE 0 
	  -- END				as quota_usage
	  
	, 0 as quota_usage
	
	, CASE NVL(NULLIF(TRIM(col_16), ''), '0')
		WHEN '0' THEN COALESCE(TRY_TO_DECIMAL(NULLIF(TRIM(col_18), '')), 0) * 1024
		WHEN '2' THEN COALESCE(TRY_TO_DECIMAL(NULLIF(TRIM(col_18), '')), 0) * 10
		ELSE COALESCE(TRY_TO_DECIMAL(NULLIF(TRIM(col_18), '')), 0)
	  END 				as quota_consumption	
	
	, CASE NVL(NULLIF(TRIM(col_16), ''), '0')
		WHEN '0' THEN COALESCE(TRY_TO_DECIMAL(NULLIF(TRIM(col_19), '')), 0) * 1024
		WHEN '2' THEN COALESCE(TRY_TO_DECIMAL(NULLIF(TRIM(col_19), '')), 0) * 10
		ELSE COALESCE(TRY_TO_DECIMAL(NULLIF(TRIM(col_19), '')), 0)
	  END 				as quota_balance
	
	, CASE NVL(NULLIF(TRIM(col_16), ''), '0')
		WHEN '0' THEN COALESCE(TRY_TO_DECIMAL(NULLIF(TRIM(col_20), '')), 0) * 1024
		WHEN '2' THEN COALESCE(TRY_TO_DECIMAL(NULLIF(TRIM(col_20), '')), 0) * 10
		ELSE COALESCE(TRY_TO_DECIMAL(NULLIF(TRIM(col_20), '')), 0)
	  END 				as quota_value
	
	, NULL				as sid
	, TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH12:MI:SS AM')	as emp_load_date
	
	, CASE NVL(NULLIF(TRIM(col_16), ''), '0')
		WHEN '0' THEN COALESCE(TRY_TO_DECIMAL(NULLIF(TRIM(col_23), '')), 0) * 1024
		WHEN '2' THEN COALESCE(TRY_TO_DECIMAL(NULLIF(TRIM(col_23), '')), 0) * 10
		ELSE COALESCE(TRY_TO_DECIMAL(NULLIF(TRIM(col_23), '')), 0)
	  END 				as actual_quota_usage
	
	, SPLIT_PART(a.METADATA$FILENAME, '/', 2) as file_name
	
	, CASE 
		WHEN col_24 IS NULL OR TRIM(col_24) = '' OR NOT REGEXP_LIKE(col_24, '^[0-9]+$') THEN 0
		ELSE CAST(col_24 AS DECIMAL(38, 2))
	  END	as quota_flag
	
	, NULLIF(TRIM(col_25), '')	as group_sub_id
	
	-- , CASE 
		-- WHEN TRIM(old_quota_consumption) IS NULL OR TRIM(old_quota_consumption) = '' OR NOT REGEXP_LIKE(old_quota_consumption, '^[0-9]+$') THEN 0
		-- ELSE CAST(old_quota_consumption AS DECIMAL(38, 2))
	  -- END	as old_quota_consumption
	
	, 0 as old_quota_consumption
	
	-- , CASE 
		-- WHEN TRIM(old_quota_balance) IS NULL OR TRIM(old_quota_balance) = '' OR NOT REGEXP_LIKE(old_quota_balance, '^[0-9]+$') THEN 0
		-- ELSE CAST(old_quota_balance AS DECIMAL(38, 2))
	  -- END	as old_quota_balance
	, 0 as old_quota_balance
	
	-- , CASE 
		-- WHEN TRIM(old_quota_value) IS NULL OR TRIM(old_quota_value) = '' OR NOT REGEXP_LIKE(old_quota_value, '^[0-9]+$') THEN 0
		-- ELSE CAST(old_quota_value AS DECIMAL(38, 2))
	  -- END	as old_quota_value
	, 0 as old_quota_value
	
	-- , CASE 
		-- WHEN TRIM(quotatype) IS NULL OR TRIM(quotatype) = '' THEN NULL
		-- ELSE quotatype
	  -- END	as quotatype
	, 0 as quotatype
	, TO_DECIMAL(TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY'))		as "year"
	, TO_DECIMAL(TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMM'))	as "month"
	, TO_DECIMAL(TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD'))	as "day"

from L0.EXT_UPCC a
	
	left join L0.EXT_UPCC_RO_ID b 
		on a.col_9 = b.service_name
		
where try_to_numeric(col_14) 
	and col_2 is not null 
	and upper(col_15) not in ('NA','AO_BCKT_THRLNG')
	and trim(col_14) in ('2', '3', '16','100','102','110','139','140','111','108','103','112','109','141')