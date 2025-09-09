insert into POC_XL_1.L1.USG_NRT
WITH CleanedDaw AS (
    SELECT
        -- daw.*,
        REPLACE(daw.customer_id, CHR(1), '') AS customer_id,
        REPLACE(daw.cycle_code, CHR(1), '') AS cycle_code,
        REPLACE(daw.cycle_month, CHR(1), '') AS cycle_month,
        REPLACE(daw.cycle_year, CHR(1), '') AS cycle_year,
        REPLACE(daw.event_id, CHR(1), '') AS event_id,
        REPLACE(daw.event_type_id, CHR(1), '') AS event_type_id,
        LTRIM(RTRIM(REPLACE(daw.subscriber_id, CHR(1), ''))) AS subscriber_id,
        LTRIM(RTRIM(REPLACE(daw.account, CHR(1), ''))) AS account,
        LTRIM(RTRIM(REPLACE(daw.billing_arrangement, CHR(1), ''))) AS billing_arrangement,
        LTRIM(RTRIM(REPLACE(daw.logical_destination, CHR(1), ''))) AS logical_destination,
        LTRIM(RTRIM(REPLACE(daw.payment_category, CHR(1), ''))) AS payment_category,
        REPLACE(daw.request_number, CHR(1), '') AS request_number,
        LTRIM(RTRIM(REPLACE(daw.resource_type, CHR(1), ''))) AS resource_type,
        REPLACE(daw.resource_value, CHR(1), '') AS resource_value,
        REPLACE(daw.service_key, CHR(1), '') AS service_key,
        LTRIM(RTRIM(REPLACE(daw.service_message, CHR(1), ''))) AS service_message,
        CASE
            WHEN TRY_TO_NUMBER(SPLIT_PART(daw.session_id, '_', 1)) IS NOT NULL AND POSITION('_' IN daw.session_id) > 0
            THEN SPLIT_PART(daw.session_id, '_', 1)
            ELSE '0'
        END AS session_id_part1_str,
        CASE
            WHEN TRY_TO_NUMBER(TRIM(SPLIT_PART(daw.session_id, '_', 2))) IS NOT NULL AND POSITION('_' IN daw.session_id) > 0
            THEN TRIM(SPLIT_PART(daw.session_id, '_', 2))
            ELSE '0'
        END AS session_id_part2_str,
        LTRIM(RTRIM(REPLACE(daw.session_id, CHR(1), ''))) AS session_id,
        REPLACE(daw.source_id, CHR(1), '') AS source_id,
        COALESCE(REPLACE(daw.subscriber_key_source, CHR(1), ''), REPLACE(daw.resource_value, CHR(1), '')) AS gis_lookup_value,
		daw.subscriber_key_source,
        REPLACE(daw.additional_offer_id, CHR(1), '') AS additional_offer_id,
        REPLACE(daw.balance_amount, CHR(1), '') AS balance_amount,
        REPLACE(daw.charging_quantity, CHR(1), '') AS charging_quantity,
        LTRIM(RTRIM(REPLACE(daw.charging_quantity_uom, CHR(1), ''))) AS charging_quantity_uom,
        REPLACE(daw.charging_rate, CHR(1), '') AS charging_rate,
        LTRIM(RTRIM(REPLACE(daw.charging_rate_uom, CHR(1), ''))) AS charging_rate_uom,
        LTRIM(RTRIM(REPLACE(daw.cna, CHR(1), ''))) AS cna,
        REPLACE(daw.discount_amount, CHR(1), '') AS discount_amount,
        LTRIM(RTRIM(REPLACE(daw.event_period, CHR(1), ''))) AS event_period,
        REPLACE(daw.free_units, CHR(1), '') AS free_units,
        LTRIM(RTRIM(REPLACE(daw.last_step, CHR(1), ''))) AS last_step,
        LTRIM(RTRIM(REPLACE(daw.originating_cell_id, CHR(5) || CHR(1), ''))) AS originating_cell_id,
        LTRIM(RTRIM(REPLACE(daw.physical_poc, CHR(1), ''))) AS physical_poc,
        REPLACE(daw.price_plan_id, CHR(1), '') AS price_plan_id,
        REPLACE(daw.rated_amount, CHR(1), '') AS rated_amount,
        REPLACE(daw.rated_offer_id, CHR(1), '') AS rated_offer_id,
        REPLACE(CASE WHEN CHARINDEX('-', daw.rated_units) > 0 THEN SUBSTRING(daw.rated_units, CHARINDEX('-', daw.rated_units), LENGTH(daw.rated_units)) ELSE daw.rated_units END, CHR(1), '') AS rated_units,
        REPLACE(daw.tax_amount, CHR(1), '') AS tax_amount,
        REPLACE(daw.charged_units, CHR(1), '') AS charged_units,
        REPLACE(daw.reserved_units, CHR(1), '') AS reserved_units,
        REPLACE(daw.usage_units, CHR(1), '') AS usage_units,
        TRY_CAST(REPLACE(daw.b_number, CHR(1), '') AS BIGINT) AS b_number,
        LTRIM(RTRIM(REPLACE(daw.destination_poc, CHR(1), ''))) AS destination_poc,
        LTRIM(RTRIM(REPLACE(daw.dialed_digits, CHR(1), ''))) AS dialed_digits,
        LTRIM(RTRIM(REPLACE(daw.dna, CHR(1), ''))) AS dna,
        LTRIM(RTRIM(REPLACE(daw.terminating_cell_id, CHR(1), ''))) AS terminating_cell_id,
        REPLACE(daw.volume_reserved_units, CHR(1), '') AS volume_reserved_units,
        REPLACE(daw.total_volume_usage_units, CHR(1), '') AS total_volume_usage_units,
        LTRIM(RTRIM(REPLACE(daw.apn, CHR(1), ''))) AS apn,
        LTRIM(RTRIM(REPLACE(daw.apn_rating_type, CHR(1), ''))) AS apn_rating_type,
        LTRIM(RTRIM(REPLACE(daw.sid, CHR(1), ''))) AS sid,
        LTRIM(RTRIM(REPLACE(daw.media_content_type, CHR(1), ''))) AS media_content_type,
        LTRIM(RTRIM(REPLACE(daw.physical_poc_description, CHR(1), ''))) AS physical_poc_description,
        LTRIM(RTRIM(REPLACE(daw.destination_poc_description, CHR(1), ''))) AS destination_poc_description,
        LTRIM(RTRIM(REPLACE(daw.origin_country_name, CHR(1), ''))) AS origin_country_name,
        LTRIM(RTRIM(REPLACE(daw.destination_country_name, CHR(1), ''))) AS destination_country_name,
        LTRIM(RTRIM(REPLACE(daw.url, CHR(1), ''))) AS url,
        LTRIM(RTRIM(REPLACE(daw.cgi_group_id, CHR(1), ''))) AS cgi_group_id,
        LTRIM(RTRIM(REPLACE(daw.l9_imsi, CHR(1), ''))) AS l9_imsi,
        LTRIM(RTRIM(REPLACE(daw.l9_future_string_2, CHR(1), ''))) AS l9_future_string_2,
        REPLACE(daw.l9_charge_amt_beff_allowance, CHR(1), '') AS l9_charge_amt_beff_allowance,
        LTRIM(RTRIM(REPLACE(daw.l9_future_string_5, CHR(1), ''))) AS l9_future_string_5,
        LTRIM(RTRIM(REPLACE(daw.l9_future_string_6, CHR(1), ''))) AS l9_future_string_6,
        LTRIM(RTRIM(REPLACE(daw.sgsn, CHR(1), ''))) AS sgsn,
        LTRIM(RTRIM(daw.session_counter)) AS session_counter,
        LTRIM(RTRIM(REPLACE(daw.counter_id, CHR(1), ''))) AS counter_id,
        LTRIM(RTRIM(REPLACE(daw.counter_status, CHR(1), ''))) AS counter_status,
        LTRIM(RTRIM(REPLACE(daw.info_bucket_1, CHR(1), ''))) AS info_bucket_1,
        LTRIM(RTRIM(REPLACE(daw.info_bucket_2, CHR(1), ''))) AS info_bucket_2,
        LTRIM(RTRIM(REPLACE(daw.info_bucket_3, CHR(1), ''))) AS info_bucket_3,
        LTRIM(RTRIM(REPLACE(daw.info_bucket_4, CHR(1), ''))) AS info_bucket_4,
        LTRIM(RTRIM(REPLACE(daw.info_bucket_5, CHR(1), ''))) AS info_bucket_5,
        LTRIM(RTRIM(REPLACE(daw.info_quota, CHR(1), ''))) AS info_quota,
        REPLACE(daw.hybrid_ind, CHR(1), '') AS hybrid_ind,
        daw.start_time,
        daw.record_prefix,
        daw.service_filter,
        daw.event_process_date,
        daw.event_type,
        daw.free_indicator,
        daw.payment_channel,
        daw.payment_channel_uom,
        daw.request_type,
        daw.service_type,
        daw.event_action_code,
        daw.unit_type,
        daw.home_poc,
        daw.call_zone,
        daw.roam_zone,
        daw.volume_unit_type,
        daw.qos,
        daw.first_event,
        daw.message_class,
        daw.allowance_ind,
        daw.employee_id,
        daw.cell_capacity
        ,daw.day_of_week  
        ,daw.l9_community_disc_ind  
        ,daw.l9_community_disc_percent  
        ,daw.l9_cgi_discount_ind  
        ,daw.l9_discount_id    
        ,daw.third_party_info  
        ,daw.third_party_info_amt  
        ,ROW_NUMBER() OVER (ORDER BY daw.start_time, daw.customer_id, daw.session_id) AS rn_key
    FROM
        POC_XL_1.L0.EXT_USAGE_NRT AS daw 
),
GIS_Prefix_Expanded AS (
    SELECT
        c.rn_key,
        c.gis_lookup_value,
        g.prefix AS ref_prefix,
        g.gis_id
    FROM CleanedDaw c
    JOIN POC_XL_1.L0.EXT_REF_PREFIX_GIS g
        ON (
            g.prefix = TRY_CAST(SUBSTRING(c.gis_lookup_value, 1, 10) AS DECIMAL) OR
            g.prefix = TRY_CAST(SUBSTRING(c.gis_lookup_value, 1, 9) AS DECIMAL) OR
            g.prefix = TRY_CAST(SUBSTRING(c.gis_lookup_value, 1, 8) AS DECIMAL) OR
            g.prefix = TRY_CAST(SUBSTRING(c.gis_lookup_value, 1, 7) AS DECIMAL)
        )
),
GIS_Prefix_Lookup AS (
    SELECT
        rn_key,
        gis_id
    FROM GIS_Prefix_Expanded
    QUALIFY ROW_NUMBER() OVER (PARTITION BY rn_key ORDER BY LENGTH(ref_prefix) DESC) = 1
),
DOM_Prefix_Expanded AS (
    SELECT
        c.rn_key,
        c.b_number,
        d.prefix AS ref_prefix,
        d.prefix_dom_sk_id
    FROM CleanedDaw c
    JOIN POC_XL_1.L0.EXT_DIM_PREFIX_DOM d
        ON d.prefix IN (
            SUBSTRING(c.b_number::VARCHAR, 1, 9),
            SUBSTRING(c.b_number::VARCHAR, 1, 8),
            SUBSTRING(c.b_number::VARCHAR, 1, 7),
            SUBSTRING(c.b_number::VARCHAR, 1, 6),
            SUBSTRING(c.b_number::VARCHAR, 1, 5),
            SUBSTRING(c.b_number::VARCHAR, 1, 4),
            SUBSTRING(c.b_number::VARCHAR, 1, 3)
        )
),
DOM_Prefix_Lookup AS (
    SELECT
        rn_key,
        prefix_dom_sk_id
    FROM DOM_Prefix_Expanded
    QUALIFY ROW_NUMBER() OVER (PARTITION BY rn_key ORDER BY LENGTH(ref_prefix) DESC) = 1
),
IDD_Prefix_Expanded AS (
    SELECT
        c.rn_key,
        c.b_number,
        i.prefix_intl AS ref_prefix_intl,
        i.prefix_idd_sk_id
    FROM CleanedDaw c
    JOIN POC_XL_1.L0.EXT_DIM_PREFIX_IDD i
        ON i.prefix_intl IN (
            TRY_CAST(SUBSTRING(c.b_number::VARCHAR, 1, 7) AS BIGINT),
            TRY_CAST(SUBSTRING(c.b_number::VARCHAR, 1, 6) AS BIGINT),
            TRY_CAST(SUBSTRING(c.b_number::VARCHAR, 1, 5) AS BIGINT),
            TRY_CAST(SUBSTRING(c.b_number::VARCHAR, 1, 4) AS BIGINT),
            TRY_CAST(SUBSTRING(c.b_number::VARCHAR, 1, 3) AS BIGINT),
            TRY_CAST(SUBSTRING(c.b_number::VARCHAR, 1, 2) AS BIGINT),
            TRY_CAST(SUBSTRING(c.b_number::VARCHAR, 1, 1) AS BIGINT)
        )
),
IDD_Prefix_Lookup AS (
    SELECT
        rn_key,
        prefix_idd_sk_id
    FROM IDD_Prefix_Expanded
    QUALIFY ROW_NUMBER() OVER (PARTITION BY rn_key ORDER BY LENGTH(ref_prefix_intl) DESC) = 1
)

select 
    c.record_prefix,
    CAST(c.customer_id AS DECIMAL) AS customer_id_decimal,
    CAST(c.cycle_code AS DECIMAL) AS cycle_code_decimal,
    CAST(c.cycle_month AS DECIMAL) AS cycle_month_decimal,
    CAST(c.cycle_year AS DECIMAL) AS cycle_year_decimal,
    CAST(c.event_id AS DECIMAL) AS event_id_decimal,
    CAST(c.event_type_id AS DECIMAL) AS event_type_id_decimal,
    c.service_filter,
    c.start_time, 
    c.subscriber_id,
    c.account,
    c.billing_arrangement,
    c.event_process_date,
    c.event_type,
    c.free_indicator,
    c.logical_destination,
    c.payment_category,
    c.payment_channel,
    c.payment_channel_uom,
    CAST(c.request_number AS DECIMAL) AS request_number_decimal,
    c.request_type,
    c.resource_type,
    CAST(c.resource_value AS DECIMAL) AS resource_value_array,
    CAST(c.service_key AS DECIMAL) AS service_key_decimal,
    c.service_message,
    c.service_type,
    c.session_id_part1_str AS session_id_part1,
    c.session_id_part2_str AS session_id_part2,
    c.session_id,
    CAST(c.source_id AS DECIMAL) AS source_id_decimal,
    CAST(c.gis_lookup_value AS DECIMAL) AS subscriber_key_processed,
    CAST(c.additional_offer_id AS DECIMAL) AS additional_offer_id_decimal,
    CAST(c.balance_amount AS DECIMAL) AS balance_amount_decimal,
    CAST(c.charging_quantity AS DECIMAL) AS charging_quantity_decimal,
    c.charging_quantity_uom,
    CAST(c.charging_rate AS DECIMAL) AS charging_rate_decimal,
    c.charging_rate_uom,
    c.cna,
    CAST(c.discount_amount AS DECIMAL) AS discount_amount_decimal,
    c.event_action_code,
    c.event_period,
    c.unit_type,
    CAST(c.free_units AS DECIMAL) AS free_units_decimal,
    c.home_poc,
    c.last_step,
    c.originating_cell_id,
    c.physical_poc,
    CAST(c.price_plan_id AS DECIMAL) AS price_plan_id_decimal,
    CAST(c.rated_amount AS DECIMAL) AS rated_amount_decimal,
    CAST(c.rated_offer_id AS DECIMAL) AS rated_offer_id_decimal,
    CAST(c.rated_units AS DECIMAL) AS rated_units_decimal,
    CAST(c.tax_amount AS DECIMAL) AS tax_amount_decimal,
    c.call_zone,
    CAST(c.charged_units AS DECIMAL) AS charged_units_decimal,
    CAST(c.reserved_units AS DECIMAL) AS reserved_units_decimal,
    CAST(c.usage_units AS DECIMAL) AS usage_units_decimal,
    c.roam_zone,
    c.b_number,
    c.destination_poc,
    c.dialed_digits,
    c.dna,
    c.terminating_cell_id,
    c.volume_unit_type,
    CAST(c.volume_reserved_units AS DECIMAL) AS volume_reserved_units_decimal,
    CAST(c.total_volume_usage_units AS DECIMAL) AS total_volume_usage_units_decimal,
    c.qos,
    c.apn,
    c.apn_rating_type,
    c.first_event,
    c.sid,
    c.message_class,
    c.media_content_type,
    c.physical_poc_description,
    c.destination_poc_description,
    c.origin_country_name,
    c.destination_country_name,
    c.allowance_ind,
    c.url,
    c.employee_id,
    c.cgi_group_id,
    c.cell_capacity,
    c.day_of_week,
    c.l9_imsi,
    c.l9_future_string_2,
    CAST(c.l9_charge_amt_beff_allowance AS DECIMAL) AS l9_charge_amt_beff_allowance_decimal,
    c.l9_future_string_5,
    c.l9_future_string_6,
    c.l9_community_disc_ind,
    c.l9_community_disc_percent,
    c.l9_cgi_discount_ind,
    c.l9_discount_id,
    TO_DECIMAL(TO_CHAR(c.start_time::TIMESTAMP_NTZ, 'YYYYMMDDHH24MISS'), 18, 0) AS start_timestamp_converted,
    dim_gis.gis_sk,
    '' AS get_header_record_set_id,
    '' AS unknown_column_placeholder,
    c.sgsn,
    prod2.prod_sk_id,
    CASE WHEN onnet_pref.prefix IS NOT NULL THEN 'OnNet' ELSE 'OffNet' END AS network_type,
    pp.pp_sk_id,
    sub_prod.soc_sk_id,
    COALESCE(dom_lkp.prefix_dom_sk_id, -1) AS domestic_prefix_sk,
    COALESCE(idd_lkp.prefix_idd_sk_id, -1) AS international_prefix_sk,
    COALESCE(spc.prefix_spc_sk_id, -1) AS spc_prefix_sk,
    '' AS charge_date,
    c.session_counter,
    c.counter_id,
    c.counter_status,
    c.info_bucket_1,
    c.info_bucket_2,
    c.info_bucket_3,
    c.info_bucket_4,
    c.info_bucket_5,
    c.info_quota,
    c.third_party_info,
    c.hybrid_ind,
    c.third_party_info_amt
FROM
    CleanedDaw AS c
LEFT JOIN
    GIS_Prefix_Lookup AS gpl ON c.rn_key = gpl.rn_key
LEFT JOIN
    POC_XL_1.L0.EXT_DIM_GIS AS dim_gis ON dim_gis.gis_code = gpl.gis_id
LEFT JOIN
    DOM_Prefix_Lookup AS dom_lkp ON c.rn_key = dom_lkp.rn_key
LEFT JOIN
    IDD_Prefix_Lookup AS idd_lkp ON c.rn_key = idd_lkp.rn_key
LEFT JOIN
    POC_XL_1.L0.EXT_DIM_PP AS pp ON pp.pp_id = CAST(c.price_plan_id AS DECIMAL) AND pp.payment_cat = c.payment_category
LEFT JOIN
    POC_XL_1.L0.EXT_DIM_PREFIX_SPC AS spc
    ON spc.sdc = c.b_number
    AND spc.service_key = CAST(c.service_key AS DECIMAL)
    AND spc.call_zone = TRIM(c.call_zone)
LEFT JOIN
    POC_XL_1.L0.EXT_DIM_PRODUCT_2 AS prod2
    ON prod2.event_action_code = TRIM(c.event_action_code)
    AND prod2.event_type = TRIM(c.event_type)
    AND prod2.service_type = TRIM(c.service_type)
    AND prod2.service_key = TRIM(CAST(c.service_key AS DECIMAL))
    AND prod2.service_filter = TRIM(c.service_filter)
    AND prod2.call_zone = TRIM(c.call_zone)
LEFT JOIN
    POC_XL_1.L0.EXT_DIM_SUB_PRODUCT AS sub_prod
    ON sub_prod.soc_id = CAST(c.rated_offer_id AS DECIMAL)
LEFT JOIN
    POC_XL_1.L0.EXT_REF_ONNET_PREFIX AS onnet_pref ON onnet_pref.prefix = SUBSTRING(TRIM(c.dna), 5, 5)
WHERE
    (TRIM(c.event_type) = 'X' AND
     UPPER(TRIM(c.service_filter)) IN ('PREPYM', 'MO2MV0') AND
     c.payment_category = 'BOTH')
    OR (c.service_filter != 'MO2MV0')
;