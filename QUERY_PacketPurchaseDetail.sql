
insert overwrite  into POC_XL_1.L1.PACKET_PURCHASE_DETAIL
with det_dwh as --Purchase Combo Payment
(select 
SYS_CREATION_DATE, PROCESS_DATE, CUSTOMER_ID, SUBSCRIBER_ID, EVENT_TYPE, PRIM_RESOURCE_VAL, PPS_NEW_BALANCE, PPS_AMT, CD_MAIN_ID, L9_DEALER_CGI, SOC_CD, FILE_ID, SESSION_COUNTER, CHARGING_DATE, SESSION_ID, SOURCE_ID, PYM_CAT, THIRD_PARTY_INFO, HYBRID_IND, THIRD_PARTY_INFO_AMT, YEAR, MONTH, DAY
from POC_XL_1.L0.EXT_DET_DWH
--where len(SPLIT_PART(third_party_info_amt, '|', 6)) > 40
--and third_party_info_amt is not null
--limit 100
),
det_dwh_split as
(
select * , value as isi
from det_dwh,    
LATERAL FLATTEN(input => SPLIT(SPLIT_PART(third_party_info_amt, '|', 6), ';'))
--limit 100
)
(
(
SELECT
	to_date(sys_creation_date) as date_id ,
	null as rcg_id,
	substr(sys_creation_date,12,8) as t_timestamp,
	null as gis_sk,
    -- COALESCE(
    --     DETDWH.l9_dealer_cgi,                                      
    --     CASE
    --         WHEN smsmt_cgi.calld_nbr IS NOT NULL AND substr(sys_creation_date,12,2) = HOUR(smsmt_cgi.evt_time::timestamp) AND substr(sys_creation_date,15,2) = MINUTE(smsmt_cgi.evt_time::timestamp)
    --         THEN smsmt_cgi.cell_id
    --         WHEN smsmt_cgi.calld_nbr IS NOT NULL AND substr(sys_creation_date,12,2) = HOUR(smsmt_cgi.evt_time::timestamp)
    --         THEN smsmt_cgi.cell_id
    --         ELSE NULL
    --     END,
    --     eir_cgi.cgi                                      
    -- ) AS cgi, --tbc untuk column smsmt_cgi.cell_id kalau udah confirm column-nya tinggal diganti 
    null as cgi,
	null as method_id,
	DETDWH.subscriber_id,
	DETDWH.prim_resource_val,
	DETDWH.customer_id,
	null as pym_chl_no,
	null as bal_exp_dte,
	null as rmd_main_id,
	DETDWH.soc_cd,
	SPLIT_PART(DETDWH.isi, '=', 1) as cd_main_id,
	//'' as pps_amt,
    cast(SPLIT_PART(DETDWH.isi, '=', 2) as float) as pps_amt,
	DETDWH.pym_cat,
	DETDWH.third_party_info,
	DETDWH.third_party_info_amt,
	DETDWH.hybrid_ind,
	TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH:MM:SS') as insert_date 
FROM
    det_dwh_split DETDWH
-- LEFT JOIN
--     POC_XL_1.L0.ext_smsmt_sample smsmt_cgi 
--     ON DETDWH.prim_resource_val = smsmt_cgi.calld_nbr
--     AND (
--            (substr(sys_creation_date,12,2) = HOUR(smsmt_cgi.evt_time::timestamp) AND substr(sys_creation_date,15,2) = MINUTE(smsmt_cgi.evt_time::timestamp))
--         OR (substr(sys_creation_date,12,2) = HOUR(smsmt_cgi.evt_time::timestamp))
--     )
-- LEFT JOIN
--     POC_XL_1.L1.EIR eir_cgi
--     ON DETDWH.prim_resource_val = eir_cgi.subs_no
--     AND substr(sys_creation_date,12,2) = HOUR(eir_cgi.ts::timestamp)
-- LEFT JOIN 
-- 	--POC_XL_1.L0.ext_smsmt_sample smsmt_main
-- 	POC_XL_1.L1.SMSMT smsmt_main
--     ON smsmt_main.calld_nbr = DETDWH.prim_resource_val
-- 	AND smsmt_main.evt_date = to_date(sys_creation_date)
-- 	AND smsmt_main.evt_time = substr(sys_creation_date,12,8)
-- LEFT JOIN 
--     POC_XL_1.L1.EIR eir_main	
-- 	ON eir_main.subs_no = DETDWH.subscriber_id
WHERE 
	 trim(pym_cat) in ('PRE','BOTH')
	 and substr(cd_main_id,1,5) not in ('AXTK_','XLTK_')
	 and cd_main_id in ('RFSISP','RFSIMP', 'RFMISP', 'RFMIMP','SISP','SIMP', 'MISP', 'MIMP')
     -- and SPLIT_PART(DETDWH.isi, '=', 1) <> ''
)
UNION ALL
( --Purchase Normal
SELECT
	to_date(sys_creation_date) as date_id ,
	null as rcg_id,
	substr(sys_creation_date,12,8) as t_timestamp,
	null as gis_sk,
    -- COALESCE(
    --     DETDWH.l9_dealer_cgi,                                      
    --     CASE
    --         WHEN smsmt_cgi.calld_nbr IS NOT NULL AND substr(sys_creation_date,12,2) = HOUR(smsmt_cgi.evt_time::timestamp) AND substr(sys_creation_date,15,2) = MINUTE(smsmt_cgi.evt_time::timestamp)
    --         THEN smsmt_cgi.cell_id
    --         WHEN smsmt_cgi.calld_nbr IS NOT NULL AND substr(sys_creation_date,12,2) = HOUR(smsmt_cgi.evt_time::timestamp)
    --         THEN smsmt_cgi.cell_id
    --         ELSE NULL
    --     END,
    --     eir_cgi.cgi                                      
    -- ) AS cgi, --tbc untuk column smsmt_cgi.cell_id kalau udah confirm column-nya tinggal diganti 
    null as cgi,
	null as method_id,
	DETDWH.subscriber_id,
	DETDWH.prim_resource_val,
	DETDWH.customer_id,
	null as pym_chl_no,
	null as bal_exp_dte,
	null as rmd_main_id,
	DETDWH.soc_cd,
	DETDWH.cd_main_id,
	DETDWH.pps_amt,
	DETDWH.pym_cat,
	DETDWH.third_party_info,
	DETDWH.third_party_info_amt,
	DETDWH.hybrid_ind,
	TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH:MM:SS') as insert_date 
FROM
    POC_XL_1.L0.EXT_DET_DWH DETDWH
LEFT JOIN
    POC_XL_1.L0.ext_smsmt_sample smsmt_cgi 
    ON DETDWH.prim_resource_val = smsmt_cgi.calld_nbr
    AND (
           (substr(sys_creation_date,12,2) = HOUR(smsmt_cgi.evt_time::timestamp) AND substr(sys_creation_date,15,2) = MINUTE(smsmt_cgi.evt_time::timestamp))
        OR (substr(sys_creation_date,12,2) = HOUR(smsmt_cgi.evt_time::timestamp))
    )
-- LEFT JOIN
--     POC_XL_1.L1.EIR eir_cgi
--     ON DETDWH.prim_resource_val = eir_cgi.subs_no
--     AND substr(sys_creation_date,12,2) = HOUR(eir_cgi.ts::timestamp)
-- LEFT JOIN 
-- 	--POC_XL_1.L0.ext_smsmt_sample smsmt_main
-- 	POC_XL_1.L1.SMSMT smsmt_main
--     ON smsmt_main.calld_nbr = DETDWH.prim_resource_val
-- 	AND smsmt_main.evt_date = to_date(sys_creation_date)
-- 	AND smsmt_main.evt_time = substr(sys_creation_date,12,8)
-- LEFT JOIN 
--     POC_XL_1.L1.EIR eir_main	
-- 	ON eir_main.subs_no = DETDWH.subscriber_id
WHERE 
	 trim(pym_cat) in ('PRE','BOTH')
	 and substr(cd_main_id,1,5) not in ('AXTK_','XLTK_')
	 and cd_main_id not in ('RFSISP','RFSIMP', 'RFMISP', 'RFMIMP','SISP','SIMP', 'MISP', 'MIMP')
)
UNION ALL
( --Purchase SiDOmpul
SELECT
	to_date(sys_creation_date) as date_id ,
	null as rcg_id,
	substr(sys_creation_date,12,8) as t_timestamp,
	null as gis_sk,
    --COALESCE(
    --    DETDWH.l9_dealer_cgi,                                      
    --    CASE
    --        WHEN smsmt_cgi.calld_nbr IS NOT NULL AND substr(sys_creation_date,12,2) = HOUR(smsmt_cgi.evt_time::timestamp) AND substr(sys_creation_date,15,2) = MINUTE(smsmt_cgi.evt_time::timestamp)
    --        THEN smsmt_cgi.cell_id
    --        WHEN smsmt_cgi.calld_nbr IS NOT NULL AND substr(sys_creation_date,12,2) = HOUR(smsmt_cgi.evt_time::timestamp)
    --        THEN smsmt_cgi.cell_id
    --        ELSE NULL
    --    END,
    --    eir_cgi.cgi                                      
    --) AS cgi, --tbc untuk column smsmt_cgi.cell_id kalau udah confirm column-nya tinggal diganti 
    null as cgi,
    null as method_id,
	DETDWH.subscriber_id,
	SPLIT_PART(DETDWH.l9_dealer_cgi, '-', 1) as subs_key, 
	DETDWH.customer_id,
	null as pym_chl_no,
	null as bal_exp_dte,
	null as rmd_main_id,
	DETDWH.soc_cd,
	DETDWH.cd_main_id,
	DETDWH.pps_amt,
	DETDWH.pym_cat,
	DETDWH.third_party_info,
	DETDWH.third_party_info_amt,
	DETDWH.hybrid_ind,
	TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH:MM:SS') as insert_date 
FROM
    POC_XL_1.L0.EXT_DET_DWH DETDWH
LEFT JOIN
    POC_XL_1.L0.ext_smsmt_sample smsmt_cgi 
    ON DETDWH.prim_resource_val = smsmt_cgi.calld_nbr
    AND (
           (substr(sys_creation_date,12,2) = HOUR(smsmt_cgi.evt_time::timestamp) AND substr(sys_creation_date,15,2) = MINUTE(smsmt_cgi.evt_time::timestamp))
        OR (substr(sys_creation_date,12,2) = HOUR(smsmt_cgi.evt_time::timestamp))
    )
-- LEFT JOIN
--     POC_XL_1.L1.EIR eir_cgi
--     ON DETDWH.prim_resource_val = eir_cgi.subs_no
--     AND substr(sys_creation_date,12,2) = HOUR(eir_cgi.ts::timestamp)
-- LEFT JOIN 
-- 	POC_XL_1.L0.ext_smsmt_sample smsmt_main
-- 	ON smsmt_main.calld_nbr = DETDWH.prim_resource_val --ini cuman sementara sampe column smsmt.serve_msisdn confirm
-- 	--ON smsmt_main.serve_msisdn = DETDWH.prim_resource_val  --ini nyalain aja atau ganti sama pengganti smsmt.serve_msisdn kalau udah confirm
-- 	AND smsmt_main.evt_date = to_date(sys_creation_date)
-- 	AND smsmt_main.evt_time = substr(sys_creation_date,12,8)
-- LEFT JOIN 
--     POC_XL_1.L1.EIR eir_main	
-- 	ON eir_main.subs_no = DETDWH.subscriber_id
WHERE 
	trim(pym_cat) in ('PRE','BOTH')
	and substr(cd_main_id,1,5)  in ('AXTK_','XLTK_')
))
;
