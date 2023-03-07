# imports
import psycopg2
import pandas as pd

# connection to aurora
# getting parameters from the secret store
connection = psycopg2.connect(
    host = 'your_RDB_AWS_instance_Endpoint',
    port = 5432,
    user = 'prs',
    password = 'YOUR_PASSWORD',
    database='prod_prs'
    )
cursor=connection.cursor()

# sql statements
# raw plus df to hold them
raw_sql = """
SELECT hcpc, descriptor
FROM cdr_raw_data.hcpcs_codes
"""
raw_df = pd.read_sql(raw_sql, con=connection)

upsert_asc_hcpcs_query = """
 insert into np_sdps_qnol.asc_hcpcs
            (hcpc_cd,
             hcpc_grp1_ind,
             hcpc_grp1_eff_dt,
             hcpc_grp1_end_dt,
             hcpc_grp2_ind,
             hcpc_grp2_eff_dt,
             hcpc_grp2_end_dt,
             hcpc_grp3_ind,
             hcpc_grp3_eff_dt,
             hcpc_grp3_end_dt
             )
SELECT     d.hcpcs_cd,
               d.hcpcs_asc_ind_cd,
               d.hcpcs_actn_efctv_dt,
               d.hcpcs_actn_end_dt,
               d.hcpc_grp2_ind,
	           d.hcpc_grp2_eff_dt,
	           d.hcpc_grp2_end_dt,
               null as hcpc_grp3_ind,
	           null as hcpc_grp3_eff_dt,
	           null as hcpc_grp3_end_dt
        FROM   (SELECT DISTINCT g.hcpcs_code as hcpcs_cd,
                                (case when g.asc_grp = 'YY' then replace (g.asc_grp, 'YY', 'Y') end) as hcpcs_asc_ind_cd,
                                g.asc_dt as hcpcs_actn_efctv_dt,
                                g.term_dt as hcpcs_actn_end_dt,
                                null as hcpc_grp2_ind,
                                a.hcpcs_asc_excluded_dt as hcpc_grp2_eff_dt,
                                a.last_edit_date as hcpc_grp2_end_dt
                FROM   cdr_raw_data.anweb_hcpcs_codes g left outer join cdr_raw_data.asc_hcpcs_codes a on (g.hcpcs_code = a.hcpcs_code)
                WHERE g.asc_grp = 'YY'
                AND cast (g.clndr_hcpcs_yr_num as integer)  >= extract(YEAR FROM now()) - 6
                ORDER  BY g.hcpcs_code ASC) d
on conflict (hcpc_cd, hcpc_grp1_ind, hcpc_grp1_eff_dt, hcpc_grp1_end_dt)
do update set (hcpc_grp2_eff_dt, hcpc_grp2_end_dt) = (excluded.hcpc_grp2_eff_dt, excluded.hcpc_grp2_end_dt);
commit;
""" 

upsert_sddw_hcpcs_query = """
		INSERT INTO np_sdps_sddw.l_hsc_cd_hcpcs (
		hcpcs_code, 
		HCPCS_DESCRIPTION,
		HCPCS_GROUP,
		HCPCS_GROUP_BEGIN_DT,
		HCPCS_GROUP_END_DT,
		ADDED_TO_FILE_DATE,
		LAST_EDIT_DATE,
		USER_ID
		)
		SELECT 	hcpcs_code, 
			HCPCS_DESCRIPTION,
			HCPCS_GROUP,
			HCPCS_GROUP_BEGIN_DT,
			HCPCS_GROUP_END_DT,
			ADDED_TO_FILE_DATE,
			LAST_EDIT_DATE,
			USER_ID
        FROM 
        (SELECT rank() over(PARTITION BY hcpcs_code ORDER BY c.rnk) AS rnk,
                              c.rnk AS rnk1,
					c.hcpcs_code, 
					c.HCPCS_DESCRIPTION,
					c.HCPCS_GROUP,
					c.HCPCS_GROUP_BEGIN_DT,
					c.HCPCS_GROUP_END_DT,
					c.ADDED_TO_FILE_DATE,
					c.LAST_EDIT_DATE,
					c.USER_ID,
					c.act_eff_dt
		        FROM
				(SELECT distinct h.rnk,
					h.hcpcs_code, 
					h.HCPCS_DESCRIPTION,
					h.HCPCS_GROUP,
					h.HCPCS_GROUP_BEGIN_DT,
					h.HCPCS_GROUP_END_DT,
					h.ADDED_TO_FILE_DATE,
					h.LAST_EDIT_DATE,
					h.USER_ID,
					h.act_eff_dt
		        FROM   
			        (SELECT rank() over(PARTITION BY d.hcpcs_code ORDER BY d.clndr_hcpcs_yr_num DESC, d.act_eff_dt DESC) AS rnk,
			            d.*
						from (SELECT DISTINCT g.hcpcs_code,
									g.clndr_hcpcs_yr_num,
									g.short_description as HCPCS_DESCRIPTION,
									(case when g.asc_grp = 'YY' then replace (g.asc_grp, 'YY', 'Y') end) as HCPCS_GROUP,
									g.asc_dt as HCPCS_GROUP_BEGIN_DT,
									g.term_dt as HCPCS_GROUP_END_DT,
									g.add_dt as ADDED_TO_FILE_DATE,
									g.act_eff_dt,
									(select now()::date) as LAST_EDIT_DATE,
									('new data process from CMS-site by ' || user) as user_id 
			                FROM   cdr_raw_data.anweb_hcpcs_codes g --left outer join cdr_raw_data.asc_hcpcs_codes a on (g.hcpcs_code = a.hcpcs_code)
			                WHERE  cast (clndr_hcpcs_yr_num as integer) = 
				                      (CASE
				                          WHEN extract(MONTH FROM now()) >= 8 THEN
				                           extract(YEAR FROM now()) + 1
				                          ELSE
				                           extract(YEAR FROM now())
				                      end)
			                ORDER  BY g.hcpcs_code asc
				                ) d
			            ) h
		         ) c
		      ) z order by 2, 1   
on conflict (hcpcs_code, hcpcs_description, hcpcs_group_begin_dt, added_to_file_date)
do update set (hcpcs_description, hcpcs_group, hcpcs_group_begin_dt, hcpcs_group_end_dt, added_to_file_date, last_edit_date, user_id) = (excluded.hcpcs_description, excluded.hcpcs_group, excluded.hcpcs_group_begin_dt, excluded.hcpcs_group_end_dt, excluded.added_to_file_date, excluded.last_edit_date, excluded.user_id);
commit;
"""

upsert_sddw_hcpcs_query = """
		INSERT INTO np_sdps_sdol.l_hsc_cd_hcpcs (
		hcpcs_code, 
		HCPCS_DESCRIPTION,
		HCPCS_GROUP,
		HCPCS_GROUP_BEGIN_DT,
		HCPCS_GROUP_END_DT,
		ADDED_TO_FILE_DATE,
		LAST_EDIT_DATE,
		USER_ID
		)
		SELECT 	hcpcs_code, 
			HCPCS_DESCRIPTION,
			HCPCS_GROUP,
			HCPCS_GROUP_BEGIN_DT,
			HCPCS_GROUP_END_DT,
			ADDED_TO_FILE_DATE,
			LAST_EDIT_DATE,
			USER_ID
        FROM 
        (SELECT rank() over(PARTITION BY hcpcs_code ORDER BY c.rnk) AS rnk,
                              c.rnk AS rnk1,
					c.hcpcs_code, 
					c.HCPCS_DESCRIPTION,
					c.HCPCS_GROUP,
					c.HCPCS_GROUP_BEGIN_DT,
					c.HCPCS_GROUP_END_DT,
					c.ADDED_TO_FILE_DATE,
					c.LAST_EDIT_DATE,
					c.USER_ID,
					c.act_eff_dt
		        FROM
				(SELECT distinct h.rnk,
					h.hcpcs_code, 
					h.HCPCS_DESCRIPTION,
					h.HCPCS_GROUP,
					h.HCPCS_GROUP_BEGIN_DT,
					h.HCPCS_GROUP_END_DT,
					h.ADDED_TO_FILE_DATE,
					h.LAST_EDIT_DATE,
					h.USER_ID,
					h.act_eff_dt
		        FROM   
			        (SELECT rank() over(PARTITION BY d.hcpcs_code ORDER BY d.clndr_hcpcs_yr_num DESC, d.act_eff_dt DESC) AS rnk,
			            d.*
						from (SELECT DISTINCT g.hcpcs_code,
									g.clndr_hcpcs_yr_num,
									g.short_description as HCPCS_DESCRIPTION,
									(case when g.asc_grp = 'YY' then replace (g.asc_grp, 'YY', 'Y') end) as HCPCS_GROUP,
									g.asc_dt as HCPCS_GROUP_BEGIN_DT,
									g.term_dt as HCPCS_GROUP_END_DT,
									g.add_dt as ADDED_TO_FILE_DATE,
									g.act_eff_dt,
									(select now()::date) as LAST_EDIT_DATE,
									('new data process from CMS-site by ' || user) as user_id 
			                FROM   cdr_raw_data.anweb_hcpcs_codes g --left outer join cdr_raw_data.asc_hcpcs_codes a on (g.hcpcs_code = a.hcpcs_code)
			                WHERE  cast (clndr_hcpcs_yr_num as integer) = 
				                      (CASE
				                          WHEN extract(MONTH FROM now()) >= 8 THEN
				                           extract(YEAR FROM now()) + 1
				                          ELSE
				                           extract(YEAR FROM now())
				                      end)
			                ORDER  BY g.hcpcs_code asc
				                ) d
			            ) h
		         ) c
		      ) z order by 2, 1   
on conflict (hcpcs_code, hcpcs_description, hcpcs_group_begin_dt, added_to_file_date)
do update set (hcpcs_description, hcpcs_group, hcpcs_group_begin_dt, hcpcs_group_end_dt, added_to_file_date, last_edit_date, user_id) = (excluded.hcpcs_description, excluded.hcpcs_group, excluded.hcpcs_group_begin_dt, excluded.hcpcs_group_end_dt, excluded.added_to_file_date, excluded.last_edit_date, excluded.user_id);
commit;
"""