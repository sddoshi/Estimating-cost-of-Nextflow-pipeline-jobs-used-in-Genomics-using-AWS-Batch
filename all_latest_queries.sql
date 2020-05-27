/*
  Estimating compute cost of NextFlow.io job run using AWS Batch, AWS Glue and Athena.
  ATHENA Tables and Views
  Tables are created in Athena using AWS Glue crawlers
  Queries given below used to create views on those tables for estimating the cost of Nextflow jobs run on AWS batch.
*/
--###################### VIEW: Per job costing #############################

CREATE OR REPLACE VIEW per_job_costing AS 
SELECT DISTINCT
  "a"."jobid"
, COALESCE(("c"."line_item_usage_amount" * "vf"."memory_factor"), 0) "COST_PER_GB_HOURS"
, "c"."line_item_usage_amount"
, "vf"."memory_factor"
, "vf"."line_item_unblended_rate"
, "vf"."product_memory"
, "a"."jobname"
, "a"."taskarn"
, "a"."containerinstancearn"
, "d"."ec2instanceid"
, "d"."instancetype"
FROM
  ((((athenacurcfn_customer_c_u_r_athena.parsed a
INNER JOIN athenacurcfn_customer_c_u_r_athena.customer_cur_athena c ON ("trim"("c"."line_item_resource_id") = "trim"("a"."taskarn")))
INNER JOIN athenacurcfn_customer_c_u_r_athena.ecs d ON ("trim"("a"."containerinstancearn") = "trim"("d"."containerinstancearn")))
INNER JOIN athenacurcfn_customer_c_u_r_athena.unique_jobid_for_container_arn vu ON ("vu"."jobid" = "a"."jobid"))
INNER JOIN athenacurcfn_customer_c_u_r_athena.vcpu_memory_factory vf ON ("vu"."ec2instanceid" = "vf"."ec2instanceid"))
WHERE ("c"."line_item_usage_type" = 'USE1-ECS-EC2-GB-Hours')

---######################### VIEW: Unique_jobid_for_container_arn
CREATE OR REPLACE VIEW unique_jobid_for_container_arn AS 
SELECT DISTINCT
  "a"."jobid"
, "d"."ec2instanceid"
, "d"."instancetype"
, "a"."taskarn"
, "d"."containerinstancearn"
FROM
  (athenacurcfn_customer_c_u_r_athena.parsed a
INNER JOIN athenacurcfn_customer_c_u_r_athena.ecs d ON ("trim"("a"."containerinstancearn") = "trim"("d"."containerinstancearn")))

--########################### View: vcpu_memory_factory
CREATE OR REPLACE VIEW vcpu_memory_factory AS 
SELECT DISTINCT
  "d"."ec2instanceid"
, COALESCE((CAST("line_item_unblended_rate" AS decimal(19,9)) / CAST("c"."product_vcpu" AS decimal(19,9))), 0) "vCPU_Factor"
, COALESCE((CAST("line_item_unblended_rate" AS decimal(19,9)) / CAST("replace"("trim"("c"."product_memory"), ' GiB') AS decimal(19,9))), 0) "Memory_Factor"
, "d"."instancetype"
, "c"."line_item_resource_id"
, "c"."product_vcpu"
, "c"."product_memory"
, "c"."line_item_unblended_rate"
, "c"."product_usagetype"
FROM
  (athenacurcfn_customer_c_u_r_athena.unique_jobid_for_container_arn d
INNER JOIN athenacurcfn_customer_c_u_r_athena.customer_cur_athena c ON ("c"."line_item_resource_id" = "d"."ec2instanceid"))
WHERE ("c"."product_usagetype" LIKE 'BoxUsage:%')


------------------------------ Tables created by Glue crawler on S3 ------------------------------

--########### Table1 ##########
SELECT * FROM "athenacurcfn_customer_c_u_r_athena"."cost_and_usage_data_status" limit 10;

--############# Table2 ############
SELECT * FROM "athenacurcfn_customer_c_u_r_athena"."cost_and_usage_data_status_daily" limit 10;

--########## Tabel 3
SELECT * FROM "athenacurcfn_customer_c_u_r_athena"."ecs" limit 10;

--######### Tabel 4
SELECT * FROM "athenacurcfn_customer_c_u_r_athena"."customer_cur_athena" limit 10;

--######### Tabel 5
SELECT * FROM "athenacurcfn_customer_c_u_r_athena"."customer_nextflow_trace_session" limit 10;

--####### Tabel 6
SELECT * FROM "athenacurcfn_customer_c_u_r_athena"."parsed" limit 10;


------------------------- JOIN: Final join to get cost agreegate per Nextflow job id  --------------------------------------

SELECT b.sessionid,
       sum(COST_PER_GB_HOURS) as JOB_COST
FROM "athenacurcfn_[customer-name]_c_u_r_athena"."per_job_costing" a
INNER JOIN "athenacurcfn_[customer-name]_c_u_r_athena"."[customer-name]_nextflow_trace_session" b
    ON trim(a.jobid) = trim(b.native_id)       
group by b.sessionid

