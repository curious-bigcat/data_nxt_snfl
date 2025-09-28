use role ACCOUNTADMIN;
set myname = current_user();
create role if not exists VHOL;
grant role VHOL to user identifier($myname);
grant create database on account to role VHOL;
grant EXECUTE TASK, EXECUTE MANAGED TASK on ACCOUNT to role VHOL;
grant IMPORTED PRIVILEGES on DATABASE SNOWFLAKE to role VHOL;


create or replace warehouse VHOL_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 5, AUTO_RESUME= TRUE;
grant all privileges on warehouse VHOL_WH to role VHOL;

use role VHOL;
create or replace database VHOL_ST;
grant all privileges on database VHOL_ST to role VHOL;
use database VHOL_ST;
use schema PUBLIC;
use warehouse VHOL_WH;


create or replace stage VHOL_STAGE
FILE_FORMAT = ( TYPE=JSON,STRIP_OUTER_ARRAY=TRUE );

create or replace table CC_TRANS_STAGING (RECORD_CONTENT variant);



create or replace procedure SIMULATE_KAFKA_STREAM(mystage STRING,prefix STRING,numlines INTEGER)
  RETURNS STRING
  LANGUAGE JAVA
  PACKAGES = ('com.snowflake:snowpark:latest')
  HANDLER = 'StreamDemo.run'
  AS
  $$
    import com.snowflake.snowpark_java.Session;
    import java.io.*;
    import java.util.HashMap;
    public class StreamDemo {
      public String run(Session session, String mystage,String prefix,int numlines) {
        SampleData SD=new SampleData();
        BufferedWriter bw = null;
        File f=null;
        try {
            f = File.createTempFile(prefix, ".json");
            FileWriter fw = new FileWriter(f);
	        bw = new BufferedWriter(fw);
            boolean first=true;
            bw.write("[");
            for(int i=1;i<=numlines;i++){
                if (first) first = false;
                else {bw.write(",");bw.newLine();}
                bw.write(SD.getDataLine(i));
            }
            bw.write("]");
            bw.close();
            return session.file().put(f.getAbsolutePath(),mystage,options)[0].getStatus();
        }
        catch (Exception ex){
            return ex.getMessage();
        }
        finally {
            try{
	            if(bw!=null) bw.close();
                if(f!=null && f.exists()) f.delete();
	        }
            catch(Exception ex){
	            return ("Error in closing:  "+ex);
	        }
        }
      }
      
      private static final HashMap<String,String> options = new HashMap<String, String>() {
        { put("AUTO_COMPRESS", "TRUE"); }
      };
      
      // sample data generator (credit card transactions)
    public static class SampleData {
      private static final java.util.Random R=new java.util.Random();
      private static final java.text.NumberFormat NF_AMT = java.text.NumberFormat.getInstance();
      String[] transactionType={"PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","REFUND"};
      String[] approved={"true","true","true","true","true","true","true","true","true","true","false"};
      static {
        NF_AMT.setMinimumFractionDigits(2);
        NF_AMT.setMaximumFractionDigits(2);
        NF_AMT.setGroupingUsed(false);
      }
      
      private static int randomQty(int low, int high){
        return R.nextInt(high-low) + low;
      }
      
      private static double randomAmount(int low, int high){
        return R.nextDouble()*(high-low) + low;
      }
      
      private String getDataLine(int rownum){
        StringBuilder sb = new StringBuilder()
            .append("{")
            .append("\"element\":"+rownum+",")
            .append("\"object\":\"basic-card\",")
            .append("\"transaction\":{")
            .append("\"id\":"+(1000000000 + R.nextInt(900000000))+",")
            .append("\"type\":"+"\""+transactionType[R.nextInt(transactionType.length)]+"\",")
            .append("\"amount\":"+NF_AMT.format(randomAmount(1,5000)) +",")
            .append("\"currency\":"+"\"USD\",")
            .append("\"timestamp\":\""+java.time.Instant.now()+"\",")
            .append("\"approved\":"+approved[R.nextInt(approved.length)]+"")
            .append("},")
            .append("\"card\":{")
                .append("\"number\":"+ java.lang.Math.abs(R.nextLong()) +"")
            .append("},")
            .append("\"merchant\":{")
            .append("\"id\":"+(100000000 + R.nextInt(90000000))+"")
            .append("}")
            .append("}");
        return sb.toString();
      }
    }
}
$$;

call SIMULATE_KAFKA_STREAM('@VHOL_STAGE','SNOW_',1000000);


list @VHOL_STAGE PATTERN='.*SNOW_.*';


copy into CC_TRANS_STAGING from @VHOL_STAGE PATTERN='.*SNOW_.*';


select count(*) from CC_TRANS_STAGING;
select * from CC_TRANS_STAGING limit 10;

select RECORD_CONTENT:card:number as card_id from CC_TRANS_STAGING limit 10;


create or replace view CC_TRANS_STAGING_VIEW (card_id, merchant_id, transaction_id, amount, currency, approved, type, timestamp ) as (
select
RECORD_CONTENT:card:number::varchar card_id,
RECORD_CONTENT:merchant:id::varchar merchant_id,
RECORD_CONTENT:transaction:id::varchar transaction_id,
RECORD_CONTENT:transaction:amount::float amount,
RECORD_CONTENT:transaction:currency::varchar currency,
RECORD_CONTENT:transaction:approved::boolean approved,
RECORD_CONTENT:transaction:type::varchar type,
RECORD_CONTENT:transaction:timestamp::datetime timestamp
from CC_TRANS_STAGING);

alter table CC_TRANS_STAGING set CHANGE_TRACKING = true;
alter view CC_TRANS_STAGING_VIEW set CHANGE_TRACKING = true;


create or replace stream CC_TRANS_STAGING_VIEW_STREAM on view CC_TRANS_STAGING_VIEW SHOW_INITIAL_ROWS=true;
select count(*) from CC_TRANS_STAGING_VIEW_STREAM;
select * from CC_TRANS_STAGING_VIEW_STREAM limit 10;


create or replace table CC_TRANS_ALL (
card_id varchar,
merchant_id varchar,
transaction_id varchar,
amount float,
currency varchar,
approved boolean,
type varchar,
timestamp datetime)


create or replace task GENERATE_TASK
WAREHOUSE=VHOL_WH
SCHEDULE = '1 minute'
COMMENT = 'Generates simulated real-time data for ingestion'
as
call SIMULATE_KAFKA_STREAM('@VHOL_STAGE','SNOW_',1000000);


execute task GENERATE_TASK;

alter task GENERATE_TASK RESUME;


create or replace task PROCESS_FILES_TASK
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
SCHEDULE = '3 minute'
COMMENT = 'Ingests Incoming Staging Datafiles into Staging Table'
as
copy into CC_TRANS_STAGING from @VHOL_STAGE PATTERN='.*SNOW_.*';

execute task PROCESS_FILES_TASK;

alter task PROCESS_FILES_TASK resume;


create or replace task REFINE_TASK
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
SCHEDULE = '4 minute'
COMMENT = '2.  ELT Process New Transactions in Landing/Staging Table into a more Normalized/Refined Table (flattens JSON payloads)'
when
SYSTEM$STREAM_HAS_DATA('CC_TRANS_STAGING_VIEW_STREAM')
as
insert into CC_TRANS_ALL (select                     
card_id, merchant_id, transaction_id, amount, currency, approved, type, timestamp
from CC_TRANS_STAGING_VIEW_STREAM);

execute task REFINE_TASK;


CREATE OR REPLACE PROCEDURE VHOL_ST.PUBLIC.SIMULATE_KAFKA_STREAM("MYSTAGE" VARCHAR, "PREFIX" VARCHAR, "NUMLINES" NUMBER(38,0))
RETURNS VARCHAR
LANGUAGE JAVA
PACKAGES = ('com.snowflake:snowpark:1.16.0')
HANDLER = 'StreamDemo.run'
EXECUTE AS OWNER
AS '
    import com.snowflake.snowpark_java.Session;
    import java.io.*;
    import java.util.HashMap;
    public class StreamDemo {
      public String run(Session session, String mystage,String prefix,int numlines) {
        SampleData SD=new SampleData();
        BufferedWriter bw = null;
        File f=null;
        try {
            f = File.createTempFile(prefix, ".json");
            FileWriter fw = new FileWriter(f);
	        bw = new BufferedWriter(fw);
            boolean first=true;
            bw.write("[");
            for(int i=1;i<=numlines;i++){
                if (first) first = false;
                else {bw.write(",");bw.newLine();}
                bw.write(SD.getDataLine(i));
            }
            bw.write("]");
            bw.close();
            return session.file().put(f.getAbsolutePath(),mystage,options)[0].getStatus();
        }
        catch (Exception ex){
            return ex.getMessage();
        }
        finally {
            try{
	            if(bw!=null) bw.close();
                if(f!=null && f.exists()) f.delete();
	        }
            catch(Exception ex){
	            return ("Error in closing:  "+ex);
	        }
        }
      }
      
      private static final HashMap<String,String> options = new HashMap<String, String>() {
        { put("AUTO_COMPRESS", "TRUE"); }
      };
      
      // sample data generator (credit card transactions)
    public static class SampleData {
      private static final java.util.Random R=new java.util.Random();
      private static final java.text.NumberFormat NF_AMT = java.text.NumberFormat.getInstance();
      String[] transactionType={"PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","REFUND"};
      String[] approved={"true","true","true","true","true","true","true","true","true","true","false"};
      static {
        NF_AMT.setMinimumFractionDigits(2);
        NF_AMT.setMaximumFractionDigits(2);
        NF_AMT.setGroupingUsed(false);
      }
      
      private static int randomQty(int low, int high){
        return R.nextInt(high-low) + low;
      }
      
      private static double randomAmount(int low, int high){
        return R.nextDouble()*(high-low) + low;
      }
      
      private String getDataLine(int rownum){
        StringBuilder sb = new StringBuilder()
            .append("{")
            .append("\\"element\\":"+rownum+",")
            .append("\\"object\\":\\"basic-card\\",")
            .append("\\"transaction\\":{")
            .append("\\"id\\":"+(1000000000 + R.nextInt(900000000))+",")
            .append("\\"type\\":"+"\\""+transactionType[R.nextInt(transactionType.length)]+"\\",")
            .append("\\"amount\\":"+NF_AMT.format(randomAmount(1,5000)) +",")
            .append("\\"currency\\":"+"\\"USD\\",")
            .append("\\"timestamp\\":\\""+java.time.Instant.now()+"\\",")
            .append("\\"approved\\":"+approved[R.nextInt(approved.length)]+"")
            .append("},")
            .append("\\"card\\":{")
                .append("\\"number\\":"+ java.lang.Math.abs(R.nextLong()) +"")
            .append("},")
            .append("\\"merchant\\":{")
            .append("\\"id\\":"+(100000000 + R.nextInt(90000000))+"")
            .append("}")
            .append("}");
        return sb.toString();
      }
    }
}
';

create or replace task VHOL_ST.PUBLIC.PROCESS_FILES_TASK
	schedule='3 minute'
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	COMMENT='Ingests Incoming Staging Datafiles into Staging Table'
	as copy into CC_TRANS_STAGING from @VHOL_STAGE PATTERN='.*SNOW_.*';

    create or replace task VHOL_ST.PUBLIC.REFINE_TASK
	schedule='4 minute'
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	COMMENT='2.  ELT Process New Transactions in Landing/Staging Table into a more Normalized/Refined Table (flattens JSON payloads)'
	when SYSTEM$STREAM_HAS_DATA('CC_TRANS_STAGING_VIEW_STREAM')
	as insert into CC_TRANS_ALL (select                     
card_id, merchant_id, transaction_id, amount, currency, approved, type, timestamp
from CC_TRANS_STAGING_VIEW_STREAM);

create or replace task VHOL_ST.PUBLIC.REFINE_TASK
	schedule='4 minute'
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	COMMENT='2.  ELT Process New Transactions in Landing/Staging Table into a more Normalized/Refined Table (flattens JSON payloads)'
	when SYSTEM$STREAM_HAS_DATA('CC_TRANS_STAGING_VIEW_STREAM')
	as insert into CC_TRANS_ALL (select                     
card_id, merchant_id, transaction_id, amount, currency, approved, type, timestamp
from CC_TRANS_STAGING_VIEW_STREAM);