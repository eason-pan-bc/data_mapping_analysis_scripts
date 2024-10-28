# How to use the NULL Columns finder tool
- make a copy of `.evn.example` and add your secrets to your `.env`
- configure parameters in `find_null_columns.py`
- `make setup` for install all dependencies
- `make run` for running the tool
- Also support full range mode to check all rows instead of random sampling. You can enable it by tweaking the `random_sampling` config in `find_null_columns.py`
- (new feature) A feature to figure out non-null columns from tables not directly connected to EVENT_ID related tables. (Need to figure out these table names and connected column names in DbSchema first)
    - e.g. CORP_PARTY table is related to x filing, and ADDRESS table connected to CORP_PARTY table by ADDR_ID

# Data Mapping Analysis COLIN part, first step Scripts
- Make sure you have DbSchema installed (not tested in other tools), and connected to COLIN db, either DEV or TST
- This is a semi-automated process, you will need to run some verification scripts provided to verify whether a table actually related or not
- You will still need to figure out which columns are affected by yourself, this part is not included in this tool
- This tool / process is not 100% accurate, will need some human verification too

## How to use the Scripts
### Step 1: Find out all tables directly related to EVENT table with EVENT_ID
- Copy the SQL code from the `sql_scripts/script_1.sql` file to a SQL editor in DbSchema and run it.
    <br/>you will get a results like this ![alt text](screenshots/image.png)
- Run the queries in the QUERY_TO_RUN column to verify if the count is 0 or > 0, drop the tables with a 0 count
    <br/> Example output: <br/>![alt text](screenshots/image-1.png)
    - Alternatively, you can use `sql_scripts/script_2.sql` to see the actual data, remember to change the table_name and column_name

### Step 2: Find tables related to the filter-outed tables from Step 1
- Using `script_1.sql` script again, but this time, change the `table_name` on **line 12** to the table name you filtered out from Step-1, 1 table a time. 
- Repeat the process in step-1 to verify whether the count is 0 or not, to drop or keep the table

### Step 3: (Optional) Repeat Step 2, but using results from Step 2
You can do this as many times as you want, that means how many levels deeper you've digged in. From my experience, 2-levels is a good start point, and I find 3-levels can give me a good result

### Next Step: Figure out columns
- You may able to find the find_null_columns tool be helpful.


