# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG customer_demo;
# MAGIC
# MAGIC -- Insert 100 sample customers (let CUSTOMER_ID auto-generate)
# MAGIC INSERT INTO customer_bronze.customer
# MAGIC   (`FIRST_NAME`,`LAST_NAME`,`MIDDLE_NAME`,`GENDER`,
# MAGIC    `DATE_OF_BIRTH`,`EMAIL`,`PHONE`,
# MAGIC    `CREATED_DATE`,`LAST_UPDATED_DATE`,`STATUS`)
# MAGIC WITH lists AS (
# MAGIC   SELECT
# MAGIC     array('Alex','Priya','John','Emily','Noah','Sofia','Liam','Olivia','Ethan','Mia',
# MAGIC           'Aiden','Isabella','Lucas','Ava','Michael','Harper','Benjamin','Chloe','Daniel','Grace',
# MAGIC           'Elijah','Zoe','Matthew','Layla','James','Amelia','Henry','Sofia','Jack','Aria',
# MAGIC           'Wyatt','Scarlett','David','Luna','Sebastian','Nora','Julian','Victoria','Eli','Chloe',
# MAGIC           'Anthony','Hannah','Nathan','Penelope','Gabriel','Lily','Carter','Zoey','Dylan','Evelyn',
# MAGIC           'Aaron','Riley','Andrew','Avery','Christopher','Gianna','Joshua','Stella','Isaac','Nina',
# MAGIC           'Caleb','Madison','Jonathan','Leah','Thomas','Aaliyah','Hudson','Paisley','Adam','Ellie',
# MAGIC           'Joseph','Violet','Grayson','Camila','Luke','Riley','Nathaniel','Aurora','Connor','Maya',
# MAGIC           'Asher','Hazel','Charles','Savannah','Evan','Brooklyn','Parker','Addison','Ryan','Lydia',
# MAGIC           'Adrian','Elena','Mateo','Aubrey','Leo','Hannah','Jason','Paisley','Brooks','Valentina') AS fn,
# MAGIC     array('Morgan','Shah','Smith','Davis','Johnson','Gonzalez','Brown','Martinez','Wilson','Anderson',
# MAGIC           'Thomas','Taylor','Moore','Jackson','Lee','Harris','Clark','Lewis','Walker','Hall',
# MAGIC           'Allen','Young','King','Wright','Lopez','Hill','Scott','Green','Adams','Baker',
# MAGIC           'Nelson','Carter','Mitchell','Perez','Roberts','Turner','Phillips','Campbell','Parker','Evans',
# MAGIC           'Edwards','Collins','Stewart','Sanchez','Morris','Rogers','Reed','Cook','Morgan','Bell',
# MAGIC           'Bailey','Cooper','Richardson','Cox','Howard','Ward','Torres','Peterson','Gray','Ramirez',
# MAGIC           'James','Watson','Brooks','Kelly','Sanders','Price','Bennett','Wood','Barnes','Ross',
# MAGIC           'Henderson','Cole','Jenkins','Perry','Powell','Long','Patterson','Hughes','Flores','Washington',
# MAGIC           'Butler','Simmons','Foster','Graham','Alexander','Bryant','Russell','Griffin','Diaz','Hayes',
# MAGIC           'Myers','Ford','Hamilton','Gibson','Mcdonald','Porter','Hunter','Harrison','Grant','Stone') AS ln,
# MAGIC     array('A.','B.','C.','D.','E.',NULL,NULL,NULL) AS mn,
# MAGIC     array('Male','Female','Other','Prefer not to say') AS genders,
# MAGIC     array('Active','Inactive','Pending','Blocked','On Hold') AS statuses
# MAGIC ),
# MAGIC base AS (
# MAGIC   -- Create 100 indexes (0..99)
# MAGIC   SELECT posexplode(sequence(1,100)) AS (idx, n) FROM values(1)
# MAGIC )
# MAGIC SELECT
# MAGIC   element_at(fn, (idx % size(fn)) + 1)                                            AS FIRST_NAME,
# MAGIC   element_at(ln, (idx % size(ln)) + 1)                                            AS LAST_NAME,
# MAGIC   element_at(mn, (idx % size(mn)) + 1)                                            AS MIDDLE_NAME,
# MAGIC   element_at(genders, (idx % size(genders)) + 1)                                  AS GENDER,
# MAGIC   -- Birthdays between 1978-01-01 and ~2003-09-27
# MAGIC   date_add(date'1978-01-01', (idx * 93) % 9400)                                   AS DATE_OF_BIRTH,
# MAGIC   lower(concat(substr(element_at(fn,(idx % size(fn))+1),1,1),
# MAGIC                element_at(ln,(idx % size(ln))+1),
# MAGIC                cast((idx+1) as string),'@example.com'))                           AS EMAIL,
# MAGIC   concat('+1', lpad(cast(2145550100 + idx as string),10,'0'))                     AS PHONE,
# MAGIC   -- Created/Updated dates in recent windows
# MAGIC   date_add(date'2024-01-01', (idx * 7) % 200)                                     AS CREATED_DATE,
# MAGIC   date_add(date'2024-06-01', (idx * 5) % 200)                                     AS LAST_UPDATED_DATE,
# MAGIC   element_at(statuses, (idx % size(statuses)) + 1)                                AS STATUS
# MAGIC FROM base, lists;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG customer_demo;
# MAGIC
# MAGIC -- Insert 100 sample addresses
# MAGIC INSERT INTO customer_bronze.customer_addresses
# MAGIC   (`CUSTOMER_ID`, `ADDRESS_TYPE`, `ADDRESS_LINE_1`, `ADDRESS_LINE_2`,
# MAGIC    `CITY`, `STATE`, `POSTAL_CODE`, `COUNTRY`, `IS_PRIMARY`)
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     id AS CUSTOMER_ID,
# MAGIC     CASE WHEN RAND() < 0.5 THEN 'Home' ELSE 'Work' END AS ADDRESS_TYPE,
# MAGIC     CONCAT(
# MAGIC       CAST(FLOOR(RAND() * 9999) + 1 AS STRING), ' ',
# MAGIC       element_at(
# MAGIC         array('Main St','Maple Ave','Pine Rd','Cedar Ln','Elm St','Oak Dr',
# MAGIC               'Sunset Blvd','Highland Ave','Broadway','Lakeview Dr'),
# MAGIC         CAST(RAND() * 10 AS INT) + 1
# MAGIC       )
# MAGIC     ) AS ADDRESS_LINE_1,
# MAGIC     CASE WHEN RAND() < 0.25 THEN CONCAT('Apt ', CAST(FLOOR(RAND() * 50) + 1 AS STRING)) ELSE NULL END AS ADDRESS_LINE_2,
# MAGIC     element_at(
# MAGIC       array('Plano','Frisco','Dallas','Austin','Houston','San Antonio','Irving','McKinney','Garland','Arlington'),
# MAGIC       CAST(RAND() * 10 AS INT) + 1
# MAGIC     ) AS CITY,
# MAGIC     element_at(
# MAGIC       array('TX','CA','NY','FL','IL','WA','AZ','MA','NC','GA'),
# MAGIC       CAST(RAND() * 10 AS INT) + 1
# MAGIC     ) AS STATE,
# MAGIC     LPAD(CAST(FLOOR(RAND() * 90000) + 10000 AS STRING), 5, '0') AS POSTAL_CODE,
# MAGIC     element_at(array('USA','Canada','UK'), CAST(RAND() * 3 AS INT) + 1) AS COUNTRY,
# MAGIC     CASE WHEN RAND() < 0.7 THEN TRUE ELSE FALSE END AS IS_PRIMARY
# MAGIC   FROM (
# MAGIC     SELECT SEQUENCE(1,100) AS ids
# MAGIC   ) seq
# MAGIC   LATERAL VIEW EXPLODE(ids) t AS id
# MAGIC )
# MAGIC SELECT * FROM base;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG customer_demo;
# MAGIC
# MAGIC INSERT INTO customer_bronze.customer_contacts
# MAGIC   (`CUSTOMER_ID`, `CONTACT_TYPE`, `CONTACT_VALUE`, `PREFERRED_CONTACT_METHOD`)
# MAGIC WITH src AS (
# MAGIC   SELECT
# MAGIC     CUSTOMER_ID,
# MAGIC     EMAIL,
# MAGIC     PHONE,
# MAGIC     ROW_NUMBER() OVER (ORDER BY CUSTOMER_ID) AS rn
# MAGIC   FROM customer_bronze.customer
# MAGIC ),
# MAGIC top100 AS (
# MAGIC   SELECT CUSTOMER_ID, EMAIL, PHONE
# MAGIC   FROM src
# MAGIC   WHERE rn <= 100
# MAGIC ),
# MAGIC contacts AS (
# MAGIC   SELECT
# MAGIC     CUSTOMER_ID,
# MAGIC     CASE WHEN RAND() < 0.5 THEN 'Email' ELSE 'Phone' END AS CONTACT_TYPE,
# MAGIC     CASE
# MAGIC       WHEN RAND() < 0.5
# MAGIC         THEN LOWER(COALESCE(EMAIL, CONCAT('user', CUSTOMER_ID, '@example.com')))
# MAGIC       ELSE COALESCE(PHONE, CONCAT('+1', LPAD(CAST(2145551000 + CUSTOMER_ID AS STRING), 10, '0')))
# MAGIC     END AS CONTACT_VALUE,
# MAGIC     CASE WHEN RAND() < 0.6 THEN TRUE ELSE FALSE END AS PREFERRED_CONTACT_METHOD
# MAGIC   FROM top100
# MAGIC )
# MAGIC SELECT * FROM contacts;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG customer_demo;
# MAGIC
# MAGIC INSERT INTO customer_bronze.customer_preferences
# MAGIC   (`CUSTOMER_ID`, `PREFERENCE_TYPE`, `PREFERENCE_VALUE`, `OPTED_IN`)
# MAGIC WITH lists AS (
# MAGIC   SELECT
# MAGIC     array('Email_Marketing','SMS_Alerts','Push_Notifications','Language','Contact_Time') AS pref_types,
# MAGIC     array('Daily','Weekly','Monthly') AS email_freq,
# MAGIC     array('All','Orders','Security') AS sms_types,              -- when opted out we'll force 'None'
# MAGIC     array('All','Recommendations','Reminders') AS push_types,   -- when opted out we'll force 'None'
# MAGIC     array('EN','ES','FR','DE') AS langs,
# MAGIC     array('Morning','Afternoon','Evening') AS times
# MAGIC ),
# MAGIC customers AS (
# MAGIC   SELECT CUSTOMER_ID, ROW_NUMBER() OVER (ORDER BY CUSTOMER_ID) AS rn
# MAGIC   FROM customer_bronze.customer
# MAGIC ),
# MAGIC top100 AS (
# MAGIC   SELECT CUSTOMER_ID, rn FROM customers WHERE rn <= 100
# MAGIC ),
# MAGIC typed AS (
# MAGIC   SELECT
# MAGIC     t.CUSTOMER_ID,
# MAGIC     element_at(l.pref_types, ((t.rn - 1) % size(l.pref_types)) + 1)                        AS PREFERENCE_TYPE,
# MAGIC     -- Base value chosen per type (before opt-out handling)
# MAGIC     CASE element_at(l.pref_types, ((t.rn - 1) % size(l.pref_types)) + 1)
# MAGIC       WHEN 'Email_Marketing'   THEN element_at(l.email_freq, ((t.rn - 1) % size(l.email_freq)) + 1)
# MAGIC       WHEN 'SMS_Alerts'        THEN element_at(l.sms_types,  ((t.rn*3 - 1) % size(l.sms_types)) + 1)
# MAGIC       WHEN 'Push_Notifications'THEN element_at(l.push_types, ((t.rn*5 - 1) % size(l.push_types)) + 1)
# MAGIC       WHEN 'Language'          THEN element_at(l.langs,      ((t.rn*7 - 1) % size(l.langs)) + 1)
# MAGIC       WHEN 'Contact_Time'      THEN element_at(l.times,      ((t.rn*9 - 1) % size(l.times)) + 1)
# MAGIC     END AS BASE_VALUE,
# MAGIC     -- Whether 'None' is meaningful for this type
# MAGIC     CASE element_at(l.pref_types, ((t.rn - 1) % size(l.pref_types)) + 1)
# MAGIC       WHEN 'Email_Marketing' THEN TRUE
# MAGIC       WHEN 'SMS_Alerts' THEN TRUE
# MAGIC       WHEN 'Push_Notifications' THEN TRUE
# MAGIC       ELSE FALSE
# MAGIC     END AS CAN_BE_NONE,
# MAGIC     t.rn
# MAGIC   FROM top100 t CROSS JOIN lists l
# MAGIC ),
# MAGIC opted AS (
# MAGIC   SELECT
# MAGIC     CUSTOMER_ID,
# MAGIC     PREFERENCE_TYPE,
# MAGIC     -- 20% opt-out for channels that allow 'None'; always opted-in for Language/Contact_Time
# MAGIC     CASE WHEN CAN_BE_NONE AND (rn % 5) = 0 THEN FALSE ELSE TRUE END AS OPTED_IN,
# MAGIC     BASE_VALUE,
# MAGIC     CAN_BE_NONE
# MAGIC   FROM typed
# MAGIC )
# MAGIC SELECT
# MAGIC   CUSTOMER_ID,
# MAGIC   PREFERENCE_TYPE,
# MAGIC   CASE
# MAGIC     WHEN (NOT OPTED_IN) AND CAN_BE_NONE THEN 'None'
# MAGIC     ELSE BASE_VALUE
# MAGIC   END AS PREFERENCE_VALUE,
# MAGIC   OPTED_IN
# MAGIC FROM opted;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG customer_demo;
# MAGIC
# MAGIC INSERT INTO customer_bronze.customer_transactions
# MAGIC   (`CUSTOMER_ID`, `TRANSACTION_DATE`, `TRANSACTION_AMOUNT`, `TRANSACTION_TYPE`, `PAYMENT_METHOD`)
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     id AS CUSTOMER_ID,
# MAGIC     DATE_ADD('2023-01-01', CAST(RAND() * 600 AS INT)) AS TRANSACTION_DATE,
# MAGIC     ROUND(50 + RAND() * 950, 2) AS TRANSACTION_AMOUNT,
# MAGIC     element_at(
# MAGIC       array('Purchase', 'Refund', 'Subscription', 'Upgrade', 'Renewal'),
# MAGIC       CAST(RAND() * 5 AS INT) + 1
# MAGIC     ) AS TRANSACTION_TYPE,
# MAGIC     element_at(
# MAGIC       array('Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer', 'Gift Card'),
# MAGIC       CAST(RAND() * 5 AS INT) + 1
# MAGIC     ) AS PAYMENT_METHOD
# MAGIC   FROM (
# MAGIC     SELECT SEQUENCE(1, 100) AS ids
# MAGIC   ) seq
# MAGIC   LATERAL VIEW EXPLODE(ids) t AS id
# MAGIC )
# MAGIC SELECT * FROM base;
# MAGIC

# COMMAND ----------

