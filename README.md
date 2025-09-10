# xlsmart_poc
POC ELT Project – Snowflake

**Overview**

The data was initially migrated from Amazon S3 into the Snowflake stage as L1. After that, the data was transformed and loaded into L2.

This project demonstrates my experience in designing and implementing ELT pipelines using Snowflake as the cloud data warehouse.
The focus is on transforming raw telecom-related data (Packet Purchase, Usage, and Charging Records) into structured L2 curated tables

The process involves:
Data cleansing (handling delimiters, null values, trimming invalid characters).
Data parsing & flattening (e.g., splitting third_party_info_amt fields).
Data classification (e.g., separating Combo Purchase, Normal Purchase, and SiDompul Purchase).
Applying business rules and mapping to reference data.
Loading into target L2 layer tables with proper partitions (year, month, day)

Below is the flow of the process:
<img width="1384" height="752" alt="Picture1" src="https://github.com/user-attachments/assets/d70d8b2b-1cf1-4128-bbbb-8e62f01cf489" />
