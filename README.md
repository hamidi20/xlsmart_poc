# xlsmart_poc
POC ELT Project – Snowflake

**Overview**

This project demonstrates my experience in designing and implementing ELT pipelines using Snowflake as the cloud data warehouse.
The focus is on transforming raw telecom-related data (Packet Purchase, Usage, and Charging Records) into structured L1 curated tables

The process involves:
Data cleansing (handling delimiters, null values, trimming invalid characters).
Data parsing & flattening (e.g., splitting third_party_info_amt fields).
Data classification (e.g., separating Combo Purchase, Normal Purchase, and SiDompul Purchase).
Applying business rules and mapping to reference data.
Loading into target L1 layer tables with proper partitions (year, month, day)

