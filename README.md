# dbt_poc
dbt (Data Build Tool) is a SQL-first transformation workflow that lets teams quickly and collaboratively deploy analytics code following software engineering best practices like modularity, portability, CI/CD, and documentation.

dbt is designed to handle the transformation layer (the silver and gold layers of our medallion architecture) of the ‘extract-load-transform’ framework for data platforms (in our case Databricks). dbt creates a connection to a data platform and runs SQL code against the warehouse (Lakehouse in our case) to transform data. 

The scope of this document is to conduct a POC on whether this tool can help our analytics engineering teams boost their productivity, provide a better developer experience and enhance the war we maintain our pipelines.
