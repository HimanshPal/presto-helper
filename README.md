# Overview

Quickly create DDL for a [Presto database](https://prestodb.io/docs/current/).

The way it works is you run the script with an S3 path as an
argument. The script generates `CREATE TABLE` and `ALTER TABLE`
statements. Execute those statements to create the database.
