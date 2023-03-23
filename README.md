This is an experimental back-end for the Wikidata Query Service.
It works similar to Virtuoso, storing triples in MySQL.
The gimmick here is the automated management of many tables; a triple ITEM-P214-STRING is mapped to a table containing only the ITEM ID and the STRING, without having to store the property, as that is part of the table name.
MySQL is able to many tens of thousands of tables.
Inserting and deleting triples is as fast as MySQL INSERT/DELETE.
Querying is as fast as MySQL SELECT.
Read-only MySQL replicas can be scaled to handle read traffic, as is already done for Wikipedia etc.

# Speed test
Reading the 108K triple test set from local file, writing to a database on Toolforge via fast internet.

## De novo with spawns
This will create ~1400 tables in the database. This is slow, but will only happen once initially.
```
target/release/wdqsbe  3.59s user 1.85s system 1% cpu 7:55.70 total
```

## With tables and data
Importing the same data again. Tables and rows exists, `INSERT IGNORE` is run but will skip actual changes.
```
target/release/wdqsbe  1.74s user 1.51s system 6% cpu 48.413 total
```

## Without INSERT (just compute, with minimal DB access)
Commented out the actual `INSERT IGNORE` command. This will only read once from the database, and not touch it after that, but all processing will be done.
```
target/release/wdqsbe  1.17s user 1.26s system 96% cpu 2.517 total
```
