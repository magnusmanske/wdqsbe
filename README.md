This is an experimental back-end for the Wikidata Query Service.
It works similar to Virtuoso, storing triples in MySQL.
The gimmick here is the automated management of many tables; a triple ITEM-P214-STRING is mapped to a table containing only the ITEM ID and the STRING, without having to store the property, as that is part of the table name.
MySQL is able to many tens of thousands of tables.
Inserting and deleting triples is as fast as MySQL INSERT/DELETE.
Querying is as fast as MySQL SELECT.
Read-only MySQL replicas can be scaled to handle read traffic, as is already done for Wikipedia etc.
