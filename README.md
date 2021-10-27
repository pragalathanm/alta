In a nutshell `Alta` is used to duplicate a MySQL table. The inherent idempotency nature of `Alta` comes in handy when you want to alter a very large table in MySQL.
#How does it work?
You need to create the target table with all the necessary `ALTER TABLE` commands. 
The target table should be of the name `_sourceTable`
Then you can use the following command to copy records from `sourceTable` to `_sourceTable` (note the underscore prefix).

```alta-1.0 -t <table-name>```
Once copied you can use MySQL `rename table`[command](https://dev.mysql.com/doc/refman/8.0/en/rename-table.html) to swap the tables.

**Alta is idempotent. You can run it multiple times to copy the data in multiple iterations**

The full list of parameters
```
Usage: alta-1.0 [-c=<chunkSize>] [-e=<endId>] [-s=<startId>] [-t=<tableName>]
  -c, --chunk-size=<chunkSize> the chunk of records that needs to be copied in one go
  -e, --end-id=<endId>         ID of the recotd to copy till
  -s, --start-id=<startId>     ID of the record to copy from
  -t, --table=<tableName>      the name of the table
```

How to pass MySQL credentials?
```
-Dhost : hostname of mysql server, defaults to 'localhost'
-Dport : port of mysql server, defaults to 3306
-Ddatabase : database/scehma
-Dusername : mysql username
-Dpassword : mysql password
```
If you need precise control over the DB url, you can use the following parameter
```-Dquarkus.datasource.jdbc.url```; in that case, parameters `host`, `port` and `database` will be ignored. 

E.g. if you want to duplicate employees table of company database from localhost mysql server
```
alta-1.0 -t employees -Dhost=localhost -Dport=3306 -Ddatabase=company -c 1024
```
The chunk size determines how many records has to be fetched and copied to the target table in one go.
Depending on your production hardware you need to fine tune the `chunkSize` parameter to improve performance.

###What if you want to copy only range of records, for example, for archival?
You can use `--start-id` and `--end-id` parameters.
#How to build?
```./mvnw package -Pnative -Dquarkus.native.container-build=true```

#Limitation
- Alta supports only MySQL for now
- Alta can operate only on tables with simple numeric primary keys