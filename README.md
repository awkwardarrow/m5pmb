# Parallel MySQL backup

Create consistent logical backup of MyISAM and InnoDB tables.

## Installation

```
go get github.com/awkwardarrow/m5pmb
```

## Dependencies

```
go get github.com/go-sql-driver/mysql
go get github.com/cheggaaa/pb
```

## License

 *** Pending license approval

## Usage 

```shell
> ./m5pmb < file_with_tables.txt
```

In result, new directory in current folder will be created with .sql and .dat files needed to restore the tables.
Use ./m5metapart > file_with_tables.txt script to generate sample input file from your database


E.g. following output was generated to create backup of ~500M MySQL Enterprise Monitor instance

```shell
> time /common/m5metapart | /common/m5pmb 
Parsing input...
Creating directories:
12 / 12 [=====================================] 100.00 % 0
Dumping DDL tables:
335 / 335 [===================================] 100.00 % 0
Locking tables... ...done
Dumping MyISAM tables:
28 / 28 [=====================================] 100.00 % 0
Starting InnoDB transactions:
8 / 8 [=======================================] 100.00 % 0
Unlocking tables... ...done
Dumping InnoDB tables:
307 / 307 [===================================] 100.00 % 0
Backup completed successfully

real	0m4.425s
```

Just try in your test server and refer log about exact actions taken

## Limitations which may be addressed later

  * The tool uses SELECT ... INTO OUTFILE commands, which causes corresponding limitations
  * No configuration parameters, so modify the script for e.g.:
connection details, concurrency, verbosity, destination, ignored databases, etc
  * Triggers are not included into backup

## Features and Advantages

  * Much faster than mysqldump --all-databases when data read is not bottleneck
  * Consistent backup with reliable binlog position
  * Automatically balances work between worker threads
  * Tries to split huge tables into smaller chunks to simplify further restore

## Typical usage scenario

  * Generate and verify input file using m5metapart
  * Mount partition where 'mysql' user has full write access. For the fastest result it should be on dedicated phycial device
  * Run m5pmb

## Comparison to mydumper

m5pmb is somewhat lighter version of mydumper. Key differences:
  * m5pmb uses SELECT * INTO OUTFILE, which puts some limitations, but should be much faster (when data reads is not bottleneck)
  * Currently m5pmb should handle huge Partitioned tables better. (Tries to dump each partition separately)
  * m5pmb is split into two phases for getting list of tables (m5metapart) and actual backup (m5pmb). So it is possible to review / customize list of processed tables. mydumper uses regular expressions for that
  * m5pmb creates raw data files, while mydumper generates INSERT sql commands
  * Currently m5pmb doesn't work around slow queries before issueing FLUSH TABLES WITH READ LOCK
  * Currently mydumper doesn't supprot backup of MyISAM
  