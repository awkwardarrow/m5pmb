package main
import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"log"
	"regexp"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

var g_ConnectString = "root:1@tcp(127.0.0.1:3306)/test"
var g_verbose = true
var g_sizeThreashold = 16*1024*1024

var g_dirs = []string {}

var g_ignored_schemas = map[string]bool {
	"information_schema":true,
	"performance_schema":true,
}

var g_ignored_tables = map[string]map[string]bool {
	"mysql":map[string]bool { 
		"innodb_index_stats":true,
		"innodb_table_stats":true,
		"slow_log":true,
		"general_log":true,
		"ndb_binlog_index":true,
		"slave_master_info":true,
		"slave_relay_log_info":true,
		"slave_worker_info":true,
	},
}


var g_processed_engines = map[string]bool {}


func l(s string) {
	if g_verbose {
		log.Print(s)
	}
}

func ln(s string) {
	if g_verbose {
		log.Println(s)
	}
}

func QueryXX(con *sql.DB, query string, args ...interface{}) *sql.Rows {
	rows, err := con.Query(query, args...)

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	return rows
}

func ScanXX(rows *sql.Rows, args ...interface{}) {
	for rows.Next() {
		err := rows.Scan(args...)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Scan returned error:")
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}
}

func ReadRowsEst(rows *sql.Rows) int64 {
	var colCount int
	{
		cols, err:= rows.Columns()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		colCount = len(cols)
	}
	var r int64

	for rows.Next() {
		var x1,x2,x3,x4,x5,x6,x7,x8,x9 sql.NullString
		switch colCount {
				case 10: 
					err := rows.Scan(&x1, &x2, &x3, &x4, &x5, &x6, &x7, &x8, &r, &x9)
					if err != nil {
						fmt.Fprintln(os.Stderr, "Scan returned error:")
						fmt.Fprintln(os.Stderr, err)
						os.Exit(2)
					}
				default:
						fmt.Fprintln(os.Stderr, "Unexpected EXPLAIN format %d columns", colCount)
						os.Exit(2)
		}
	}
	return r
}

// ugly trick to simplify handling recursion
var g_Min, g_Max float64

func build_metadata_from_huge_table_num_pk(d,name,column string, partsNeeded int, left, right float64, con *sql.DB) {
	middle := (left+right)/2

	sMin := strconv.FormatFloat(left, 'f', 2, 64)
	sAvg := strconv.FormatFloat(middle, 'f', 2, 64)
	sMax := strconv.FormatFloat(right, 'f', 2, 64)

	if partsNeeded < 2 {
		if left == g_Min {
			sMin = "\"\""
		}
		if right == g_Max {
			sMax = "\"\""
		}

		fmt.Println(d +" "+ name +" "+ column +" "+ sMin +" "+ sMax )
		return
	}

	var rowEstimLeft, rowEstimRight int64

	{
		rows := QueryXX(con, "explain select * from `"+ d  +"`.`"+ name +"` where `"+ column +"` between "+ sMin +" and " + sAvg )
		defer rows.Close()
		rowEstimLeft = ReadRowsEst(rows)
	}

	{
		rows := QueryXX(con, "explain select * from `"+ d  +"`.`"+ name +"` where `"+ column +"` between "+ sAvg +" and " + sMax )
		defer rows.Close()
		rowEstimRight = ReadRowsEst(rows)
	}

	var splitLeft, splitRight int

	splitRight = int(int64(partsNeeded)*rowEstimRight/(rowEstimRight+rowEstimLeft))
	if splitRight < 1 {
		splitRight = 1
	}
	splitLeft = partsNeeded-splitRight

	build_metadata_from_huge_table_num_pk(d,name,column, splitLeft, left, middle, con)
	build_metadata_from_huge_table_num_pk(d,name,column, splitRight, middle, right, con)
}

func build_metadata_from_huge_table(d,name string, rowCount, size int, con *sql.DB) {
	var column, data_type string
	{
		rows := QueryXX(con, "select column_name, data_type from information_schema.statistics join information_schema.columns using(table_schema, table_name, column_name) where index_name='primary' and seq_in_index=1 and table_schema = ? and table_name = ? order by cardinality desc limit 1", d, name)
		defer rows.Close()
		ScanXX(rows, &column, &data_type)
	}

	partCount := size/g_sizeThreashold

	switch data_type {
		case "int", "tinyint", "bigint", "decimal", "double", "smalint", "float":
			{
				rows := QueryXX(con, "select min(`"+ column +"`), max(`"+ column +"`) from `"+ d  +"`.`"+ name +"`")
				defer rows.Close()
				ScanXX(rows, &g_Min, &g_Max)
			}
			ln("Estimate key ranges for "+ name)
			build_metadata_from_huge_table_num_pk(d,name,column, partCount, g_Min, g_Max, con)
		default:
			fmt.Fprintln(os.Stderr, "Warning: cannot split big table %s %s", d, name)
			fmt.Println(d +" "+ name)
	}
}

/* 
Function: parse_partition_expression
Typical input :
CREATE TABLE `trb3` (
  `id` int(11) default NULL,
  `name` varchar(50) default NULL,
  `purchased` date default NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1
PARTITION BY RANGE (YEAR(purchased)) (
  PARTITION p0 VALUES LESS THAN (1990) ENGINE = MyISAM,
  PARTITION p1 VALUES LESS THAN (1995) ENGINE = MyISAM,
  PARTITION p2 VALUES LESS THAN (2000) ENGINE = MyISAM,
  PARTITION p3 VALUES LESS THAN (2005) ENGINE = MyISAM
)
Typical output - pair:
" < "  -- i.e. converts "LESS THAN" into corresponding SQL operation
[]{"(1990)","(1995)","(2000)","(2005)",} -- i.e. parse partition's values
If not able to parse - just return empty values
*/
func parse_partition_expression(create_sql string) (string, []string) {
	var expr string
	re := regexp.MustCompile(`\(.*\)`)
	var values []string
	for _, s := range strings.Split(create_sql, "\n") {
		if strings.HasPrefix(s, "(PARTITION ") || strings.HasPrefix(s, " PARTITION ") {

			if expr == "" {
				if (strings.Contains(s, " VALUES LESS THAN (")) {
					expr = " < "
				} else if (strings.Contains(s, " VALUES IN (")) {
					expr = " in "
				}
			}
			v := re.FindString(s)
			if v!="" {
				// handle values like "(()" and "())"
				v1 := re.FindString(strings.TrimLeft(v,"("))
				if v1!="" {
					v = v1
				}
				v1 = re.FindString(strings.TrimRight(v,")"))
				if v1!="" {
					v = v1
				}

				values = append(values, v)
			}
		}
	}
	return expr, values
}


func build_metadata_from_huge_partitioned_table(d, name, partition_method, partition_expression string, con *sql.DB) {
	ln("Parse partitions for "+ name)

	// try to parse create table to get expression from part
	var create_table_sql, x1 string
	{
		rows := QueryXX(con, fmt.Sprintf("show create table `%s`.`%s`", d, name))
		defer rows.Close()
		ScanXX(rows, &x1, &create_table_sql)
	}

	condition_expression, values := parse_partition_expression(create_table_sql)

	if condition_expression == "" || len(values) == 0 {
		fmt.Fprintln(os.Stderr, "Warning: cannot parse partitions for big table " + name)
		fmt.Println(d +" "+ name)
	} else if condition_expression == " < " {
		var last_value string
		for i, v := range values {
			if i==0 {
				fmt.Println(d +" "+ name + " where " + partition_expression + condition_expression + v)
			} else if i==len(values)-1 {
				fmt.Println(d +" "+ name + " where " + partition_expression + " >= " + last_value)
			} else {
				fmt.Println(d +" "+ name + " where " + partition_expression + condition_expression + v + " and "+ partition_expression + " >= " + last_value)
			}
			last_value = v
		}
	} else {
		for _, v := range values {
			fmt.Println(d +" "+ name + " where " + partition_expression + condition_expression + v)
		}
	}
}

func build_metadata_from_database(d, engine string, con *sql.DB) {
	l("Query schema "+ d + " ...")

	rows, err := con.Query("select table_name, table_rows, data_length from information_schema.tables where table_schema = ? and engine = ?", d, engine)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	defer rows.Close()

	for rows.Next() {
		var name string
		var rowCount, size int
		err = rows.Scan(&name, &rowCount, &size)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Scan returned error:")
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}

		ignored_db, ok := g_ignored_tables[d]
		if ok {
			ignored, ok := ignored_db[name]
			if ok && ignored {
				continue
			}
		}

		processed, ok := g_processed_engines[engine]
		if !ok || !processed {
			fmt.Println(engine)
			g_processed_engines[engine]=true
		}

		if size < g_sizeThreashold {
			fmt.Println(d +" "+ name)
		} else {

			// check whether huge table is partitioned and handle accordingly
			// TODO test subpartitions
			var partition_method, partition_expression string
			{
				rows := QueryXX(con, "select partition_method, partition_expression from information_schema.partitions where partition_method is not null and table_schema = ? and table_name = ? limit 1", d, name)
				defer rows.Close()
				ScanXX(rows, &partition_method, &partition_expression)
			}

			if partition_method != "" {
				build_metadata_from_huge_partitioned_table(d, name, partition_method, partition_expression, con)
			} else {
				build_metadata_from_huge_table(d, name, rowCount, size, con)
			}
		}
	}
}

func build_metadata() {
	l("Connecting mysql... ")
	con, err := sql.Open("mysql", g_ConnectString)

	if err != nil {
		fmt.Fprintln(os.Stderr,"sql.Open {%s} returned error:", g_ConnectString)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	l("Collecting databases... ")
	var rows *sql.Rows
	rows, err = con.Query("show databases")
	if err != nil {
		fmt.Fprintln(os.Stderr, "error: ", "show databases")
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	defer rows.Close()

	for rows.Next() {
		var db string
		err := rows.Scan(&db)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Scan returned error:")
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}

		ignored, ok := g_ignored_schemas[db]
		if !ok || !ignored {
			g_dirs = append(g_dirs, db)
		}
	}

	err = rows.Err()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Db error:")
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	ln("Reading MyISAM tables:")
	for _, d := range g_dirs {
		build_metadata_from_database(d, "MyISAM", con)
	}

	ln("Reading InnoDB tables:")
	for _, d := range g_dirs {
		build_metadata_from_database(d, "InnoDB", con)
	}
}

func main() {
	build_metadata()
}