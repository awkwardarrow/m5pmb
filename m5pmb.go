package main
import (
	"fmt"
	"os"
	"os/exec"
	"io/ioutil"
	"bufio"
	"strings"
	"strconv"
	"time"
	"log"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/cheggaaa/pb"
)

const g_ConnectString = "user:password@tcp(127.0.0.1:3306)/test"
const g_fakeRun = false
const g_verbose = true
const g_verboseLevel = 1
const g_concurrency = 8
const g_separ = string(os.PathSeparator)
const g_import = g_separ +"import"
const g_data   = g_separ +"data"
const g_log    = g_separ +"log"


var Trace *log.Logger

type tablePart struct {
	schema string
	name string
	pk string
	from,to string
	partSeq int // number used when table comes in several parts
}


// global variables from input to backup functions
var g_dirs = []string {}
var g_myisam_tables = []tablePart {}
var g_innodb_tables = []tablePart {}


func (t *tablePart) BuildOutFileSql(filename string) string {
	sql := fmt.Sprintf("select * from `%s`.`%s` ", t.schema, t.name)
	
	if t.pk == "where" {
		sql += "where " + t.from
	} else if t.pk != "" {
		if t.to != "" && t.from != "" {
			sql += fmt.Sprintf(" where `%s` > %s and `%s` <= %s ", t.pk, t.from, t.pk, t.to)
		} else if t.from == "" {
			sql += fmt.Sprintf(" where `%s` <= %s ", t.pk, t.to)
		} else if t.to == "" {
			sql += fmt.Sprintf(" where `%s` > %s ", t.pk, t.from)
		}
	}
	// make sure to use Unix path separator in SQL on Windows
	if g_separ != "/" {
		filename = strings.Replace(filename,"\\","/",-1)
	}				

	sql += fmt.Sprintf(" into outfile '%s' ", filename)
	return sql
}


func checkErr(err error, format string, args ...interface{}) {
	if err!=nil {
		Trace.Printf(format + " error:", args...)
		Trace.Print(err)
		fmt.Fprintln(os.Stderr, "Got error, refer trace log")
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func checkWarn(err error, format string, args ...interface{}) {
	if err!=nil {
		Trace.Printf(format + " error:", args...)
		Trace.Print(err)
	}
}

func FileCreateXX(name string, id int) *os.File {
	if g_fakeRun || g_verbose {
		if g_verboseLevel>1 {
			Trace.Printf("W%d: Create: %s", id, name)
		}
	}

	if g_fakeRun {
		return nil
	} else {
		f, err := os.Create(name)
		checkErr(err, "W%d: Create {%s}:", id, name)
		if g_verboseLevel>1 {
			Trace.Printf("W%d: Created File %s", id, strconv.FormatInt(int64(f.Fd()), 16))
		}
		return f
	}
}

func FileWriteXX(f *os.File, s string, id int) {
	msg := "W%d: FileWrite: %s"
	if f != nil {
		msg = strconv.FormatInt(int64(f.Fd()), 16) +": "+ msg
	}
	if g_fakeRun || g_verbose {
		if g_verboseLevel>1 {
			Trace.Printf(msg, id, s)
		}
	}

	if g_fakeRun {
		return
	} else {
		_, err := f.Write([]byte(s))
		checkErr(err, msg, id, s)
		return
	}
}

func FileCloseXX(f *os.File, id int)  {
	msg := "W%d: FileClose "
	if f != nil {
		msg = strconv.FormatInt(int64(f.Fd()), 16) +": "+ msg
	}
	if g_fakeRun || g_verbose {
		if g_verboseLevel>1 {
			Trace.Printf(msg, id)
		}
	}

	if g_fakeRun {
		return
	} else {
		err := f.Sync()
		checkErr(err, msg, id)
		err = f.Close()
		checkErr(err, msg, id)
		return
	}
}

func WriteFileXX(name, s string, id int) {
	msg := "W%d WriteFile: %s %s"
	if g_fakeRun || g_verbose {
		if g_verboseLevel>1 {
			Trace.Printf(msg, id, name, s)
		}
	}

	if g_fakeRun {
		return
	} else {
		err := ioutil.WriteFile(name, []byte(s), 0644)
		checkErr(err, msg, id, name, s)
		return
	}
}


func OpenXX(id int) *sql.DB {
	if g_fakeRun || g_verbose {
		Trace.Printf("W%d: Open: %s", id, g_ConnectString)
	}

	if g_fakeRun {
		return nil
	} else {
		r, err := sql.Open("mysql", g_ConnectString)
		checkErr(err, "W%d: Open {%s}:", id, g_ConnectString)
		return r
	}
}

func CloseXX(con *sql.DB, id int) {
	if g_fakeRun || g_verbose {
		Trace.Printf("W%d: Close", id)
	}

	if !g_fakeRun {
		checkWarn( con.Close(), "W%d Close", id )
	}
}

func ExecXX(con *sql.DB, sql string, id int) {
	if g_fakeRun || g_verbose {
		Trace.Printf("W%d: %s", id, sql)
	}

	if !g_fakeRun {
		_, err:= con.Exec(sql)
		checkErr(err, "W%d: Sql {%s}", id, sql)
	}
}

func QueryXX(con *sql.DB, sql string, id int) *sql.Rows {
	if g_fakeRun || g_verbose {
		Trace.Printf("W%d: %s", id, sql)
	}

	if g_fakeRun {
		return nil
	} else {
		r, err := con.Query(sql)
		checkErr(err, "W%d: Sql {%s}", id, sql)
		return r
	}
}

func Query2XX(con *sql.DB, sql string, id int) string {
	if g_fakeRun || g_verbose {
		Trace.Printf("W%d: %s", id, sql)
	}

	if g_fakeRun {
		return ""
	} else {
		rows,err := con.Query(sql)
		defer rows.Close()
		checkErr(err, "W%d: Sql {%s}", id, sql)

		var x1,res  string

		for rows.Next() {
			err := rows.Scan(&x1, &res)
			checkErr(err, "row.Scan()")
		}

		return res
	}
}


func NewWork(t tablePart) *tablePart {
	return &tablePart{t.schema, t.name, t.pk, t.from, t.to, t.partSeq}
}

func Worker(id int, wd,prefix string, pending chan *tablePart, started, progress, done chan *int) {
	con := OpenXX(id)
	flog  := FileCreateXX(wd + g_import +g_separ+ prefix+ strconv.Itoa(id) +".sql" , id)

	if started != nil {
		ExecXX(con, "set tx_isolation='repeatable-read'", id)
		ExecXX(con, "start transaction", id)
		started <- nil
	}

	for {
		t := <- pending
		
		if t == nil {
			CloseXX(con, id)
			FileCloseXX(flog, id)
			done <- nil
			break
		}

		outfile := wd + g_data + g_separ + t.schema + g_separ + t.name

		if prefix != "DDL" {
			if t.pk != "" {
				 outfile += ".part" + strconv.Itoa(t.partSeq)
			}
			outfile += ".dat"
			FileWriteXX(flog, fmt.Sprintf("load data infile '%s' into table `%s`.`%s`;\n", outfile, t.schema, t.name), id)

			sql := t.BuildOutFileSql(outfile)
			ExecXX(con, sql, id)
		} else if t.partSeq == 0 {
			outfile += "__ddl.sql"

			createTable := Query2XX(con, fmt.Sprintf("show create table `%s`.`%s`", t.schema, t.name), id)
			createTable = fmt.Sprintf("use `%s`; %s;", t.schema, createTable)

			FileWriteXX(flog, fmt.Sprintf("source %s;\n", outfile), id)
			WriteFileXX(outfile, createTable, id)
		}

		progress <- nil
	}
}


func backup_tables(wd, engine string, tables []tablePart, after_start_callback *func()) {
	if len(tables) == 0 {
		return
	}
	pending := make(chan *tablePart)
	progress := make(chan *int)
	done     := make(chan *int)

	// we use this channel only if after_start_callback is provided
	var started chan *int = nil

	if after_start_callback != nil {
		started = make(chan *int)
	}

	workerCount := g_concurrency
	if workerCount > len(tables) {
		workerCount = len(tables)
	}

	go func() {
		for _, t := range tables {
			pending <- NewWork(t)
		}
		// send nil into each worker to signal close
		for i:=0; i < workerCount; i++ {
			pending <- nil
		}
	} ()

	for i := 0; i < workerCount; i++ {
		go Worker(i, wd, engine, pending, started, progress, done)
	}

	if after_start_callback != nil {
		fmt.Println("Starting InnoDB transactions:")
		bar := pb.StartNew(workerCount)

		for i := 0; i < workerCount; i++ {
			<-started
			bar.Increment()
		}
		bar.Update()
		fmt.Print("Unlocking tables...")
		(*after_start_callback)()
		close(started)
		fmt.Println(" ...done")
	}

	fmt.Println("Dumping "+ engine +" tables:")

	bar := pb.StartNew(len(tables))

	for i := 0; i < len(tables); i++ {
		<-progress
		bar.Increment()
	}
	bar.Update()
	close(progress)

	for i := 0; i < workerCount; i++ {
		<-done
	}
	close(pending)
	close(done)
}

func backupddl(wd string) {
	backup_tables(wd, "DDL", append(g_myisam_tables, g_innodb_tables...), nil)
}

func backupmyisam(wd string) {
	backup_tables(wd, "MyISAM", g_myisam_tables, nil)
}

// callback cb should be called after when the transactions are started (to release global lock)
func backupinnodb(wd string, cb *func()) {
	backup_tables(wd, "InnoDB", g_innodb_tables, cb)
}


func ftwrl(wd string) *sql.DB {
	fmt.Print("Locking tables...")
	con := OpenXX(-1)

	// todo with timout ?
	ExecXX(con, "FLUSH TABLES WITH READ LOCK", -1)

	rows := QueryXX(con, "SHOW MASTER STATUS", -1)

	if !g_fakeRun {
		var binfile string
		var binpos int

		defer rows.Close()

		var colCount int
		{
			cols, err:= rows.Columns()
			checkErr(err, "row.Columns()")
			colCount = len(cols)
		}

		for rows.Next() {
			var x1,x2,x3 string
			switch colCount {
				case 4: 
					err := rows.Scan(&binfile, &binpos, &x1, &x2)
					checkErr(err, "row.Scan()")
				case 5: 
					err := rows.Scan(&binfile, &binpos, &x1, &x2, &x3)
					checkErr(err, "row.Scan()")
				default:
					break
			}

			if binfile != "" {
				WriteFileXX( wd + g_import + g_separ + "binlog.txt", fmt.Sprintf("%s %d", binfile, binpos), -1 )
				Trace.Printf("Binlog : %s, %d", binfile, binpos)
			}
		}
	}
	fmt.Println(" ...done")
	return con
}


func backup(wd string) {
	mkdirs(wd)
	backupddl(wd)

	unlockconn := ftwrl(wd)

	backupmyisam(wd)

	unlock := func() {
		// TODO make sure connection is still active
		CloseXX(unlockconn, -1)
	}

	backupinnodb( wd, &unlock )
	
	Trace.Print("Backup completed successfully");
	fmt.Println("Backup completed successfully");
}

func MkdirXX(name string) {
	if g_fakeRun || g_verbose {
		Trace.Printf("Mkdir: {%s}", name)
	}
	if !g_fakeRun {
		checkErr(os.Mkdir(name, 0777), "Mkdir {%s}", name)
	}
}

func MkdirAllXX(name string) {
	if g_fakeRun || g_verbose {
		Trace.Printf("MkdirAll: {%s}", name)
	}
	if !g_fakeRun {
		checkErr(os.MkdirAll(name, 0777), "MkdirAll {%s}", name)
	}
}


func ChmodXX(name string) {
	if g_fakeRun || g_verbose {
		Trace.Printf("Chmod: {%s}", name)
	}
	if !g_fakeRun {
		checkWarn(os.Chmod(name, 0777), "Chmod {%s}", name)
	}
}

func ChdirXX(dir string) {
	if g_fakeRun || g_verbose {
		Trace.Printf("Chdir: {%s}", dir)
	}
	if !g_fakeRun {
		checkErr(os.Chdir(dir), "Chdir {%s}", dir)
	}
}

func RemoveAllXX(path string) {
	if g_fakeRun || g_verbose {
		Trace.Printf("removeAll: {%s}", path)
	}
	if !g_fakeRun {
		checkErr(os.RemoveAll(path), "RemoveAll {%s}", path)
	}
}


func mkdirs(wd string) {
	fmt.Println("Creating directories:")
	bar := pb.StartNew(len(g_dirs))

	fddl  := FileCreateXX(wd + g_import +g_separ+ "databases.sql" , -1)
	defer FileCloseXX(fddl, -1)

	for _, dir := range g_dirs {
		FileWriteXX(fddl, fmt.Sprintf("create database if not exists `%s`;\n",dir), -1)
		dir = wd + g_separ +"data"+ g_separ + dir
		MkdirXX(dir)
		ChmodXX(dir)
		bar.Increment()
	}
	
	bar.Update()

}

// TODO windows
func CommandXX(cmd string) {
	if g_fakeRun || g_verbose {
		Trace.Printf("exec: {sh -c %s}", cmd)
	}

	if !g_fakeRun {
		checkErr(exec.Command("sh","-c",cmd).Run(), "exec {sh -c %s}", cmd)
	}
}


func readinput() {
	fmt.Println("Parsing input...")

	i := 0
	s := bufio.NewScanner(os.Stdin)

	dbmap := make(map[string]int)
	nextInnodb := true

	var prev_schema, prev_name string
	partSeq := 0

	for s.Scan() {
		j := 0
		if strings.EqualFold(s.Text(), "InnoDB")	{
			nextInnodb = true
		} else if strings.EqualFold(s.Text(), "MyISAM")	{
			nextInnodb = false
		} else {
			s1 :=  bufio.NewScanner(strings.NewReader(s.Text()))
			s1.Split(bufio.ScanWords)

			var t tablePart

			for s1.Scan() {
				v := s1.Text();
				if v == "\"\"" {
					v = ""
				}
				switch j {
					case 0: t.schema = v
					case 1: t.name   = v
					case 2: t.pk     = v
					case 3: t.from   = v
					default:
						if t.pk == "where" {
							t.from += " "+v
						} else if j==4 {
							t.to     = v
						}
				}
				j++
			}
			if t.pk != "where" && j!=5 && j !=2 {
				Trace.Printf("Unexpected input at line %d : %d", i, j)
				os.Exit(1)
			}
			// collect unique databases
			dbmap[t.schema] = 1

			if t.name==prev_name && t.schema==prev_schema {
				partSeq++
				t.partSeq = partSeq
			} else {
				partSeq = 0
			}

			if nextInnodb {
				g_innodb_tables = append(g_innodb_tables, t)
			} else {
				g_myisam_tables = append(g_myisam_tables, t)
			}
			prev_schema=t.schema
			prev_name=t.name

		}
		i++
	}

	// mysql database should be first, so handle it specially
	var mysqldb_present bool = false
	for k, _ := range dbmap {
		if k == "mysql" {
			mysqldb_present = true
		} else {
			g_dirs = append(g_dirs, k)	
		}
	}

	if mysqldb_present {
		g_dirs = append([]string{"mysql"}, g_dirs...)
	}
}

func main() {
	Trace = log.New(os.Stderr, "", log.Ldate|log.Ltime)
	readinput()

	if !g_fakeRun {
		Trace = log.New(ioutil.Discard, "", log.Ldate|log.Ltime)
	} else {
		Trace = log.New(os.Stdout, "", log.Ldate|log.Ltime)
	}

	now := time.Now()
	rootDir := fmt.Sprintf("%02d%02d%02d%02d%02d%02d", now.Year()-2000, now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second())
	
	MkdirAllXX(rootDir + g_separ + g_log)

	wd, err := os.Getwd()
	if err != nil {
			log.Print("Getwd returned error:")
			log.Print(err)
			os.Exit(1)
	}

	wd +=  g_separ + rootDir

	if !g_fakeRun {
		traceFileName := wd + g_separ + g_log + g_separ + "trace.log"
		file, err := os.OpenFile(traceFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalln("Failed to open log file", traceFileName, ":", err)
		}
		Trace = log.New(file, "", log.Ldate|log.Ltime) 
	}

	
	MkdirAllXX(rootDir + g_separ + g_data)
	ChmodXX(rootDir + g_separ + g_data)

	MkdirAllXX(rootDir + g_separ + g_import)

	backup(wd)
}