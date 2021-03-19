package mysql

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"testing"

	"github.com/go-sql-driver/mysql"
)

var db *sql.DB

func TestMain(m *testing.M) {
	var err error
	parsedNewDSN, err := mysql.ParseDSN(os.Getenv("NEWDB"))
	if err != nil {
		log.Fatal(err)
	}

	parsedNewDSN.MultiStatements = true
	db, err = sql.Open("mysql", parsedNewDSN.FormatDSN())
	if err != nil {
		log.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}
	m.Run()
}

func TestTables(t *testing.T) {
	tb, err := tables(db, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", tb)
}

func TestFields(t *testing.T) {
	fids, err := fields(db, "admin")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", fids)
}

func TestIndexes(t *testing.T) {
	idxs, err := indexes(db, "admin")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", idxs)
}

func TestDiff(t *testing.T) {
	differ, err := New(os.Getenv("NEWDB"), os.Getenv("OLDDB"))
	if err != nil {
		t.Fatal(err)
	}
	res, err := differ.Diff("")
	if err != nil {
		t.Fatal(err)
	}
	sres, _ := json.MarshalIndent(res, "", "  ")
	t.Logf("%+v", string(sres))

	gen, err := differ.Generate(res)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range gen {
		t.Log(s)
	}
}
