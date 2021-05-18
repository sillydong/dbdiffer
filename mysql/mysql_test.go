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
	tb, tbp, err := tables(db, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v\n", tb)
	t.Logf("%+v\n", tbp)
}

func TestFields(t *testing.T) {
	fids, fidsp, err := fields(db, "redispatch")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v\n", fids)
	t.Logf("%+v\n", fidsp)
}

func TestIndexes(t *testing.T) {
	idxs, idxsp, err := indexes(db, "redispatch_item")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v\n", idxs)
	t.Logf("%+v", idxsp)
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
