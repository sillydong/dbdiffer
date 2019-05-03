package dbdiff

import (
	"reflect"
)

type Differ interface {
	Close() error
	Diff() (*Result, error)
	Generate(*Result) ([]string, error)
}

type Result struct {
	Tables struct {
		Drop   map[string]Table
		Create map[string]Table
		Change map[string]map[string]string
	}
	Fields struct {
		Create map[string]map[string]Field
		Drop   map[string]map[string]Field
		Change map[string]map[string]Field
		Add    map[string]map[string]Field
	}
	Indexes struct {
		Create map[string]map[string]Index
		Change map[string]map[string]Index
		Add    map[string]map[string]Index
		Drop   map[string]map[string]Index
	}
}

func (r Result) IsEmpty() bool {
	return len(r.Tables.Drop) == 0 &&
		len(r.Tables.Create) == 0 &&
		len(r.Tables.Change) == 0 &&
		len(r.Fields.Create) == 0 &&
		len(r.Fields.Drop) == 0 &&
		len(r.Fields.Change) == 0 &&
		len(r.Fields.Add) == 0 &&
		len(r.Indexes.Create) == 0 &&
		len(r.Indexes.Change) == 0 &&
		len(r.Indexes.Add) == 0 &&
		len(r.Indexes.Drop) == 0
}

type Table struct {
	Name      string
	Engine    string
	Version   string
	RowFormat string
	Options   string
	Comment   string
	Collation string
}

func (t Table) Equal(t2 *Table) bool {
	return t.Name == t2.Name &&
		t.Engine == t2.Engine &&
		t.Version == t2.Version &&
		t.RowFormat == t2.RowFormat &&
		t.Options == t2.Options &&
		t.Comment == t2.Comment &&
		t.Collation == t2.Collation
}

type Field struct {
	Field     string
	Type      string
	Collation *string
	Null      string
	Key       string
	Default   *string
	Extra     string
	Comment   string
	After     string
}

func (f Field) Equal(f2 *Field) bool {
	return f.Field == f2.Field &&
		f.Type == f2.Type &&
		((f.Collation == nil && f2.Collation == nil) || *f.Collation == *f2.Collation) &&
		f.Null == f2.Null &&
		f.Key == f2.Key &&
		((f.Default == nil && f2.Default == nil) || *f.Default == *f2.Default) &&
		f.Extra == f2.Extra &&
		f.Comment == f2.Comment
}

type Index struct {
	Table        string
	NonUnique    int
	KeyName      string
	ColumnName   []string
	Collation    string
	IndexType    string
	Comment      string
	IndexComment string
}

func (i Index) Equal(i2 *Index) bool {
	return i.Table == i2.Table &&
		i.NonUnique == i2.NonUnique &&
		i.KeyName == i2.KeyName &&
		reflect.DeepEqual(i.ColumnName, i2.ColumnName) &&
		i.Collation == i2.Collation &&
		i.IndexType == i2.IndexType &&
		i.Comment == i2.Comment &&
		i.IndexComment == i2.IndexComment
}
