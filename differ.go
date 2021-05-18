package dbdiffer

import (
	"reflect"
)

type Differ interface {
	Close() error
	Diff(prefix string) (*Result, error)
	Generate(*Result) ([]string, error)
}

type Result struct {
	Drop   []Table
	Create []Table
	Change []Table
}

func (r Result) IsEmpty() bool {
	return len(r.Drop) == 0 && len(r.Create) == 0 && len(r.Change) == 0
}

type ResultFields struct {
	Create []Field // used for creating table
	Drop   []Field
	Change []Field
	Add    []Field
}

func (f ResultFields) IsEmpty() bool {
	return len(f.Create) == 0 && len(f.Drop) == 0 && len(f.Change) == 0 && len(f.Add) == 0
}

type ResultIndexes struct {
	Create []Index // used for creating table
	Add    []Index
	Drop   []Index
}

func (f ResultIndexes) IsEmpty() bool {
	return len(f.Create) == 0 && len(f.Drop) == 0 && len(f.Add) == 0
}

type Table struct {
	Name      string
	Engine    string
	Version   string
	RowFormat string
	Options   string
	Comment   string
	Collation string
	Fields    ResultFields
	Indexes   ResultIndexes
}

func (t Table) Equal(t2 Table) bool {
	return t.Name == t2.Name &&
		t.Engine == t2.Engine &&
		t.Version == t2.Version &&
		t.RowFormat == t2.RowFormat &&
		t.Options == t2.Options &&
		t.Comment == t2.Comment &&
		t.Collation == t2.Collation
}

func (t Table) IsEmpty() bool {
	return t.Engine == "" && t.Version == "" && t.RowFormat == "" && t.Options == "" && t.Comment == "" && t.Collation == "" &&
		t.Fields.IsEmpty() && t.Indexes.IsEmpty()
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

func (f Field) Equal(f2 Field) bool {
	return f.Field == f2.Field &&
		f.Type == f2.Type &&
		((f.Collation == nil && f2.Collation == nil) || *f.Collation == *f2.Collation) &&
		f.Null == f2.Null &&
		// f.Key == f2.Key &&
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

func (i Index) Equal(i2 Index) bool {
	return i.Table == i2.Table &&
		i.NonUnique == i2.NonUnique &&
		i.KeyName == i2.KeyName &&
		reflect.DeepEqual(i.ColumnName, i2.ColumnName) &&
		i.Collation == i2.Collation &&
		i.IndexType == i2.IndexType &&
		i.Comment == i2.Comment &&
		i.IndexComment == i2.IndexComment
}
