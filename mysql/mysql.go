package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/sillydong/dbdiffer"
)

type Driver struct {
	newDb *sql.DB
	oldDb *sql.DB
}

// New creates a new Driver driver.
// The DSN is documented here: https://github.com/go-sql-driver/mysql#dsn-data-source-name
func New(newDsn, oldDsn string) (dbdiffer.Differ, error) {
	parsedNewDSN, err := mysql.ParseDSN(newDsn)
	if err != nil {
		return nil, err
	}

	parsedNewDSN.MultiStatements = true
	newDb, err := sql.Open("mysql", parsedNewDSN.FormatDSN())
	if err != nil {
		return nil, err
	}
	if err := newDb.Ping(); err != nil {
		return nil, err
	}

	parsedOldDSN, err := mysql.ParseDSN(oldDsn)
	if err != nil {
		return nil, err
	}

	parsedOldDSN.MultiStatements = true
	oldDb, err := sql.Open("mysql", parsedOldDSN.FormatDSN())
	if err != nil {
		return nil, err
	}
	if err := oldDb.Ping(); err != nil {
		return nil, err
	}

	d := &Driver{
		newDb: newDb,
		oldDb: oldDb,
	}
	return d, nil
}

// NewFromDB returns a mysql driver from a sql.DB
func NewFromDB(newDb, oldDb *sql.DB) (dbdiffer.Differ, error) {
	if _, ok := newDb.Driver().(*mysql.MySQLDriver); !ok {
		return nil, errors.New("new database instance is not using the MySQL driver")
	}
	if _, ok := oldDb.Driver().(*mysql.MySQLDriver); !ok {
		return nil, errors.New("old database instance is not using the MySQL driver")
	}

	if err := newDb.Ping(); err != nil {
		return nil, err
	}

	if err := oldDb.Ping(); err != nil {
		return nil, err
	}

	d := &Driver{
		newDb: newDb,
		oldDb: oldDb,
	}
	return d, nil
}

// Close closes the connection to the Driver server.
func (d *Driver) Close() error {
	err := d.newDb.Close()
	if err != nil {
		return err
	}

	err = d.oldDb.Close()
	if err != nil {
		return err
	}
	return nil
}

func (d *Driver) Diff(prefix string) (diff *dbdiffer.Result, err error) {
	//retrive new database structure
	newtables, err := tables(d.newDb, prefix)
	if err != nil {
		return nil, err
	}
	newtablefields := make(map[string]map[string]dbdiffer.Field, len(newtables))
	newtableindexes := make(map[string]map[string]dbdiffer.Index, len(newtables))
	for _, table := range newtables {
		newtablefields[table.Name], err = fields(d.newDb, table.Name)
		if err != nil {
			return nil, err
		}
		newtableindexes[table.Name], err = indexes(d.newDb, table.Name)
		if err != nil {
			return nil, err
		}
	}

	//retrive old database structure
	oldtables, err := tables(d.oldDb, prefix)
	if err != nil {
		return nil, err
	}
	oldtablefields := make(map[string]map[string]dbdiffer.Field, len(oldtables))
	oldtableindexes := make(map[string]map[string]dbdiffer.Index, len(oldtables))
	for _, table := range oldtables {
		oldtablefields[table.Name], err = fields(d.oldDb, table.Name)
		if err != nil {
			return nil, err
		}
		oldtableindexes[table.Name], err = indexes(d.oldDb, table.Name)
		if err != nil {
			return nil, err
		}
	}

	//compare
	result := dbdiffer.Result{
		Tables: struct {
			Drop   map[string]dbdiffer.Table
			Create map[string]dbdiffer.Table
			Change map[string]map[string]string
		}{
			Drop:   map[string]dbdiffer.Table{},
			Create: map[string]dbdiffer.Table{},
			Change: map[string]map[string]string{},
		},
		Fields: struct {
			Create map[string]map[string]dbdiffer.Field
			Drop   map[string]map[string]dbdiffer.Field
			Change map[string]map[string]dbdiffer.Field
			Add    map[string]map[string]dbdiffer.Field
		}{
			Create: map[string]map[string]dbdiffer.Field{},
			Drop:   map[string]map[string]dbdiffer.Field{},
			Change: map[string]map[string]dbdiffer.Field{},
			Add:    map[string]map[string]dbdiffer.Field{},
		},
		Indexes: struct {
			Create map[string]map[string]dbdiffer.Index
			Change map[string]map[string]dbdiffer.Index
			Add    map[string]map[string]dbdiffer.Index
			Drop   map[string]map[string]dbdiffer.Index
		}{
			Create: map[string]map[string]dbdiffer.Index{},
			Change: map[string]map[string]dbdiffer.Index{},
			Add:    map[string]map[string]dbdiffer.Index{},
			Drop:   map[string]map[string]dbdiffer.Index{},
		},
	}

	//table
	for oldname, olddetail := range oldtables {
		//在新表中不存在，drop
		if _, exist := newtables[oldname]; !exist {
			result.Tables.Drop[oldname] = olddetail
		}
	}
	for newname, newdetail := range newtables {
		//旧表中没有，新建表，新建fields，新建index
		if olddetail, exist := oldtables[newname]; !exist {
			result.Tables.Create[newname] = newdetail
			result.Fields.Create[newname] = newtablefields[newname]
			result.Indexes.Create[newname] = newtableindexes[newname]
		} else {
			//对比表属性
			change := make(map[string]string, 0)
			if newdetail.Name != olddetail.Name {
				change["Name"] = newdetail.Name
			}
			if newdetail.Engine != olddetail.Engine {
				change["Engine"] = newdetail.Engine
			}
			if newdetail.RowFormat != olddetail.RowFormat {
				change["RowFormat"] = newdetail.RowFormat
			}
			// if newdetail.Options != olddetail.Options {
			// 	change["Options"] = newdetail.Options
			// }
			if newdetail.Collation != olddetail.Collation {
				change["Collation"] = newdetail.Collation
			}
			if newdetail.Comment != olddetail.Comment {
				change["Comment"] = newdetail.Comment
			}
			if len(change) > 0 {
				result.Tables.Change[newname] = change
			}
		}
	}

	//index
	for tablename, oldindexes := range oldtableindexes {
		if newindexes, exists := newtableindexes[tablename]; exists {
			for indexname, indexdetail := range oldindexes {
				if _, exists := newindexes[indexname]; !exists {
					//索引在新表中不存在， 删除索引
					if result.Indexes.Drop[tablename] == nil {
						result.Indexes.Drop[tablename] = map[string]dbdiffer.Index{}
					}
					result.Indexes.Drop[tablename][indexname] = indexdetail
				}
			}
		} else {
			if _, exists := result.Tables.Drop[tablename]; !exists {
				//如果表不被删除，则单独删除这个表的这些索引
				result.Indexes.Drop[tablename] = map[string]dbdiffer.Index{}
				for indexname, indexdetail := range oldindexes {
					result.Indexes.Drop[tablename][indexname] = indexdetail
				}
			}
		}
	}
	for tablename, newindexes := range newtableindexes {
		if oldindexes, exists := oldtableindexes[tablename]; exists {
			for indexname, indexdetail := range newindexes {
				if oldindex, exists := oldindexes[indexname]; exists {
					//对比内容
					if !indexdetail.Equal(&oldindex) {
						//删除旧索引
						if result.Indexes.Drop[tablename] == nil {
							result.Indexes.Drop[tablename] = map[string]dbdiffer.Index{}
						}
						result.Indexes.Drop[tablename][indexname] = oldindex
						//创建新索引
						if result.Indexes.Add[tablename] == nil {
							result.Indexes.Add[tablename] = map[string]dbdiffer.Index{}
						}
						result.Indexes.Add[tablename][indexname] = oldindex
					}
				} else {
					//需要添加的索引
					if result.Indexes.Add[tablename] == nil {
						result.Indexes.Add[tablename] = map[string]dbdiffer.Index{}
					}
					result.Indexes.Add[tablename][indexname] = indexdetail
				}
			}
		}
	}

	//fields
	for tablename, oldfields := range oldtablefields {
		if newfields, exists := newtablefields[tablename]; exists {
			for fieldname, field := range oldfields {
				if _, exists := newfields[fieldname]; !exists {
					//删除字段
					if result.Fields.Drop[tablename] == nil {
						result.Fields.Drop[tablename] = map[string]dbdiffer.Field{}
					}
					result.Fields.Drop[tablename][fieldname] = field
				}
			}
		}
	}
	for tablename, newfields := range newtablefields {
		if oldfields, exists := oldtablefields[tablename]; exists {
			lastfield := ""
			for fieldname, field := range newfields {
				if oldfield, exists := oldfields[fieldname]; exists {
					//字段存在，对比内容
					if !field.Equal(&oldfield) {
						if result.Fields.Change[tablename] == nil {
							result.Fields.Change[tablename] = map[string]dbdiffer.Field{}
						}
						result.Fields.Change[tablename][fieldname] = field
					}
				} else {
					//字段不存在，添加字段
					field.After = lastfield
					if result.Fields.Add[tablename] == nil {
						result.Fields.Add[tablename] = map[string]dbdiffer.Field{}
					}
					result.Fields.Add[tablename][fieldname] = field
				}
				lastfield = fieldname
			}
		}
	}
	return &result, nil
}

func (d *Driver) Generate(result *dbdiffer.Result) ([]string, error) {
	sqls := make([]string, 0)
	if result.IsEmpty() {
		return sqls, nil
	}
	if len(result.Tables.Drop) > 0 {
		for tablename := range result.Tables.Drop {
			sqls = append(sqls, "DROP TABLE `"+tablename+"`;")
		}
	}
	if len(result.Tables.Create) > 0 {
		for tablename, tabledetail := range result.Tables.Create {
			fields, exists := result.Fields.Create[tablename]
			if !exists {
				return nil, errors.New("fail get fields to create table")
			}
			sql := "CREATE TABLE `" + tablename + "` ("
			fieldstr := make([]string, 0)
			for _, field := range fields {
				fieldstr = append(fieldstr, "`"+field.Field+"` "+strings.ToUpper(field.Type)+sqlnull(field.Null)+sqldefault(field.Default)+sqlextra(field.Extra)+sqlcomment(field.Comment))
			}
			if indexes, exists := result.Indexes.Create[tablename]; exists {
				for _, index := range indexes {
					if index.KeyName == "PRIMARY" {
						fieldstr = append(fieldstr, " PRIMARY KEY (`"+strings.Join(index.ColumnName, "`, `")+"`)")
					} else {
						fieldstr = append(fieldstr, sqluniq(index.NonUnique)+" `"+index.KeyName+"` ("+strings.Join(index.ColumnName, "`, `")+"`)")
					}
				}
			}
			chars := strings.Split(tabledetail.Collation, "_")
			sql += strings.Join(fieldstr, ", ") + ") ENGINE = " + tabledetail.Engine + " DEFAULT CHARSET = " + chars[0]
			sqls = append(sqls, sql+";")
		}
	}
	if len(result.Tables.Change) > 0 {
		for tablename, tabledetail := range result.Tables.Change {
			if len(tabledetail) > 0 {
				sql := "ALTER TABLE `" + tablename + "`"
				for k, v := range tabledetail {
					if k == "Collation" {
						chars := strings.Split(v, "_")
						sql += " DEFAULT CHARACTER SET " + chars[0] + " COLLATE " + v
					} else {
						sql += " " + strings.ToUpper(k) + " = " + v
					}
				}
				sqls = append(sqls, sql+";")
			}
		}
	}
	if len(result.Indexes.Drop) > 0 {
		for tablename, indexes := range result.Indexes.Drop {
			for name := range indexes {
				if name == "PRIMARY" {
					sqls = append(sqls, "ALTER TABLE `"+tablename+"` DROP PRIMARY KEY;")
				} else {
					sqls = append(sqls, "ALTER TABLE `"+tablename+"` DROP INDEX `"+name+"`;")
				}
			}
		}
	}
	if len(result.Fields.Drop) > 0 {
		for tablename, fields := range result.Fields.Drop {
			for name := range fields {
				sqls = append(sqls, "ALTER TABLE `"+tablename+"` DROP `"+name+"`;")
			}
		}
	}
	if len(result.Fields.Add) > 0 {
		for tablename, fields := range result.Fields.Add {
			for name, detail := range fields {
				sqls = append(sqls, "ALTER TABLE `"+tablename+"` ADD `"+name+"` "+strings.ToUpper(detail.Type)+sqlcol(detail.Collation)+sqlnull(detail.Null)+sqldefault(detail.Default)+sqlextra(detail.Extra)+sqlcomment(detail.Comment)+" AFTER `"+detail.After+"`;")
			}
		}
	}
	if len(result.Indexes.Add) > 0 {
		for tablename, indexes := range result.Indexes.Add {
			for name, detail := range indexes {
				if name == "PRIMARY" {
					sqls = append(sqls, "ALTER TABLE `"+tablename+"` ADD PRIMARY KEY (`"+strings.Join(detail.ColumnName, "`, `")+"`);")
				} else {
					sqls = append(sqls, "ALTER TABLE `"+tablename+"` ADD "+sqluniq(detail.NonUnique)+" `"+name+"` (`"+strings.Join(detail.ColumnName, "`, `")+"`);")
				}
			}
		}
	}
	if len(result.Fields.Change) > 0 {
		for tablename, fields := range result.Fields.Change {
			for name, detail := range fields {
				sqls = append(sqls, "ALTER TABLE `"+tablename+"` CHANGE `"+name+"` `"+name+"` "+strings.ToUpper(detail.Type)+sqlcol(detail.Collation)+sqlnull(detail.Null)+sqldefault(detail.Default)+sqlextra(detail.Extra)+sqlcomment(detail.Comment)+";")
			}
		}
	}

	return sqls, nil
}

func tables(db *sql.DB, prefix string) (map[string]dbdiffer.Table, error) {
	query := "SHOW TABLE STATUS;"
	if prefix != "" {
		query = "SHOW TABLE STATUS LIKE '" + prefix + "%';"
	}
	resultrows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer resultrows.Close()
	tables := make(map[string]dbdiffer.Table)
	for resultrows.Next() {

		var (
			name            string
			engine          string
			version         string
			row_format      string
			rows            int64
			avg_row_length  int64
			data_length     int64
			max_data_length int64
			index_length    int64
			data_free       int64
			auto_increment  *int64
			create_time     *string
			update_time     *string
			check_time      *string
			collection      string
			checksum        *string
			create_options  string
			comment         string
		)
		if err := resultrows.Scan(&name, &engine, &version, &row_format, &rows, &avg_row_length, &data_length, &max_data_length, &index_length, &data_free, &auto_increment, &create_time, &update_time, &check_time, &collection, &checksum, &create_options, &comment); err != nil {
			return nil, err
		}
		tables[name] = dbdiffer.Table{
			Name:      name,
			Engine:    engine,
			Version:   version,
			RowFormat: row_format,
			Options:   create_options,
			Comment:   comment,
			Collation: collection,
		}
	}
	return tables, nil
}

func fields(db *sql.DB, table string) (map[string]dbdiffer.Field, error) {
	resultrows, err := db.Query("SHOW FULL FIELDS FROM `" + table + "`;")
	if err != nil {
		return nil, err
	}
	defer resultrows.Close()
	fields := make(map[string]dbdiffer.Field, 0)
	for resultrows.Next() {
		var (
			field      string
			typ        string
			collation  *string
			null       string
			key        string
			def        *string
			extra      string
			privileges string
			comment    string
		)
		if err := resultrows.Scan(&field, &typ, &collation, &null, &key, &def, &extra, &privileges, &comment); err != nil {
			return nil, err
		}
		fields[field] = dbdiffer.Field{
			Field:     field,
			Type:      typ,
			Collation: collation,
			Null:      null,
			Key:       key,
			Default:   def,
			Extra:     extra,
			Comment:   comment,
		}
	}
	return fields, nil
}

func indexes(db *sql.DB, table string) (map[string]dbdiffer.Index, error) {
	resultrows, err := db.Query("SHOW INDEX FROM `" + table + "`;")
	if err != nil {
		return nil, err
	}
	defer resultrows.Close()
	columns, err := resultrows.Columns()
	if err != nil {
		return nil, err
	}
	column_len := len(columns)
	if column_len != 13 && column_len != 15 {
		return nil, fmt.Errorf("returned %d columns while listing index", len(columns))
	}
	indexes := make(map[string]dbdiffer.Index)
	for resultrows.Next() {
		var (
			table         string
			non_unique    int
			key_name      string
			seq_in_index  int
			column_name   string
			collation     string
			cardinality   int
			sub_part      *string
			packed        *string
			null          string
			index_type    string
			comment       string
			index_comment string
			visible       string
			expression    *string
		)
		switch column_len {
		case 13:
			if err := resultrows.Scan(&table, &non_unique, &key_name, &seq_in_index, &column_name, &collation, &cardinality, &sub_part, &packed, &null, &index_type, &comment, &index_comment); err != nil {
				return nil, err
			}
		case 15:
			if err := resultrows.Scan(&table, &non_unique, &key_name, &seq_in_index, &column_name, &collation, &cardinality, &sub_part, &packed, &null, &index_type, &comment, &index_comment, &visible, &expression); err != nil {
				return nil, err
			}
		}

		if idx, exist := indexes[key_name]; exist {
			idx.ColumnName = append(idx.ColumnName, column_name)
			indexes[key_name] = idx
		} else {
			indexes[key_name] = dbdiffer.Index{
				Table:        table,
				NonUnique:    non_unique,
				KeyName:      key_name,
				ColumnName:   []string{column_name},
				Collation:    collation,
				IndexType:    index_type,
				Comment:      comment,
				IndexComment: index_comment,
			}
		}
	}
	return indexes, nil
}

func sqlnull(s string) string {
	switch s {
	case "NO":
		return " NOT NULL"
	case "YES":
		return " NULL"
	default:
		return ""
	}
}

func sqldefault(s *string) string {
	switch s {
	case nil:
		return ""
	default:
		return " DEFAULT '" + escape(*s) + "'"
	}
}

func sqlextra(s string) string {
	switch s {
	case "":
		return ""
	default:
		return " " + strings.ToUpper(s)
	}
}

func sqlcomment(s string) string {
	switch s {
	case "":
		return ""
	default:
		return " COMMENT '" + escape(s) + "'"
	}
}

func sqluniq(s int) string {
	if s == 0 {
		return "UNIQUE"
	}
	return "INDEX"
}

func sqlcol(s *string) string {
	switch s {
	case nil:
		return ""
	default:
		chars := strings.Split(*s, "_")
		return " CHARACTER SET " + chars[0] + " COLLATE " + *s
	}
}

func escape(s string) string {
	replacer := strings.NewReplacer(
		`\`, `\\`,
		`'`, `\'`,
	)
	return replacer.Replace(s)
}
