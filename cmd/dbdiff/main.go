package main

import (
	"fmt"
	"log"
	"os"

	"github.com/sillydong/dbdiffer"
	"github.com/sillydong/dbdiffer/mysql"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.NewApp()
	app.Name = "DBDiff"
	app.Usage = "diff databases and generate upgrade sql"
	app.Flags = []cli.Flag{
		&cli.StringFlag{Name: "type", Aliases: []string{"t"}, Usage: fmt.Sprintf("database type, valid values: %v", dbdiffer.DriverList), Required: true},
		&cli.StringFlag{Name: "new", Aliases: []string{"n"}, Usage: "DSN to the database instance in higher version, format: username:password@protocol(address)/dbname?param=value", Required: true},
		&cli.StringFlag{Name: "old", Aliases: []string{"o"}, Usage: "DSN to the database instance in lower version, format: username:password@protocol(address)/dbname?param=value", Required: true},
	}
	app.Action = func(ctx *cli.Context) error {
		dbtype := ctx.String("type")
		new := ctx.String("new")
		old := ctx.String("old")

		avaiableDbTypes := map[string]struct{}{}
		for _, t := range dbdiffer.DriverList {
			avaiableDbTypes[t] = struct{}{}
		}
		if _, exist := avaiableDbTypes[dbtype]; !exist {
			return fmt.Errorf("%s is not supported", dbtype)
		}

		fmt.Printf("driver: %s\nnew db: %s\nold db: %s\n\n", dbtype, new, old)

		switch dbtype {
		case mysql.MySQL:
			d, err := mysql.New(new, old)
			if err != nil {
				return err
			}
			defer d.Close()
			res, err := d.Diff("")
			if err != nil {
				return err
			}
			sqls, err := d.Generate(res)
			if err != nil {
				return err
			}
			for _, sql := range sqls {
				fmt.Println(sql)
			}
		}

		return nil
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
