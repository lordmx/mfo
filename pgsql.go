package main

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	sq "github.com/lann/squirrel"
	_ "github.com/lib/pq"
)

func NewPgsql(
	host string,
	port int,
	user, password, database, names, timezone string,
) (*sqlx.DB, error) {
	if host == "" {
		host = "localhost"
	}

	dsn := fmt.Sprintf(
		"user=%s password=%s dbname=%s host=%s ",
		user,
		password,
		database,
		host,
	)

	if port != 0 {
		dsn += fmt.Sprintf("port=%d", port)
	}

	db, err := sqlx.Open("postgres", dsn)

	if err != nil {
		return nil, err
	}

	if names != "" {
		_, err = db.Exec(fmt.Sprintf("SET NAMES '%s'", names))

		if err != nil {
			return nil, err
		}
	}

	if timezone != "" {
		db.Exec(fmt.Sprintf("SET TIME ZONE '%s'", timezone))

		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

func QueryBuilder() sq.StatementBuilderType {
	return sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
}
