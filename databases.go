package controllers

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/prest/config"
	"github.com/prest/statements"
)

// GetDatabases list all (or filter) databases
func GetDatabases(w http.ResponseWriter, r *http.Request) (int, error) {
	requestWhere, values, err := config.PrestConf.Adapter.WhereByRequest(r, 1)
	if err != nil {
		return http.StatusBadRequest, err
	}

	query, hasCount := config.PrestConf.Adapter.DatabaseClause(r)
	sqlDatabases := fmt.Sprint(query, statements.DatabasesWhere)

	if requestWhere != "" {
		sqlDatabases = fmt.Sprint(sqlDatabases, " AND ", requestWhere)
	}

	distinct, err := config.PrestConf.Adapter.DistinctClause(r)
	if err != nil {
		return http.StatusBadRequest, err
	}
	if distinct != "" {
		sqlDatabases = strings.Replace(sqlDatabases, "SELECT", distinct, 1)
	}

	order, err := config.PrestConf.Adapter.OrderByRequest(r)

	if err != nil {
		return http.StatusBadRequest, err
	}

	if order != "" {
		sqlDatabases = fmt.Sprint(sqlDatabases, order)
	} else if !hasCount {
		sqlDatabases = fmt.Sprint(sqlDatabases, fmt.Sprintf(statements.DatabasesOrderBy, statements.FieldDatabaseName))
	}

	page, err := config.PrestConf.Adapter.PaginateIfPossible(r)
	if err != nil {
		return http.StatusBadRequest, err
	}

	sqlDatabases = fmt.Sprint(sqlDatabases, " ", page)
	sc := config.PrestConf.Adapter.Query(sqlDatabases, values...)
	if sc.Err() != nil {
		return http.StatusBadRequest, sc.Err()
	}
	w.Write(sc.Bytes())
	return http.StatusOK, nil
}
