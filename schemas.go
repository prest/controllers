package controllers

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/prest/config"
	"github.com/prest/statements"
)

// GetSchemas list all (or filter) schemas
func GetSchemas(w http.ResponseWriter, r *http.Request) (int, error) {
	requestWhere, values, err := config.PrestConf.Adapter.WhereByRequest(r, 1)
	if err != nil {
		return http.StatusBadRequest, err
	}

	sqlSchemas, hasCount := config.PrestConf.Adapter.SchemaClause(r)

	if requestWhere != "" {
		sqlSchemas = fmt.Sprint(sqlSchemas, " WHERE ", requestWhere)
	}

	distinct, err := config.PrestConf.Adapter.DistinctClause(r)
	if err != nil {
		return http.StatusBadRequest, err
	}
	if distinct != "" {
		sqlSchemas = strings.Replace(sqlSchemas, "SELECT", distinct, 1)
	}

	order, err := config.PrestConf.Adapter.OrderByRequest(r)

	if err != nil {
		return http.StatusBadRequest, err
	}

	if order != "" {
		sqlSchemas = fmt.Sprint(sqlSchemas, order)
	} else if !hasCount {
		sqlSchemas = fmt.Sprint(sqlSchemas, fmt.Sprintf(statements.SchemasOrderBy, statements.FieldSchemaName))
	}

	page, err := config.PrestConf.Adapter.PaginateIfPossible(r)
	if err != nil {
		return http.StatusBadRequest, err
	}

	sqlSchemas = fmt.Sprint(sqlSchemas, " ", page)
	sc := config.PrestConf.Adapter.Query(sqlSchemas, values...)
	if sc.Err() != nil {
		return http.StatusBadRequest, sc.Err()
	}
	w.Write(sc.Bytes())

	return http.StatusOK, nil
}
