package controllers

import (
	"fmt"
	"net/http"

	"github.com/prest/config"
	"github.com/prest/statements"
)

// GetSchemas list all (or filter) schemas
func GetSchemas(w http.ResponseWriter, r *http.Request) {
	requestWhere, values, err := config.PrestConf.Adapter.WhereByRequest(r, 1)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	sqlSchemas, hasCount := config.PrestConf.Adapter.SchemaClause(r)

	if requestWhere != "" {
		sqlSchemas = fmt.Sprint(sqlSchemas, " WHERE ", requestWhere)
	}

	order, err := config.PrestConf.Adapter.OrderByRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if order != "" {
		sqlSchemas = fmt.Sprint(sqlSchemas, order)
	} else if !hasCount {
		sqlSchemas = fmt.Sprint(sqlSchemas, fmt.Sprintf(statements.SchemasOrderBy, statements.FieldSchemaName))
	}

	page, err := config.PrestConf.Adapter.PaginateIfPossible(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	sqlSchemas = fmt.Sprint(sqlSchemas, " ", page)
	sc := config.PrestConf.Adapter.Query(sqlSchemas, values...)
	if sc.Err() != nil {
		http.Error(w, sc.Err().Error(), http.StatusBadRequest)
		return
	}
	w.Write(sc.Bytes())
}
