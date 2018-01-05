package controllers

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/prest/config"
	"github.com/prest/statements"
)

// GetTables list all (or filter) tables
func GetTables(w http.ResponseWriter, r *http.Request) (int, error) {
	requestWhere, values, err := config.PrestConf.Adapter.WhereByRequest(r, 1)
	if err != nil {
		err = fmt.Errorf("could not perform WhereByRequest: %v", err)
		return http.StatusBadRequest, err
	}

	order, err := config.PrestConf.Adapter.OrderByRequest(r)
	if err != nil {
		err = fmt.Errorf("could not perform OrderByRequest: %v", err)
		return http.StatusBadRequest, err
	}

	if order == "" {
		order = statements.TablesOrderBy
	}

	sqlTables := fmt.Sprint(
		statements.TablesSelect,
		statements.TablesWhere)

	distinct, err := config.PrestConf.Adapter.DistinctClause(r)
	if err != nil {
		return http.StatusBadRequest, err
	}
	if distinct != "" {
		sqlTables = strings.Replace(sqlTables, "SELECT", distinct, 1)
	}

	if requestWhere != "" {
		sqlTables = fmt.Sprintf("%s AND %s", sqlTables, requestWhere)
	}

	sqlTables = fmt.Sprint(sqlTables, order)
	sc := config.PrestConf.Adapter.Query(sqlTables, values...)
	if sc.Err() != nil {
		return http.StatusBadRequest, sc.Err()
	}
	w.Write(sc.Bytes())

	return http.StatusOK, nil
}

// GetTablesByDatabaseAndSchema list all (or filter) tables based on database and schema
func GetTablesByDatabaseAndSchema(w http.ResponseWriter, r *http.Request) (int, error) {
	vars := mux.Vars(r)
	database := vars["database"]
	schema := vars["schema"]

	config.PrestConf.Adapter.SetDatabase(database)

	requestWhere, values, err := config.PrestConf.Adapter.WhereByRequest(r, 3)
	if err != nil {
		err = fmt.Errorf("could not perform WhereByRequest: %v", err)
		return http.StatusBadRequest, err
	}

	sqlSchemaTables := fmt.Sprint(
		statements.SchemaTablesSelect,
		statements.SchemaTablesWhere)

	if requestWhere != "" {
		sqlSchemaTables = fmt.Sprint(sqlSchemaTables, " AND ", requestWhere)
	}

	order, err := config.PrestConf.Adapter.OrderByRequest(r)
	if err != nil {
		err = fmt.Errorf("could not perform OrderByRequest: %v", err)
		return http.StatusBadRequest, err
	}
	if order != "" {
		sqlSchemaTables = fmt.Sprint(sqlSchemaTables, order)
	} else {
		sqlSchemaTables = fmt.Sprint(sqlSchemaTables, statements.SchemaTablesOrderBy)
	}

	page, err := config.PrestConf.Adapter.PaginateIfPossible(r)
	if err != nil {
		return http.StatusBadRequest, err
	}

	sqlSchemaTables = fmt.Sprint(sqlSchemaTables, " ", page)

	valuesAux := make([]interface{}, 0)
	valuesAux = append(valuesAux, database)
	valuesAux = append(valuesAux, schema)
	valuesAux = append(valuesAux, values...)
	sc := config.PrestConf.Adapter.Query(sqlSchemaTables, valuesAux...)
	if sc.Err() != nil {
		return http.StatusBadRequest, sc.Err()
	}
	w.Write(sc.Bytes())

	return http.StatusOK, nil
}

// SelectFromTables perform select in database
func SelectFromTables(w http.ResponseWriter, r *http.Request) (int, error) {
	vars := mux.Vars(r)
	database := vars["database"]
	schema := vars["schema"]
	table := vars["table"]

	config.PrestConf.Adapter.SetDatabase(database)

	// get selected columns, "*" if empty "_columns"
	cols, err := config.PrestConf.Adapter.FieldsPermissions(r, table, "read")
	if err != nil {
		return http.StatusBadRequest, err
	}

	if len(cols) == 0 {
		err := fmt.Errorf("you don't have permission for this action, please check the permitted fields for this table")
		return http.StatusBadRequest, err
	}

	selectStr, err := config.PrestConf.Adapter.SelectFields(cols)
	if err != nil {
		return http.StatusBadRequest, err
	}
	query := fmt.Sprintf(`%s "%s"."%s"."%s"`, selectStr, database, schema, table)

	countQuery, err := config.PrestConf.Adapter.CountByRequest(r)
	if err != nil {
		err = fmt.Errorf("could not perform CountByRequest: %v", err)
		return http.StatusBadRequest, err
	}
	if countQuery != "" {
		query = fmt.Sprintf(`%s "%s"."%s"."%s"`, countQuery, database, schema, table)
	}

	joinValues, err := config.PrestConf.Adapter.JoinByRequest(r)
	if err != nil {
		err = fmt.Errorf("could not perform JoinByRequest: %v", err)
		return http.StatusBadRequest, err
	}

	for _, j := range joinValues {
		query = fmt.Sprint(query, j)
	}

	requestWhere, values, err := config.PrestConf.Adapter.WhereByRequest(r, 1)
	if err != nil {
		err = fmt.Errorf("could not perform WhereByRequest: %v", err)
		return http.StatusBadRequest, err
	}

	sqlSelect := query
	if requestWhere != "" {
		sqlSelect = fmt.Sprint(
			query,
			" WHERE ",
			requestWhere)
	}

	groupBySQL := config.PrestConf.Adapter.GroupByClause(r)

	if groupBySQL != "" {
		sqlSelect = fmt.Sprintf("%s %s", sqlSelect, groupBySQL)
	}

	order, err := config.PrestConf.Adapter.OrderByRequest(r)
	if err != nil {
		err = fmt.Errorf("could not perform OrderByRequest: %v", err)
		return http.StatusBadRequest, err
	}
	if order != "" {
		sqlSelect = fmt.Sprintf("%s %s", sqlSelect, order)
	}

	page, err := config.PrestConf.Adapter.PaginateIfPossible(r)
	if err != nil {
		err = fmt.Errorf("could not perform PaginateIfPossible: %v", err)
		return http.StatusBadRequest, err
	}
	sqlSelect = fmt.Sprint(sqlSelect, " ", page)

	runQuery := config.PrestConf.Adapter.Query
	if countQuery != "" {
		runQuery = config.PrestConf.Adapter.QueryCount
	}

	sc := runQuery(sqlSelect, values...)
	if sc.Err() != nil {
		return http.StatusBadRequest, sc.Err()
	}
	w.Write(sc.Bytes())

	return http.StatusOK, nil
}

// InsertInTables perform insert in specific table
func InsertInTables(w http.ResponseWriter, r *http.Request) (int, error) {
	vars := mux.Vars(r)
	database := vars["database"]
	schema := vars["schema"]
	table := vars["table"]

	config.PrestConf.Adapter.SetDatabase(database)

	names, placeholders, values, err := config.PrestConf.Adapter.ParseInsertRequest(r)
	if err != nil {
		err = fmt.Errorf("could not perform InsertInTables: %v", err)
		return http.StatusBadRequest, err
	}

	sql := fmt.Sprintf(statements.InsertQuery, database, schema, table, names, placeholders)

	sc := config.PrestConf.Adapter.Insert(sql, values...)
	if sc.Err() != nil {
		return http.StatusBadRequest, sc.Err()
	}
	w.Write(sc.Bytes())

	return http.StatusOK, nil
}

// DeleteFromTable perform delete sql
func DeleteFromTable(w http.ResponseWriter, r *http.Request) (int, error) {
	vars := mux.Vars(r)
	database := vars["database"]
	schema := vars["schema"]
	table := vars["table"]

	config.PrestConf.Adapter.SetDatabase(database)

	where, values, err := config.PrestConf.Adapter.WhereByRequest(r, 1)
	if err != nil {
		err = fmt.Errorf("could not perform WhereByRequest: %v", err)
		return http.StatusBadRequest, err
	}

	sql := fmt.Sprintf(statements.DeleteQuery, database, schema, table)
	if where != "" {
		sql = fmt.Sprint(sql, " WHERE ", where)
	}

	sc := config.PrestConf.Adapter.Delete(sql, values...)
	if sc.Err() != nil {
		return http.StatusBadRequest, sc.Err()
	}
	w.Write(sc.Bytes())

	return http.StatusOK, nil
}

// UpdateTable perform update table
func UpdateTable(w http.ResponseWriter, r *http.Request) (int, error) {
	vars := mux.Vars(r)
	database := vars["database"]
	schema := vars["schema"]
	table := vars["table"]

	config.PrestConf.Adapter.SetDatabase(database)

	where, whereValues, err := config.PrestConf.Adapter.WhereByRequest(r, 1)
	if err != nil {
		err = fmt.Errorf("could not perform WhereByRequest: %v", err)
		return http.StatusBadRequest, err
	}

	pid := len(whereValues) + 1 // placeholder id

	setSyntax, values, err := config.PrestConf.Adapter.SetByRequest(r, pid)
	if err != nil {
		err = fmt.Errorf("could not perform UPDATE: %v", err)
		return http.StatusBadRequest, err
	}
	sql := fmt.Sprintf(statements.UpdateQuery, database, schema, table, setSyntax)

	if where != "" {
		sql = fmt.Sprint(
			sql,
			" WHERE ",
			where)
		values = append(whereValues, values...)
	}

	sc := config.PrestConf.Adapter.Update(sql, values...)
	if sc.Err() != nil {
		return http.StatusBadRequest, sc.Err()
	}
	w.Write(sc.Bytes())

	return http.StatusOK, nil
}
