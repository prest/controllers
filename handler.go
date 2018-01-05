package controllers

import (
	"net/http"
)

// GenericHttpHandler type for http.Handler
type GenericHttpHandler func(http.ResponseWriter, *http.Request) (int, error)

// http.Handler wrapper function
func (fn GenericHttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if status, err := fn(w, r); err != nil {
		http.Error(w, err.Error(), status)
	}
}

