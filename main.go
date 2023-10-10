package main

import (
	"log"
	"net/http"
)

func main() {
	app := NewApplication()
	router := app.NewRouter()

	log.Fatal(http.ListenAndServe(":8080", router))
}
