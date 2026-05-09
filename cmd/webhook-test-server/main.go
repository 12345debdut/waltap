package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
)

// A tiny test server that receives webhook events and prints them.
// Run this, then point pgcdc at it with --sink=webhook --webhook-url=http://localhost:9090/events
func main() {
	http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()

		// Pretty-print the JSON
		var pretty json.RawMessage
		if err := json.Unmarshal(body, &pretty); err != nil {
			log.Printf("non-JSON body: %s", body)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		formatted, _ := json.MarshalIndent(pretty, "", "  ")

		fmt.Printf("── %s %s.%s ──\n%s\n\n",
			r.Header.Get("X-PGCDC-Action"),
			r.Header.Get("X-PGCDC-Table"),
			"",
			formatted,
		)

		w.WriteHeader(http.StatusOK)
	})

	log.Println("webhook test server listening on :9090")
	log.Fatal(http.ListenAndServe(":9090", nil))
}
