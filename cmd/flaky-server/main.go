package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"sync/atomic"
)

// A test server that fails the first N requests, then succeeds.
// Used to test the retry logic in RetrySink.
func main() {
	var count atomic.Int64

	http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		n := count.Add(1)
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()

		if n <= 2 {
			// Fail the first 2 requests
			log.Printf("request #%d → 500 (simulated failure) body=%d bytes", n, len(body))
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "simulated failure #%d", n)
			return
		}

		// Succeed after that
		log.Printf("request #%d → 200 (success) body=%d bytes", n, len(body))
		w.WriteHeader(http.StatusOK)
	})

	log.Println("flaky server on :9091 (first 2 requests will fail)")
	log.Fatal(http.ListenAndServe(":9091", nil))
}
