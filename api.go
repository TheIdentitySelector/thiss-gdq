package main

import (
	"github.com/blevesearch/bleve"
        "github.com/gorilla/mux"
        "net/http"
	"fmt"
        "encoding/json"
	"strings"
	"time"
)

func (md *Metadata) NewAPI() http.Handler {
        router := mux.NewRouter()
        router.StrictSlash(true)

	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		sz, _:= md.index.DocCount()
		status := map[string]interface{} {
			"size": int(sz),
			"version": fmt.Sprintf("%s - %s", Name, Version),
		}
		w.Header().Set("Content-Type", "application/json")
                json.NewEncoder(w).Encode(status)
	}).Methods("GET")

	router.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
                sz, _:= md.index.DocCount()
		if sz > 0 {
			w.Write([]byte("OK\n"))
		} else {
			w.WriteHeader(http.StatusInternalServerError)
                        w.Write([]byte("500 - Not enough data\n"))
		}
        }).Methods("GET","HEAD")

	router.HandleFunc("/.well-known/webfinger", func(w http.ResponseWriter, r *http.Request) {
		log.Print(r.URL)
		sz, err :=  md.index.DocCount()
                if err != nil {
                        log.Fatal("unable to re-index dataset - recommend manual clean")
                }
		base_url := fmt.Sprintf("%s://%s",r.URL.Scheme, r.URL.Host)
		var links []map[string]interface{} = make([]map[string]interface{},int(sz))
		validDuration, _ := time.ParseDuration("3600s")
		result := map[string]interface{} {
			"expires": time.Now().Add(validDuration),
			"subject": base_url,
			"links": links,
		}

		query := bleve.NewMatchAllQuery()
                search := bleve.NewSearchRequest(query)
                search.Size = int(sz)
                searchResults, _ := md.index.Search(search)
                for i, hit := range searchResults.Hits {
			links[i] = map[string]interface{} {
				"rel": "disco-json", "href": fmt.Sprintf("%s/entities/{sha1}%s",base_url,hit.ID),
			}
		}
		w.Header().Set("Content-Type", "application/json")
                json.NewEncoder(w).Encode(result)
	}).Methods("GET")

        router.HandleFunc("/entities/", func(w http.ResponseWriter, r *http.Request) {
		qm := r.URL.Query()
		var searchResults *bleve.SearchResult
		var err error
		if val, ok := qm["q"]; ok {
			q := strings.ToLower(val[0])
			query := bleve.NewQueryStringQuery(fmt.Sprintf("+content:%s* +type:idp", q))
			log.Printf("%s", query)
			search := bleve.NewSearchRequest(query)
			search.Size = 100
			search.Fields = append(search.Fields, "data")
			searchResults, err = md.index.Search(search)
			if err != nil {
                                log.Fatal(err) // better to give up
                        }
		} else {
			sz, _ :=  md.index.DocCount()
			query := bleve.NewMatchAllQuery()
			search := bleve.NewSearchRequest(query)
			search.Size = int(sz)
			searchResults, err = md.index.Search(search)
			if err != nil {
				log.Fatal(err) // better to give up
			}
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		comma := false
		w.Write([]byte("["))
                for _, hit := range searchResults.Hits {
			if comma {
				w.Write([]byte(","))
                        }
                        e := md.db[hit.ID]
                        if e != nil {
				comma = true
				enc.Encode(e.(map[string]interface{}))
                        } else {
                                log.Warn("missing from db: %s", hit)
                        }
                }
		w.Write([]byte("]"))
        }).Methods("GET")

        router.HandleFunc("/entities/{id}", func(w http.ResponseWriter, r *http.Request) {
                vars := mux.Vars(r)
                id := vars["id"]
                var ID string
                fmt.Sscanf(id,"{sha1}%s",&ID)
                parts := strings.Split(ID,".")
                doc := md.db[parts[0]]
                log.Println(doc)
                if doc != nil {
                        w.Header().Set("Content-Type", "application/json")
                        json.NewEncoder(w).Encode(doc)
                } else {
                        w.WriteHeader(http.StatusNotFound)
                        w.Write([]byte("404 - Not Found\n"))
                }
        }).Methods("GET")

	return router
}
