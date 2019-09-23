package main

import (
	"crypto/sha1"
	"fmt"
	"github.com/bcicen/jstream"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/analyzer/custom"
	"github.com/blevesearch/bleve/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/analysis/token/edgengram"
	"github.com/blevesearch/bleve/analysis/token/lowercase"
	"github.com/blevesearch/bleve/analysis/token/stop"
	"github.com/blevesearch/bleve/analysis/tokenizer/unicode"
	"github.com/blevesearch/bleve/analysis/tokenmap"
	blevemapping "github.com/blevesearch/bleve/mapping"
	"github.com/mitchellh/mapstructure"
	"github.com/sirupsen/logrus"
	"github.com/fsnotify/fsnotify"
	"io"
	"os"
	"sync"
	"strings"
	"time"
	//"golang.org/x/time/rate"
)

var log = logrus.New()

type Metadata struct {
	db		map[string]interface{}
	index		bleve.Index
	indexPath	string
	metadata	string
}

type entity struct {
	ID       string `json:"id"`
	EntityID string `json:"entityID"`
	Type     string `json:"type"`
	Title    string `json:"title"`
	Content  string `json:"content"`
}

func Sha1Id(entityID string) string {
	h := sha1.New()
	io.WriteString(h, entityID)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func NgramTokenFilter() map[string]interface{} {
	return map[string]interface{}{
		"type": edgengram.Name,
		"back": false,
		"min":  3.0,
		"max":  25.0,
	}
}

func StopWordsTokenMap() map[string]interface{} {
	return map[string]interface{}{
		"type": tokenmap.Name,
		"tokens": []interface{}{
			"a", "an", "of", "the", "die", "von", "av", "i", "identity", "provider", "university", "uni",
		},
	}
}

func StopWordsTokenFilter() map[string]interface{} {
	return map[string]interface{}{
		"type":           stop.Name,
		"stop_token_map": "stop_words_map",
	}
}

func NgramAnalyzer() map[string]interface{} {
	return map[string]interface{}{
		"type":      custom.Name,
		"tokenizer": unicode.Name,
		"token_filters": []string{
			lowercase.Name,
			"stop_words_filter",
			"ngram_tokenfilter",
		},
	}
}

func NewIndexMapping() blevemapping.IndexMapping {
	var err error
	mapping := bleve.NewIndexMapping()
	mapping.DefaultType = "entity"
	err = mapping.AddCustomTokenMap("stop_words_map", StopWordsTokenMap())
	if err != nil {
		log.Fatal(err)
	}
	err = mapping.AddCustomTokenFilter("stop_words_filter", StopWordsTokenFilter())
	if err != nil {
		log.Fatal(err)
	}
	err = mapping.AddCustomTokenFilter("ngram_tokenfilter", NgramTokenFilter())
	if err != nil {
		log.Fatal(err)
	}
	err = mapping.AddCustomAnalyzer("ngram_analyzer", NgramAnalyzer())
	if err != nil {
		log.Fatal(err)
	}
	entityMapping := bleve.NewDocumentMapping()
	mapping.AddDocumentMapping("entity", entityMapping)

	nopFieldMapping := bleve.NewTextFieldMapping()
	nopFieldMapping.Analyzer = keyword.Name
	nopFieldMapping.IncludeTermVectors = false
	nopFieldMapping.IncludeInAll = false
	nopFieldMapping.DocValues = false
	nopFieldMapping.Store = false
	nopFieldMapping.Index = false

	contentFieldMapping := bleve.NewTextFieldMapping()
	contentFieldMapping.Analyzer = "ngram_analyzer"
	entityMapping.AddFieldMappingsAt("Content", contentFieldMapping)

	titleFieldMapping := bleve.NewTextFieldMapping()
	titleFieldMapping.Analyzer = "standard"
	entityMapping.AddFieldMappingsAt("Title", titleFieldMapping)

	typeFieldMapping := bleve.NewTextFieldMapping()
	typeFieldMapping.Analyzer = keyword.Name
	entityMapping.AddFieldMappingsAt("Type", typeFieldMapping)

	entityMapping.AddFieldMappingsAt("ID", nopFieldMapping)
	entityMapping.AddFieldMappingsAt("EntityID", nopFieldMapping)
	return mapping
}

func (md *Metadata) LoadIndex(wg *sync.WaitGroup) {
	var err error
	rebuildIndex := false

	if md.index == nil {
		md.index, err = bleve.Open(md.indexPath)
		if err == bleve.ErrorIndexPathDoesNotExist {
			rebuildIndex = true
			//md.index, err = bleve.New(md.indexPath, NewIndexMapping())
			md.index, err = bleve.NewMemOnly(NewIndexMapping())
			if err != nil {
				log.Fatalf("failed to create index: %s", err)
			}
		} else if err != nil {
			log.Fatal(err)
		}
	}

	if rebuildIndex {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var count int = 0
			batch := md.index.NewBatch()
			batchCount := 0
			for _, data := range md.db {
				var je map[string]interface{} = data.(map[string]interface{})
				var e entity
			err = mapstructure.Decode(data, &e)
			if err != nil {
				log.Fatal(err)
			}
			var sb strings.Builder
			for key, value := range je {
				switch key {
				case
					"title",
					"scope",
					"keywords",
					"domain":
					v := strings.ReplaceAll(fmt.Sprintf("%s ", value), ",", " ")
					v = strings.ReplaceAll(v, ".", " ")
					v = strings.ReplaceAll(v, "+", " ")
					sb.WriteString(v)
				}
			}
			e.Content = sb.String()
			//log.Printf("%s -> %s", e.EntityID, e.Content)
			batch.Index(e.ID, e)
			batchCount++
			if batchCount >= *batchSize {
				err = md.index.Batch(batch)
				if err != nil {
					log.Fatal(err)
				}
				batch = md.index.NewBatch()
				batchCount = 0
			}

			count++
			}
			if batchCount > 0 {
				err = md.index.Batch(batch)
				if err != nil {
					log.Fatal(err)
				}
			}
			log.Printf("finished indexing %d documents", count)
		}()
	} else {
		wg.Add(1)
		go func() {
			defer (*wg).Done()
			log.Println("looking for removed items")
			query := bleve.NewMatchAllQuery()
			search := bleve.NewSearchRequest(query)
			sz, err :=  md.index.DocCount()
			if err != nil {
				log.Fatal("unable to re-index dataset - recommend manual clean")
			}
			search.Size = int(sz)
			searchResults, _ := md.index.Search(search)
			for _, hit := range searchResults.Hits {
				if md.db[hit.ID] == nil {
					log.Warn("removing gone entity %s", hit.ID)
					md.index.Delete(hit.ID)
				}
			}
			log.Printf("finished looking for removed items in %d documents", sz)
		}()
	}
}

func (md *Metadata) LoadDB() {
	f, _ := os.Open(md.metadata)
        decoder := jstream.NewDecoder(f, 1)
        db := make(map[string]interface{})
        for data := range decoder.Stream() {
                var je map[string]interface{} = data.Value.(map[string]interface{})
                entityID := je["entityID"].(string)
                id := Sha1Id(entityID)
                je["id"] = id
                db[id] = je
        }
        f.Close()
	log.Printf("loaded %d items into db", len(db))
	md.db = db
}

func NewMetadata(metadata, indexPath string) Metadata {
	md := &Metadata { indexPath: indexPath, metadata: metadata, }
	md.Reload()

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal("failed to setup watcher: %s", err)
	}
	defer watcher.Close()
	active := false

	//done := make(chan bool)
	go func() {
		for {
			select {
				case event, ok :=  <-watcher.Events:
					if ok && event.Op&fsnotify.Write == fsnotify.Write {
						if ! active {
							active = true
							go func() {
								log.Printf("got event %s - waiting for file to settle...", event)
								sleepDuration,_ := time.ParseDuration("3s")
								time.Sleep(sleepDuration)
								md.Reload()
								active = false
							}()
						}
					}
				case err, ok := <-watcher.Errors:
					if ok { // a bit counterintuitive... an "ok" error
						log.Warn(err)
					}
			}
		}
	}()

	err = watcher.Add(md.metadata)
	if err != nil {
                log.Fatal("failed to add %s to watchlist: %s", md.metadata, err)
        }
	//<-done

	log.Println("metadata initialized!")
	return *md
}

func (md *Metadata) Reload() {
	var wg sync.WaitGroup
	md.LoadDB()
	md.LoadIndex(&wg)
	wg.Wait()
}
