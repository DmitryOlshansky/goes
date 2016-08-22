package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	neturl "net/url"
	"strconv"
	"strings"
	"sync"
)

// Connection to a specific index in ES
type EsConn struct {
	hostPrefix string
	path       string
	types      []string
	client     *http.Client
}

var _ DataFlow = &EsConn{}

// http helper - check status and read the whole response body
func readRequest(client *http.Client, req *http.Request) (body []byte, err error) {
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	if resp.StatusCode >= 300 {
		err = errors.New(fmt.Sprintf("Http error[%s]: %s", resp.Status, string(body)))
	}
	return
}

// set of _id that failed with anything but document exists
func parseBulkFailures(resp []byte) (fails map[string]bool, err error) {
	fails = map[string]bool{}
	var respObj struct {
		Errors bool `json:"errors"`
		Items  []struct {
			Create struct {
				Id     string `json:"_id"`
				Status int    `json:"status"`
			} `json:"create"`
		} `json:"items"`
	}
	err = json.Unmarshal(resp, &respObj)
	if err != nil {
		return
	}
	if respObj.Errors {
		for _, item := range respObj.Items {
			if item.Create.Status != 201 {
				if item.Create.Status == 429 || item.Create.Status == 503 {
					fails[item.Create.Id] = true
				} else if item.Create.Status == 409 {
					// ignore already exists error
				} else {
					log.Printf("Failed %s with %d", item.Create.Id, item.Create.Status)
				}
			}
		}
	}
	return fails, nil
}

func ConnectES(url string) (*EsConn, error) {
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "http://" + url
	}
	u, err := neturl.ParseRequestURI(url)
	if err != nil {
		return nil, err
	}
	hostPrefix := fmt.Sprintf("http://%s", u.Host)
	return &EsConn{hostPrefix: hostPrefix, path: u.Path, client: http.DefaultClient}, nil
}

func (this *EsConn) NewScroll(typeName string, size int) (scroll Scroll, err error) {
	query := fmt.Sprintf(`
        {
            "query": {
            	"match_all": {}
            },
            "size": %d
        }
    `, size)
	url := fmt.Sprintf("%s%s/%s/_search?search_type=scan&scroll=5m", this.hostPrefix, this.path, typeName)
	request, err := http.NewRequest("GET", url, bytes.NewBufferString(query))
	if err != nil {
		return
	}
	resp, err := readRequest(this.client, request)
	if err != nil {
		return
	}
	//
	getScrollId := func(data []byte) (string, error) {
		v := struct {
			ScrollID string `json:"_scroll_id"`
		}{}
		err := json.Unmarshal(data, &v)
		return v.ScrollID, err
	}

	scrollID, err := getScrollId(resp)
	if err != nil {
		return
	}
	scroll = Scroll{id: scrollID, es: this}
	return
}

func (this *Scroll) Next() ([]map[string]Any, error) {
	resp, err := this.es.client.Get(this.es.hostPrefix + "/_search/scroll?scroll=5m&scroll_id=" + this.id)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, errRead := ioutil.ReadAll(resp.Body)
	if errRead != nil {
		return nil, errRead
	}

	hits := struct {
		Hits struct {
			Hits []map[string]Any `json:"hits"`
		} `json:"hits"`
	}{}
	json.Unmarshal(data, &hits)
	return hits.Hits.Hits, errRead
}

// returns failed bulk ops if any single one failed; error on major filure
func (this *EsConn) Bulk(ops []Bulk) (ret []Bulk, err error) {
	bodyBuf := bytes.Buffer{}
	for _, b := range ops {
		b.Store(&bodyBuf)
	}
	url := fmt.Sprintf("%s%s/_bulk", this.hostPrefix, this.path)
	request, err := http.NewRequest("PUT", url, &bodyBuf)
	if err != nil {
		return nil, err
	}
	resp, err := readRequest(this.client, request)
	if err != nil {
		return
	}
	fails, err := parseBulkFailures(resp)
	if err != nil {
		return
	}
	if len(fails) > 0 {
		for _, op := range ops {
			if _, ok := fails[op.Id]; ok {
				ret = append(ret, op)
			}
		}
	}
	return
}

// delete index of EsConn
func (this *EsConn) DeleteIndex() (err error) {
	req, err := http.NewRequest("DELETE", this.hostPrefix+this.path, bytes.NewReader([]byte{}))
	if err != nil {
		return
	}
	_, err = readRequest(this.client, req)
	return
}

func (this *EsConn) PutIndex(metaString string, repls int, shards int) (err error) {
	// meta - all metadata including the old index name (top-level)
	meta := make(map[string]Index)
	err = json.Unmarshal([]byte(metaString), &meta)
	if err != nil {
		return
	}
	var oldIndexName string
	for k := range meta { // FIXME: iterating over map of 1 item to get the only key ?
		oldIndexName = k
	}
	this.types = nil
	for k := range meta[oldIndexName].Mappings {
		this.types = append(this.types, k)
	}
	log.Printf("Types %s", this.types)
	metaVal := meta[oldIndexName]
	metaVal.Aliases = map[string]Any{} // clear all aliases
	// metaBlob - all index meta data
	if repls >= 0 {
		metaVal.Settings.Index.NumRepls = strconv.Itoa(repls)
	}
	if repls >= 0 {
		metaVal.Settings.Index.NumShards = strconv.Itoa(shards)
	}

	metaBlob, err := json.Marshal(metaVal)
	if err != nil {
		return
	}

	reqMapping, err := http.NewRequest("PUT", this.hostPrefix+this.path, bytes.NewBuffer(metaBlob))
	if err != nil {
		return
	}
	_, err = readRequest(this.client, reqMapping)
	return
}

func (this *EsConn) GetIndex() (string, error) {
	resp, err := this.client.Get(this.hostPrefix + this.path)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	meta := make(map[string]Index)
	err = json.Unmarshal(data, &meta)
	if err != nil {
		return "", err
	}
	indexName := ""
	for k := range meta { // FIXME: iterating over map of 1 item to get the only key ?
		indexName = k
	}
	if err != nil {
		return "", err
	}
	this.types = nil
	log.Print(indexName)
	for k := range meta[indexName].Mappings {
		this.types = append(this.types, k)
	}
	metaVal := meta[indexName]
	// clear aliases
	metaVal.Aliases = map[string]Any{} // clear all aliases
	meta[indexName] = metaVal
	log.Printf("%v", this.types)
	if resp.StatusCode >= 400 {
		log.Fatalf("Elastic error[%s]: %s", resp.Status, string(data))
	}
	data, err = json.Marshal(meta)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (this *EsConn) readBulk(typeName string, window, bulkSize int, dest chan<- []Bulk) {
	log.Printf("Exporting type: `%s`", typeName)
	scroll, err := this.NewScroll(typeName, window)
	if err != nil {
		panic(err)
	}
	batcher := Batcher{size: bulkSize, dest: dest}
	defer batcher.Flush()
	for hits, err := scroll.Next(); len(hits) != 0 && err == nil; hits, err = scroll.Next() {
		log.Printf("Fetched %d\n", len(hits))
		for _, h := range hits {
			bytes, err := json.Marshal(h["_source"])
			if err != nil {
				panic(err)
			}
			batcher.Put(Bulk{Id: h["_id"].(string), Type: typeName, Doc: bytes})
		}
	}
}

func (this *EsConn) StreamTo(window, bulkSize int, dest chan []Bulk) {
	defer close(dest)
	for _, t := range this.types {
		this.readBulk(t, window, bulkSize, dest)
	}
	return
}

func (this *EsConn) AcceptFrom(parallel int, src chan []Bulk) (err error) {
	var group sync.WaitGroup
	group.Add(parallel)
	for i := 0; i < parallel; i++ {
		go func() {
			var leftover []Bulk
			for batch := range src {
				batch = append(batch, leftover...)
				leftover, err = this.Bulk(batch)
				if err != nil {
					panic(err)
				}
				log.Printf("Imported %d/%d", len(batch)-len(leftover), len(batch))
			}
			for len(leftover) > 0 {
				log.Printf("Pushing in last %d", len(leftover))
				leftover, err = this.Bulk(leftover)
				if err != nil {
					panic(err)
				}
			}
			group.Done()
		}()
	}
	group.Wait()
	return
}
