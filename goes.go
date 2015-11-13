package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	//	"github.com/davecheney/profile"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	neturl "net/url"
	"os"
	"strconv"
	"strings"
)

type Any interface{}

// Connection to a specific index in ES
type EsConn struct {
	hostPrefix string
	path       string
	types      []string
	client     *http.Client
}

// ES scroll state - base64 of Id + connection
type Scroll struct {
	id string
	es *EsConn
}

// Index metadata
type Index struct {
	Aliases  map[string]Any `json:"aliases"`
	Mappings map[string]Any `json:"mappings"`
	Settings IndexSettings  `json:"settings"`
	Warmers  map[string]Any `json:"warmers"`
}

type IndexSettings struct {
	Index struct {
		Cache      Any    `json:"cache"`
		NumShards  string `json:"number_of_shards"`
		NumRepls   string `json:"number_of_replicas"`
		Refresh    Any    `json:"refresh_interval"`
		Similarity Any    `json:"similarity"`
		Analysis   Any    `json:"analysis"`
		TransLog   Any    `json:"translog"`
		Uuid       Any    `json:"uuid"`
		Version    Any    `json:"version"`
	} `json:"index"`
}

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
	url := fmt.Sprintf("%s%s/%s/_search?search_type=scan&scroll=1m", this.hostPrefix, this.path, typeName)
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

func (this *EsConn) Bulk(bulk []string) (err error) {
	bodyStr := strings.Join(bulk, "")
	body := bytes.NewBufferString(bodyStr)
	url := fmt.Sprintf("%s%s/_bulk", this.hostPrefix, this.path)
	request, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return err
	}
	_, err = readRequest(this.client, request)
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

// Returns name of the first type in the mapping (that's what we use - FIXME for more general ways)
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
	log.Printf("%v", this.types)
	if resp.StatusCode >= 400 {
		log.Fatalf("Elastic error[%s]: %s", resp.Status, string(data))
	}
	return string(data), nil
}

func (this Scroll) Next() ([]map[string]Any, error) {
	resp, err := this.es.client.Get(this.es.hostPrefix + "/_search/scroll?scroll=1m&scroll_id=" + this.id)
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

func (this *EsConn) readBulk(typeName string, window int, dest chan []string) error {
	log.Printf("Exporting type: `%s`", typeName)
	scroll, err := this.NewScroll(typeName, window)
	if err != nil {
		return err
	}
	var batch []string
	template := `{ "create" : { "_id" : "%v", "_type": "%v" } }` + "\n%s\n"
	for hits, err := scroll.Next(); len(hits) != 0 && err == nil; hits, err = scroll.Next() {
		log.Printf("Exported %d\n", len(hits))
		for _, h := range hits {
			bytes, err := json.Marshal(h["_source"])
			if err != nil {
				return err
			}
			// create is faster then index but must avoid collision (index is empty at start)
			bulk := fmt.Sprintf(template, h["_id"], typeName, string(bytes))
			batch = append(batch, bulk)
		}
		dest <- batch
		batch = batch[:0]
	}
	return nil
}

func (this *EsConn) StreamTo(window int, dest chan []string) (err error) {
	defer close(dest)
	for _, t := range this.types {
		err = this.readBulk(t, window, dest)
		if err != nil {
			return
		}
	}
	return
}

func (this *EsConn) AcceptFrom(src chan []string) (err error) {
	for batch := range src {
		err = this.Bulk(batch)
		if err != nil {
			return
		}
		log.Printf("Imported %d", len(batch)/2)
	}
	return
}

func Reader(src *bufio.Reader, window int, dest chan []string) {
	batch := []string{}
	defer close(dest)
	for {
		item, err := src.ReadString('\n')
		if err == io.EOF {
			dest <- batch
			break
		}
		batch = append(batch, item)
		if len(batch) == window*2 {
			dest <- batch
			batch = batch[:0]
		}
	}

}

func Writer(src chan []string, sink io.Writer) (err error) {
	for batch := range src {
		for _, b := range batch {
			_, err = io.WriteString(sink, b)
			if err != nil {
				return
			}
		}
	}
	return
}

func exportTask(p Params) error {
	log.Printf("Export %s --> %s\n", p.in, p.out)
	if !p.force {
		_, err := os.Open(p.out)
		if err == nil {
			log.Fatalf("File `%s` already exits, use --force to overwrite.", p.out)
		}
	}
	sink, err := os.Create(p.out)
	if err != nil {
		return err
	}
	defer sink.Close()
	es, err := ConnectES(p.in)
	if err != nil {
		return err
	}
	index, err := es.GetIndex()
	if err != nil {
		return err
	}
	_, err = sink.WriteString(index + "\n")
	if err != nil {
		return err
	}

	pipe := make(chan []string, 10)
	go es.StreamTo(p.window, pipe)
	return Writer(pipe, sink)
}

func importTask(p Params) (err error) {
	log.Printf("Import %s --> %s", p.in, p.out)
	src, err := os.Open(p.in)
	if err != nil {
		return
	}
	defer src.Close()
	reader := bufio.NewReader(src)
	index, err := reader.ReadString('\n')
	es, err := ConnectES(p.out)
	if err != nil {
		return
	}
	if p.force {
		err = es.DeleteIndex()
		if err != nil {
			return
		}
	}
	err = es.PutIndex(index, p.repls, p.shards)
	if err != nil {
		return
	}
	pipe := make(chan []string, 10)
	go Reader(reader, p.window, pipe)
	return es.AcceptFrom(pipe)
}

func copyTask(p Params) (err error) {
	log.Printf("Copy %s --> %s\n", p.in, p.out)
	es1, err := ConnectES(p.in)
	if err != nil {
		return
	}
	es2, err := ConnectES(p.out)
	if err != nil {
		return
	}
	index, err := es1.GetIndex()
	if err != nil {
		return
	}
	if p.force {
		_ = es2.DeleteIndex()
	}
	err = es2.PutIndex(index, p.repls, p.shards)
	pipe := make(chan []string, 10)
	go es1.StreamTo(p.window, pipe)
	return es2.AcceptFrom(pipe)
}

type Params struct {
	in     string
	out    string
	window int
	repls  int
	shards int
	force  bool
}

type Command func(Params) error

/**
Commands:
	./estool export --in <url-of-index> --out <path-to-store>
	./estool import --in <path-to-store> --out <url-to-new-index>
	./estool copy --in <url-of-src-index> --out <url-out-index>
*/
func main() {
	// defer profile.Start(profile.CPUProfile).Stop()
	commandTab := map[string]Command{
		"export": exportTask,
		"import": importTask,
		"copy":   copyTask,
	}
	args := os.Args
	commands := flag.NewFlagSet("ES-tool", flag.ExitOnError)
	input := commands.String("in", "", "input path/URL")
	output := commands.String("out", "", "output path/URL")
	force := commands.Bool("force", false, "force overwrite of existing index/file")
	window := commands.Int("window", 100, "size of scroll/scan window")
	repls := commands.Int("repls", -1, "override number of relicas (import only)")
	shards := commands.Int("shards", -1, "override number of shards (import only)")

	if len(args) == 1 {
		log.Printf("No command supplied, valid commands :\n")
		for c, _ := range commandTab {
			log.Printf("\t%s\n", c)
		}
		return
	}
	cmd := args[1]
	task, ok := commandTab[cmd]
	if !ok {
		log.Fatalf("Invalid command: %s\n", cmd)
	}
	commands.Parse(args[2:])
	if *input == "" {
		log.Fatalf("No in param provided")
	}
	if *output == "" {
		log.Fatalf("No out param provided")
	}
	err := task(Params{in: *input, out: *output, window: *window, force: *force, repls: *repls, shards: *shards})
	if err != nil {
		log.Fatal(err)
	}
}
