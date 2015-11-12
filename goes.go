package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	neturl "net/url"
	"os"
	"strings"
)

// ES scroll state - id + client/host string
type Scroll struct {
	id         string
	hostPrefix string
	client     *http.Client
}

// Connection to a specific index in ES
type EsConn struct {
	hostPrefix string
	path       string
	client     *http.Client
}

// Index metadata
type Index struct {
	Aliases  map[string]interface{} `json:"aliases"`
	Mappings map[string]interface{} `json:"mappings"`
	Settings map[string]interface{} `json:"settings"`
	Warmers  map[string]interface{} `json:"warmers"`
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
		log.Fatalf("Http error[%s]: %s", resp.Status, string(body))
	}
	return
}

func ConnectES(url string) (EsConn, error) {
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "http://" + url
	}
	u, err := neturl.ParseRequestURI(url)
	if err != nil {
		return EsConn{}, err
	}
	hostPrefix := fmt.Sprintf("http://%s", u.Host)
	return EsConn{hostPrefix: hostPrefix, path: u.Path, client: http.DefaultClient}, nil
}

func (this EsConn) NewScroll(size int) (scroll Scroll, err error) {
	query := fmt.Sprintf(`
        {
            "query": {
            	"match_all": {}
            },
            "size": %d
        }
    `, size)
	url := fmt.Sprintf("%s%s/_search?search_type=scan&scroll=1m", this.hostPrefix, this.path)
	request, err := http.NewRequest("GET", url, bytes.NewBufferString(query))
	if err != nil {
		return Scroll{}, err
	}
	resp, err := readRequest(this.client, request)
	if err != nil {
		return
	}
	scrollID, err := getScrollId(resp)
	if err != nil {
		return
	}
	scroll = Scroll{id: scrollID, hostPrefix: this.hostPrefix, client: this.client}
	return
}

func getScrollId(data []byte) (string, error) {
	v := struct {
		ScrollID string `json:"_scroll_id"`
	}{}
	err := json.Unmarshal(data, &v)
	return v.ScrollID, err
}

func (this EsConn) Bulk(typeName string, bulk []string) (err error) {
	bodyStr := strings.Join(bulk, "")
	body := bytes.NewBufferString(bodyStr)
	url := fmt.Sprintf("%s%s/%s/_bulk", this.hostPrefix, this.path, typeName)
	log.Print("Bulk: ", url)
	log.Print("Bulk body: ", bodyStr)
	request, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return err
	}
	_, err = readRequest(this.client, request)
	return
}

// delete index of EsConn
func (this EsConn) DeleteIndex() (err error) {
	req, err := http.NewRequest("DELETE", this.hostPrefix+this.path, bytes.NewReader([]byte{}))
	if err != nil {
		return
	}
	_, err = readRequest(this.client, req)
	return
}

// Returns name of the first type in the mapping (that's what we use - FIXME for more general ways)
func (this EsConn) PutIndex(metaString string) (string, error) {
	// meta - all metadata including the old index name (top-level)
	meta := make(map[string]Index)
	err := json.Unmarshal([]byte(metaString), &meta)
	if err != nil {
		return "", err
	}
	var oldIndexName string
	for k := range meta { // FIXME: iterating over map of 1 item to get the only key ?
		oldIndexName = k
	}
	var typeName string
	for k := range meta[oldIndexName].Mappings {
		typeName = k
	}
	log.Printf("Transfering index `%s`", oldIndexName)
	// metaBlob - all index meta data
	metaBlob, err := json.Marshal(meta[oldIndexName])
	if err != nil {
		return "", err
	}
	reqMapping, err := http.NewRequest("PUT", this.hostPrefix+this.path, bytes.NewBuffer(metaBlob))
	if err != nil {
		return "", err
	}
	_, err = readRequest(this.client, reqMapping)
	return typeName, err
}

func (this EsConn) GetIndex() (string, error) {
	resp, err := this.client.Get(this.hostPrefix + this.path)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= 400 {
		log.Fatalf("Elastic error[%s]: %s", resp.Status, string(data))
	}
	return string(data), nil
}

func (this Scroll) Next() ([]map[string]interface{}, error) {
	resp, err := this.client.Get(this.hostPrefix + "/_search/scroll?scroll=1m&scroll_id=" + this.id)
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
			Hits []map[string]interface{} `json:"hits"`
		} `json:"hits"`
	}{}
	json.Unmarshal(data, &hits)
	return hits.Hits.Hits, errRead
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
	_, err = sink.WriteString(index)
	if err != nil {
		return err
	}
	_, err = sink.WriteString("\n")
	if err != nil {
		return err
	}

	scroll, err := es.NewScroll(p.window)
	if err != nil {
		return err
	}

	for hits, err := scroll.Next(); len(hits) != 0 && err == nil; hits, err = scroll.Next() {
		log.Printf("Got %d docs.\n", len(hits))
		for _, h := range hits {
			id := fmt.Sprintf("{ \"index\" : { \"_id\" : \"%v\" } }\n", h["_id"])
			io.WriteString(sink, id)
			bytes, err := json.Marshal(h["_source"])
			if err != nil {
				return err
			}
			_, err = sink.Write(bytes)
			io.WriteString(sink, "\n")
			if err != nil {
				return err
			}
		}
	}

	return err
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
	typeName, err := es.PutIndex(index)
	if err != nil {
		return
	}
	log.Printf("Type: `%v`", typeName)
	batch := []string{}
	for {
		item, err := reader.ReadString('\n')
		if err == io.EOF {
			es.Bulk(typeName, batch)
			break
		}
		batch = append(batch, item)
		if len(batch) == p.window*2 {
			es.Bulk(typeName, batch)
			batch = batch[:0]
		}
	}
	return
}

func copyTask(p Params) error {
	log.Printf("Copy %s --> %s\n", p.in, p.out)
	return nil
}

type Params struct {
	in     string
	out    string
	window int
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
	window := commands.Int("window", 10, "size of scroll/scan window")

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
	err := task(Params{in: *input, out: *output, window: *window, force: *force})
	if err != nil {
		log.Fatal(err)
	}
}
