package main

import (
	"flag"
	//	"github.com/davecheney/profile"
	"log"
	"os"
)

type Any interface{}

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

// ES Bulk op
type Bulk struct {
	Id   string
	Type string
	Doc  []byte
}

type Batcher struct {
	batch []Bulk
	size  int
	dest  chan<- []Bulk
}

func (this *Batcher) Put(b Bulk) {
	this.batch = append(this.batch, b)
	if len(this.batch) == this.size {
		this.Flush()
	}
}

func (this *Batcher) Flush() {
	if len(this.batch) != 0 {
		this.dest <- this.batch
		this.batch = []Bulk{}
	}
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

type DataSource interface {
	GetIndex() (string, error)
	StreamTo(window int, dest chan []Bulk)
}

type DataSink interface {
	PutIndex(meta string, repls, shards int) error
	DeleteIndex() error
	AcceptFrom(src chan []Bulk) error
}

type DataFlow interface {
	DataSink
	DataSource
}

func Copy(src DataSource, sink DataSink, p Params) error {
	index, err := src.GetIndex()
	if err != nil {
		return err
	}
	if p.force {
		sink.DeleteIndex()
	}
	err = sink.PutIndex(index, p.repls, p.shards)
	if err != nil {
		return err
	}
	pipe := make(chan []Bulk, 10)
	go src.StreamTo(p.window, pipe)
	return sink.AcceptFrom(pipe)
}

func exportTask(p Params) (err error) {
	log.Printf("Export %s --> %s\n", p.in, p.out)
	if !p.force {
		_, err := os.Open(p.out)
		if err == nil {
			log.Fatalf("File `%s` already exits, use --force to overwrite.", p.out)
		}
	}
	sink, err := NewFileSink(p.out)
	if err != nil {
		return
	}
	es, err := ConnectES(p.in)
	if err != nil {
		return
	}
	return Copy(es, sink, p)
}

func importTask(p Params) (err error) {
	log.Printf("Import %s --> %s", p.in, p.out)
	src, err := NewFileSource(p.in)
	if err != nil {
		return
	}
	es, err := ConnectES(p.out)
	if err != nil {
		return
	}
	return Copy(src, es, p)
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
	return Copy(es1, es2, p)
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
