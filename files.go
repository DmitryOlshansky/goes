package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type FileSource struct {
	src    *os.File
	reader *bufio.Reader
}

var _ DataSource = &FileSource{}

func ParseBulk(src *bufio.Reader) (Bulk, error) {
	op, err := src.ReadBytes('\n')
	if err != nil {
		return Bulk{}, err
	}
	var bulkOp struct {
		Create struct {
			Id   string `json:"_id"`
			Type string `json:"_type"`
		} `json:"create"`
	}
	err = json.Unmarshal(op, &bulkOp)
	if err != nil {
		return Bulk{}, err
	}
	item, err := src.ReadBytes('\n')
	return Bulk{Id: bulkOp.Create.Id, Type: bulkOp.Create.Type, Doc: item}, err
}

func (bulk Bulk) Store(wrt io.Writer) error {
	const template = `{ "create" : { "_id" : "%v", "_type": "%v" } }` + "\n%s\n"
	_, err := fmt.Fprintf(wrt, template, bulk.Id, bulk.Type, bulk.Doc)
	return err
}

func NewFileSource(in string) (*FileSource, error) {
	src, err := os.Open(in)
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReader(src)
	return &FileSource{src: src, reader: reader}, nil
}

func (this *FileSource) GetIndex() (string, error) {
	return this.reader.ReadString('\n')
}

func (this *FileSource) StreamTo(window, bulkSize int, dest chan []Bulk) {
	defer close(dest)
	defer this.src.Close()
	batcher := Batcher{size: bulkSize, dest: dest}
	defer batcher.Flush()
	for {
		bulk, err := ParseBulk(this.reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		batcher.Put(bulk)
	}
}

type FileSink struct {
	sink *os.File
}

func NewFileSink(out string) (*FileSink, error) {
	sink, err := os.Create(out)
	if err != nil {
		return nil, err
	}
	return &FileSink{sink: sink}, nil
}

func (this *FileSink) PutIndex(meta string, repls, shards int) error {
	// FIXME: should parse & rewrite shards and repls
	_, err := fmt.Fprintf(this.sink, "%s\n", meta)
	return err
}

func (this *FileSink) AcceptFrom(parallel int, src chan []Bulk) error {
	defer this.sink.Close()
	for batch := range src {
		for _, b := range batch {
			err := b.Store(this.sink)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (this *FileSink) DeleteIndex() error { return nil }

var _ DataSink = &FileSink{}
