/*
Package warc provides methods for reading and writing WARC files (https://iipc.github.io/warc-specifications/) in Go.
This module is based on nlevitt's WARC module (https://github.com/nlevitt/warc).
*/
package warc

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/textproto"
	"os"
	"strings"
	"sync"
)

// RotatorSettings is used to store the settings
// needed by recordWriter to write WARC files
type RotatorSettings struct {
	// Content of the warcinfo record that will be written
	// to all WARC files
	WarcinfoContent Header
	// Prefix used for WARC filenames, WARC 1.1 specifications
	// recommend to name files this way:
	// Prefix-Timestamp-Serial-Crawlhost.warc.gz
	Prefix string
	// Compression algorithm to use
	Compression string
	// WarcSize is in MegaBytes
	WarcSize float64
	// Directory where the created WARC files will be stored,
	// default will be the current directory
	OutputDirectory string
}

type recorder struct {
	r *rotator
	c *http.Client
}

func (r *recorder) Close() {
	r.r.Close()
}

// NewRecorder creates a Recorder that records the transmissions over a transmission wrapper
func NewRecorder(settings *RotatorSettings) (*recorder, error) {
	rot, err := settings.NewWARCRotator()
	if err != nil {
		return nil, err
	}

	r := &recorder{
		r: rot,
	}

	r.c = &http.Client{
		Transport: NewRoundTripper(r.rawResponseCallback),
	}

	return r, nil
}

func (r *recorder) Client() *http.Client {
	return r.c
}

func headersFromRawData(r *Record, data []byte) {
	pattern := []byte{0x0d, 0x0a, 0x0d, 0x0a}

	var pos int
	for i := range data {
		if len(data) < i+4 {
			panic("cant find header data")
		}

		if bytes.Equal(data[i:i+4], pattern) {
			pos = i + 4
			break
		}
	}

	if pos == 0 {
		panic("cant find header data")
	}

	reader := textproto.NewReader(bufio.NewReader(bytes.NewReader(data[:pos])))
	_, err := reader.ReadLine()
	if err != nil {
		panic(err)
	}

	mimeHeader, err := reader.ReadMIMEHeader()
	if err != nil {
		panic(err)
	}

	for k, v := range mimeHeader {
		r.Header.Set(k, strings.Join(v, ","))
	}
}

func readReaderIfNotNil(r io.ReadCloser) []byte {
	if r == nil {
		return []byte{}
	}

	all, err := ioutil.ReadAll(r)
	if err != nil {
		panic(err)
	}

	if err := r.Close(); err != nil {
		panic(err)
	}

	return all
}

func (r *recorder) rawResponseCallback(req *http.Request, resp *http.Response, reqData []byte, respData []byte) {
	var batch = NewRecordBatch()

	// Add the response to the exchange
	var responseRecord = NewRecord()
	headersFromRawData(responseRecord, respData)
	responseRecord.Header.Set("WARC-Type", "response")
	responseRecord.Header.Set("WARC-Payload-Digest", "sha1:"+GetSHA1(respData))
	responseRecord.Header.Set("WARC-Target-URI", req.URL.String())
	responseRecord.Header.Set("Content-Type", "application/http; msgtype=response")

	respBody := readReaderIfNotNil(resp.Body)
	if resp.Body != nil {
		resp.Body = ioutil.NopCloser(bytes.NewReader(respBody))
	}
	responseRecord.Content = bytes.NewReader(respBody)

	// Add the request to the exchange
	var requestRecord = NewRecord()
	headersFromRawData(requestRecord, reqData)
	requestRecord.Header.Set("WARC-Type", "request")
	requestRecord.Header.Set("WARC-Payload-Digest", "sha1:"+GetSHA1(reqData))
	requestRecord.Header.Set("WARC-Target-URI", req.URL.String())
	requestRecord.Header.Set("Host", req.URL.Host)
	requestRecord.Header.Set("Content-Type", "application/http; msgtype=request")

	reqBody := readReaderIfNotNil(req.Body)
	if req.Body != nil {
		req.Body = ioutil.NopCloser(bytes.NewReader(reqBody))
	}
	requestRecord.Content = bytes.NewReader(reqBody)

	// Append records to the record batch
	batch.Records = append(batch.Records, responseRecord, requestRecord)

	r.r.ch <- batch
}

// RecordsFromHTTPResponse turns a http.Response into a warc.RecordBatch
// filling both Response and Request records
func RecordsFromHTTPResponse(response *http.Response) (*RecordBatch, error) {
	var batch = NewRecordBatch()

	// Dump response
	responseDump, err := httputil.DumpResponse(response, true)
	if err != nil {
		return batch, err
	}

	// Add the response to the exchange
	var responseRecord = NewRecord()
	responseRecord.Header.Set("WARC-Type", "response")
	responseRecord.Header.Set("WARC-Payload-Digest", "sha1:"+GetSHA1(responseDump))
	responseRecord.Header.Set("WARC-Target-URI", response.Request.URL.String())
	responseRecord.Header.Set("Content-Type", "application/http; msgtype=response")

	responseRecord.Content = strings.NewReader(string(responseDump))

	// Dump request
	requestDump, err := httputil.DumpRequestOut(response.Request, true)
	if err != nil {
		return batch, err
	}

	// Add the request to the exchange
	var requestRecord = NewRecord()
	requestRecord.Header.Set("WARC-Type", "request")
	requestRecord.Header.Set("WARC-Payload-Digest", "sha1:"+GetSHA1(requestDump))
	requestRecord.Header.Set("WARC-Target-URI", response.Request.URL.String())
	requestRecord.Header.Set("Host", response.Request.URL.Host)
	requestRecord.Header.Set("Content-Type", "application/http; msgtype=request")

	requestRecord.Content = strings.NewReader(string(requestDump))

	// Append records to the record batch
	batch.Records = append(batch.Records, responseRecord, requestRecord)

	return batch, nil
}

type rotator struct {
	wg sync.WaitGroup
	ch chan *RecordBatch
}

// NewWARCRotator creates and return a channel that can be used
// to communicate records to be written to WARC files to the
// recordWriter function running in a goroutine
func (s *RotatorSettings) NewWARCRotator() (*rotator, error) {
	// Check the rotator settings, also set default values
	err := checkRotatorSettings(s)
	if err != nil {
		return nil, err
	}

	r := &rotator{
		ch: make(chan *RecordBatch),
	}

	// Start the record writer in a goroutine
	// TODO: support for pool of recordWriter?
	r.wg.Add(1)
	go recordWriter(s, r.ch, &r.wg)

	return r, nil
}

func (r *rotator) Close() {
	close(r.ch)
	r.wg.Wait()
}

func (r *rotator) Chan() chan *RecordBatch {
	return r.ch
}

func recordWriter(settings *RotatorSettings, records chan *RecordBatch, s *sync.WaitGroup) {
	defer s.Done()

	var serial = 1
	var currentFileName string = generateWarcFileName(settings.Prefix, settings.Compression, serial)
	var currentWarcinfoRecordID string

	// Create and open the initial file
	warcFile, err := os.Create(settings.OutputDirectory + currentFileName)
	if err != nil {
		panic(err)
	}

	// Initialize WARC writer
	warcWriter, err := NewWriter(warcFile, currentFileName, settings.Compression)
	if err != nil {
		panic(err)
	}

	// Write the info record
	currentWarcinfoRecordID, err = warcWriter.WriteInfoRecord(settings.WarcinfoContent)
	if err != nil {
		panic(err)
	}

	// If compression is enabled, we close the record's GZIP chunk
	if settings.Compression != "" {
		if settings.Compression == "GZIP" {
			warcWriter.gzipWriter.Close()
			warcWriter, err = NewWriter(warcFile, currentFileName, settings.Compression)
			if err != nil {
				panic(err)
			}
		} else if settings.Compression == "ZSTD" {
			warcWriter.zstdWriter.Close()
			warcWriter, err = NewWriter(warcFile, currentFileName, settings.Compression)
			if err != nil {
				panic(err)
			}
		}
	}

	for recordBatch := range records {
		if isFileSizeExceeded(settings.OutputDirectory+currentFileName, settings.WarcSize) {
			// WARC file size exceeded settings.WarcSize
			// The WARC file is renamed to remove the .open suffix
			err := os.Rename(settings.OutputDirectory+currentFileName, strings.TrimSuffix(settings.OutputDirectory+currentFileName, ".open"))
			if err != nil {
				panic(err)
			}

			// We flush the data and close the file
			warcWriter.fileWriter.Flush()
			if settings.Compression != "" {
				if settings.Compression == "GZIP" {
					warcWriter.gzipWriter.Close()
				} else if settings.Compression == "ZSTD" {
					warcWriter.zstdWriter.Close()
				}
			}
			warcFile.Close()

			// Increment the file's serial number, then create the new file
			serial++
			currentFileName = generateWarcFileName(settings.Prefix, settings.Compression, serial)
			warcFile, err = os.Create(settings.OutputDirectory + currentFileName)
			if err != nil {
				panic(err)
			}

			// Initialize new WARC writer
			warcWriter, err = NewWriter(warcFile, currentFileName, settings.Compression)
			if err != nil {
				panic(err)
			}

			// Write the info record
			currentWarcinfoRecordID, err := warcWriter.WriteInfoRecord(settings.WarcinfoContent)
			if err != nil {
				panic(err)
			}
			_ = currentWarcinfoRecordID

			// If compression is enabled, we close the record's GZIP chunk
			if settings.Compression != "" {
				if settings.Compression == "GZIP" {
					warcWriter.gzipWriter.Close()
				} else if settings.Compression == "ZSTD" {
					warcWriter.zstdWriter.Close()
				}
			}
		}

		// Write all the records of the record batch
		for _, record := range recordBatch.Records {
			warcWriter, err = NewWriter(warcFile, currentFileName, settings.Compression)
			if err != nil {
				panic(err)
			}

			record.Header.Set("WARC-Date", recordBatch.CaptureTime)
			record.Header.Set("WARC-Warcinfo-ID", "<urn:uuid:"+currentWarcinfoRecordID+">")

			_, err := warcWriter.WriteRecord(record)
			if err != nil {
				panic(err)
			}

			// If compression is enabled, we close the record's GZIP chunk
			if settings.Compression != "" {
				if settings.Compression == "GZIP" {
					warcWriter.gzipWriter.Close()
				} else if settings.Compression == "ZSTD" {
					warcWriter.zstdWriter.Close()
				}
			}
		}
		warcWriter.fileWriter.Flush()
	}

	// Channel has been closed
	// We flush the data, close the file, and rename it
	warcWriter.fileWriter.Flush()
	if settings.Compression != "" {
		if settings.Compression == "GZIP" {
			warcWriter.gzipWriter.Close()
		} else if settings.Compression == "ZSTD" {
			warcWriter.zstdWriter.Close()
		}
	}
	warcFile.Close()

	// The WARC file is renamed to remove the .open suffix
	err = os.Rename(settings.OutputDirectory+currentFileName, strings.TrimSuffix(settings.OutputDirectory+currentFileName, ".open"))
	if err != nil {
		panic(err)
	}
}
