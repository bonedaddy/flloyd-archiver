package downloader

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/panjf2000/ants/v2"
	"go.bobheadxi.dev/zapx/zapx"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// URL is the path to the CSV file
	URL = "https://raw.githubusercontent.com/2020PB/police-brutality/data_build/all-locations.csv"
)

// Downloader downloads the media contained in the csv file
type Downloader struct {
	path   string
	logger *zap.Logger
	// enables running concurrent downloads
	wp    *ants.Pool
	count *atomic.Int64
}

// New returns a new downloader
func New(logFile, path string, concurrency int) *Downloader {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.Mkdir(path, os.FileMode(0775)); err != nil {
			panic(err)
		}
	}
	logger, err := zapx.New(logFile, false)
	if err != nil {
		panic(err)
	}
	wp, err := ants.NewPool(concurrency)
	if err != nil {
		panic(err)
	}
	return &Downloader{path, logger, wp, atomic.NewInt64(0)}
}

// Run starts the download process
func (d *Downloader) Run(timeout time.Duration) error {
	resp, err := http.Get(URL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	reader := csv.NewReader(resp.Body)
	/* rows:
	0    , 1     , 2  , 3  , 4  , 5       , 6    , 7    ,  8   , 9    , 10   , 11  ,  12   , 13
	state,edit_at,city,name,date,date_text,Link 1,Link 2,Link 3,Link 4,Link 5,Link 6,Link 7,Link 8
	*/
	i := 0
	for {
		record, err := reader.Read()
		if err != nil && err != io.EOF {
			return err
		} else if err == io.EOF {
			d.logger.Info("finished downloading videos")
			return nil
		}
		// skip the first row which are the headers
		if i == 0 {
			i++
			continue
		}
		// this row contains no video
		if len(record) < 7 {
			continue
		}
		d.wp.Submit(func() {
			max := len(record) - 1
			for i := 6; i < max; i++ {
				// no data in row so skip
				if record[i] == "" {
					continue
				}
				d.logger.Info("downloading video", zap.String("video", record[3]), zap.String("link", record[i]))
				download := func() {
					var errbuf bytes.Buffer
					// use an atomically increasing counter to prevent any possible chacne of filename conflics when running many concurrent downloaders
					cmd := exec.Command("youtube-dl", "-o", d.path+"/%(title)s.%(ext)s-"+fmt.Sprint(d.count.Inc()), record[i])
					cmd.Stderr = &errbuf
					// if this fails, then it means youtube-dl wasn't able to process the video
					if err := cmd.Start(); err != nil {
						d.logger.Error("failed to start command", zap.Error(err), zap.String("video", record[3]), zap.String("link", record[i]))
						return
					}
					done := make(chan error)
					go func() { done <- cmd.Wait() }()
					select {
					case err := <-done:
						if err != nil {
							d.logger.Error("failed to run command", zap.Error(err), zap.String("command.error", errbuf.String()), zap.String("video", record[3]), zap.String("link", record[i]))
							log.Println("failed to run command: ", err)
							return
						}
					case <-time.After(timeout):
						// TODO(bonedaddy): decide if we need this
						// it's menat to prevent any possible issue with cmd having a nil process when this is called
						defer recover()
						// kill the process
						if cmd.Process != nil {
							cmd.Process.Kill()
						}
						d.logger.Warn("download stalled, skipping", zap.String("video", record[3]), zap.String("link", record[i]))
						return
					}
				}
				download()
			}
		})
	}
}
