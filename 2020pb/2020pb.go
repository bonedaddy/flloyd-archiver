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
	"sync"
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
func (d *Downloader) Run(timeout time.Duration, max int) error {
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
	// maps the video name + link to it's count
	var (
		results []struct {
			name  string
			link  string
			count int64
		}
		mux sync.Mutex
		i   = 0
	)
	for {
		if max != 0 && i >= max {
			break
		}
		// read the next record
		record, err := reader.Read()
		if err != nil && err != io.EOF {
			return err
		} else if err == io.EOF {
			d.logger.Info("finished downloading videos")
			break
		}
		i++
		// skip the first row as it contains column names
		if i == 1 {
			continue
		}
		// this row contains no video(s)
		// as it has less than 7 elements, and the 7th element is the start of the video links
		if len(record) < 7 {
			continue
		}
		// var nameToID = make(map[string]string)
		// submit to the worker pool, allowing concurrently downloading multiple videos
		d.wp.Submit(func() {
			max := len(record) - 1
			for i := 6; i < max; i++ {
				// this column is empty, and has no data
				if record[i] == "" {
					continue
				}
				count := d.count.Inc()
				d.logger.Info("downloading video", zap.String("name", record[3]), zap.String("url", record[i]))
				download := func() {
					var errbuf bytes.Buffer
					mux.Lock()
					results = append(results, struct {
						name  string
						link  string
						count int64
					}{
						name:  record[3],
						link:  record[i],
						count: count,
					})
					mux.Unlock()
					// use an atomically increasing counter to prevent any possible chacne of filename conflics when running many concurrent downloaders
					//	cmd := exec.Command("youtube-dl", "-o", d.path+"/%(title)s.%(id).%(ext)s.%(id)s-"+fmt.Sprint(d.count.Inc()), record[i])
					cmd := exec.Command("youtube-dl", "-o", d.getName(count), record[i])
					// enables capturing the stderr output for easier debugging
					cmd.Stderr = &errbuf
					// if this fails, then it means youtube-dl wasn't able to process the video
					if err := cmd.Start(); err != nil {
						d.logger.Error("failed to start command", zap.Error(err), zap.String("name", record[3]), zap.String("url", record[i]))
						return
					}
					// this prevents a stalled download, or transient network issues from stalling the entire download process
					done := make(chan error)
					go func() { done <- cmd.Wait() }()
					select {
					case err := <-done:
						if err != nil {
							d.logger.Error("failed to run command", zap.Error(err), zap.String("command.error", errbuf.String()), zap.String("name", record[3]), zap.String("url", record[i]))
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
	// TODO(bonedaddy): write name+link -> count mapping
	fh, err := os.Create("name_mapping.txt")
	if err != nil {
		return err
	}
	for _, v := range results {
		fh.WriteString(fmt.Sprintf("name: %s\tlink: %s\tcount: %v\n", v.name, v.link, v.count))
	}
	return fh.Close()
}

func (d *Downloader) getName(count int64) string {
	return d.path + "/%(id)s." + fmt.Sprint(count) + ".%(ext)s"
}
