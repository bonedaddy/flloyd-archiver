package downloader

import (
	"encoding/csv"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
)

const (
	// URL is the path to the CSV file
	URL = "https://raw.githubusercontent.com/2020PB/police-brutality/data_build/all-locations.csv"
)

// Downloader downloads the media contained in the csv file
type Downloader struct {
	path string
}

// New returns a new downloader
func New(path string) *Downloader {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.Mkdir(path, os.FileMode(0775)); err != nil {
			panic(err)
		}
	}
	return &Downloader{path}
}

// Run starts the download process
func (d *Downloader) Run() error {
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
			log.Println("finished downloading videos")
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
		log.Printf("downloading video(s): %s", record[3])
		max := len(record) - 1
		for i := 6; i < max; i++ {
			// no data in row so skip
			if record[i] == "" {
				continue
			}
			log.Printf("downloading link: %s", record[i])
			cmd := exec.Command("youtube-dl", "-o", d.path+"/%(title)s.%(ext)s", record[i])
			if err := cmd.Run(); err != nil {
				log.Printf("WARN: run failed for link %s with error %s", record[i], err.Error())
			}
		}
	}
}
