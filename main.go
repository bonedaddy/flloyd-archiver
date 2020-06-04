package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/BrianAllred/goydl"
	ldb "github.com/RTradeLtd/go-datastores/leveldb"
	ipfsapi "github.com/RTradeLtd/go-ipfs-api/v3"
	"github.com/go-chi/chi"
	"github.com/ipfs/go-datastore"
	"github.com/urfave/cli/v2"
	"go.bobheadxi.dev/zapx/zapx"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	count = atomic.NewInt64(0)
	url   = "https://raw.githubusercontent.com/2020PB/police-brutality/data_build/all-locations.csv"
)

func main() {
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "listen.address",
			Aliases: []string{"la"},
			Value:   "0.0.0.0:5002",
		},
		&cli.StringFlag{
			Name:  "ipfs.api",
			Value: "192.168.1.201:5001",
		},
		&cli.StringFlag{
			Name:  "leveldb.path",
			Value: "archiver-store",
		},
		&cli.StringFlag{
			Name:  "log.file",
			Value: "archiver.log",
		},
		&cli.StringFlag{
			Name:  "file",
			Usage: "file to upload",
		},
	}
	app.Commands = cli.Commands{
		&cli.Command{
			Name:  "2020pb-archiver",
			Usage: "pulls the CSV from https://github.com/2020PB/police-brutality/tree/data_build",
			Action: func(c *cli.Context) error {
				if _, err := os.Stat(c.String("dir")); os.IsNotExist(err) {
					if err := os.Mkdir(c.String("dir"), os.FileMode(0775)); err != nil {
						return err
					}
				}
				resp, err := http.Get(url)
				if err != nil {
					return err
				}
				defer resp.Body.Close()
				reader := csv.NewReader(resp.Body)
				ytdl := goydl.NewYoutubeDl()
				_ = ytdl
				// opts := goydl.NewOptions()
				// opts.Output.Value = c.String("dir")
				// ytdl.Options = opts
				// go io.Copy(os.Stdout, ytdl.Stdout)
				// go io.Copy(os.Stderr, ytdl.Stderr)
				/* rows:
				0    , 1     , 2  , 3  , 4  , 5       , 6    , 7    ,  8   , 9    , 10   , 11  ,  12   , 13
				state,edit_at,city,name,date,date_text,Link 1,Link 2,Link 3,Link 4,Link 5,Link 6,Link 7,Link 8
				*/
				i := 0
				for {
					record, err := reader.Read()
					if err != nil {
						return err
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
					fmt.Println("downloading video: ", record[3])
					max := len(record) - 1
					for i := 6; i < max; i++ {
						// empty link
						if record[i] == "" {
							continue
						}
						fmt.Println("downloading link: ", record[i])
						cmd := exec.Command("youtube-dl", "-o", c.String("dir")+"/%(title)s.%(ext)s", record[i])
						if err := cmd.Run(); err != nil {
							log.Println("WARN: run failed: ", err)
						}
					}
				}
				return nil
			},
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  "dir",
					Usage: "directory to save youtube videos to",
					Value: "videos",
				},
			},
		},
		&cli.Command{
			Name: "run",
			Action: func(c *cli.Context) error {
				logger, err := zapx.New(c.String("log.file"), false)
				if err != nil {
					return err
				}
				shell := ipfsapi.NewShell(c.String("ipfs.api"))
				router := chi.NewRouter()
				ds, err := ldb.NewDatastore(c.String("leveldb.path"), nil)
				if err != nil {
					return err
				}
				router.HandleFunc("/*", func(w http.ResponseWriter, r *http.Request) {
					if err := r.ParseForm(); err != nil {
						w.WriteHeader(http.StatusBadRequest)
						w.Write([]byte(err.Error()))
						return
					}
					fh, header, err := r.FormFile("data")
					if err != nil {
						w.WriteHeader(http.StatusBadRequest)
						w.Write([]byte(err.Error()))
						return
					}
					hash, err := shell.Add(fh)
					if err != nil {
						w.WriteHeader(http.StatusBadRequest)
						w.Write([]byte(err.Error()))
						return
					}
					num := count.Inc()
					name := fmt.Sprintf("%s-%v-%v", header.Filename, time.Now().UnixNano(), num)
					logger.Info("new upload detected", zap.String("file.name", name), zap.String("file.hash", hash), zap.Int64("number", num))
					ds.Put(datastore.NewKey(name), []byte(hash))
					ds.Put(datastore.NewKey(hash+"-"+fmt.Sprint(num)), []byte(name))
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(hash))
				})
				srv := &http.Server{
					Handler: router,
					Addr:    c.String("listen.address"),
				}
				return srv.ListenAndServe()
			},
		},
		&cli.Command{
			Name: "upload",
			Action: func(c *cli.Context) error {
				fh, err := os.Open(c.String("file"))
				if err != nil {
					return err
				}
				defer fh.Close()
				bodyBuf := &bytes.Buffer{}
				bodyWriter := multipart.NewWriter(bodyBuf)
				fileWriter, err := bodyWriter.CreateFormFile("data", c.String("file"))
				if err != nil {
					return err
				}
				if _, err := io.Copy(fileWriter, fh); err != nil {
					return err
				}
				if err := bodyWriter.Close(); err != nil {
					return err
				}
				req, err := http.NewRequest("POST", c.String("endpoint"), bodyBuf)
				if err != nil {
					return err
				}
				req.Header.Add("Content-Type", bodyWriter.FormDataContentType())
				hc := &http.Client{}
				resp, err := hc.Do(req)
				if err != nil {
					return err
				}
				defer resp.Body.Close()
				data, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					return err
				}
				fmt.Println(string(data))
				return nil
			},
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  "endpoint",
					Usage: "endpoint to upload to",
					Value: "http://dev.api.ipfs.temporal.cloud:5002",
				},
			},
		},
	}
	if err := app.Run(os.Args); err != nil {
		panic(err)
	}
}
