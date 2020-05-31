package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"go.uber.org/atomic"

	ldb "github.com/RTradeLtd/go-datastores/leveldb"
	ipfsapi "github.com/RTradeLtd/go-ipfs-api/v3"
	"github.com/go-chi/chi"
	"github.com/ipfs/go-datastore"
	"github.com/urfave/cli/v2"
)

var (
	count = atomic.NewInt64(0)
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
	}
	app.Commands = cli.Commands{
		&cli.Command{
			Name: "run",
			Action: func(c *cli.Context) error {
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
					ds.Put(datastore.NewKey(header.Filename+time.Now().String()+fmt.Sprint(count.Inc())), []byte(hash))
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
	}
	if err := app.Run(os.Args); err != nil {
		panic(err)
	}
}
