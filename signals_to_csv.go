package main

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/gob"
	"fmt"
	"io"
	"os"

	"github.com/kshedden/rfid2/rfid"
)

func main() {

	if len(os.Args) != 2 || (os.Args[1] != "patient" && os.Args[1] != "provider") {
		msg := fmt.Sprintf("Usage: %s [patient|provider]\n", os.Args[0])
		os.Stderr.WriteString(msg)
		os.Exit(1)
	}
	gn := os.Args[1]

	fid, err := os.Open(fmt.Sprintf("%s_signals.gob.gz", gn))
	if err != nil {
		panic(err)
	}
	defer fid.Close()

	gid, err := gzip.NewReader(fid)
	if err != nil {
		panic(err)
	}
	defer gid.Close()

	dec := gob.NewDecoder(gid)

	outf, err := os.Create(fmt.Sprintf("%s_signals.csv.gz", gn))
	if err != nil {
		panic(err)
	}
	defer outf.Close()

	out := gzip.NewWriter(outf)
	defer out.Close()
	enc := csv.NewWriter(out)

	// Write out the header
	var tr []string
	switch gn {
	case "patient":
		tr = []string{"TagId", "CSN", "ClarityStart", "ClarityEnd", "Time"}
	case "provider":
		tr = []string{"TagId", "UMid", "Time"}
	default:
		panic(fmt.Sprintf("Unknown group type: %s\n", gn))
	}
	for k := 0; k < len(rfid.IPcode); k++ {
		tr = append(tr, rfid.RoomName[rfid.RoomCode(k)])
	}
	if err := enc.Write(tr); err != nil {
		panic(err)
	}

	var r rfid.SignalRec
	for {
		err := dec.Decode(&r)
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		tr = tr[0:0]
		tr = append(tr, fmt.Sprintf("%d", r.TagId))
		switch gn {
		case "provider":
			tr = append(tr, fmt.Sprintf("%d", r.UMid))
		case "patient":
			tr = append(tr, fmt.Sprintf("%d", r.CSN))
			if !r.ClarityStart.IsZero() {
				tr = append(tr, fmt.Sprintf("%s", r.ClarityStart.Format("2006-01-02T15:04")))
			} else {
				tr = append(tr, "")
			}
			if !r.ClarityEnd.IsZero() {
				tr = append(tr, fmt.Sprintf("%s", r.ClarityEnd.Format("2006-01-02T15:04")))
			} else {
				tr = append(tr, "")
			}
		default:
			panic(fmt.Sprintf("Unknown group type: %s\n", gn))
		}
		tr = append(tr, fmt.Sprintf("%s", r.TimeStamp.Format("2006-01-02T15:04")))
		for _, z := range r.Signals {
			tr = append(tr, fmt.Sprintf("%.0f", 1000000*z))
		}

		if err := enc.Write(tr); err != nil {
			panic(err)
		}
	}
}
