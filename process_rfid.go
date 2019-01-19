package main

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path"
	"sort"
	"time"

	"github.com/kshedden/rfid2/rfid"
)

var (
	logger *log.Logger
)

type byIdTime []*rfid.RFIDrecord

func (a byIdTime) Len() int      { return len(a) }
func (a byIdTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byIdTime) Less(i, j int) bool {

	switch {
	case a[i].TagId < a[j].TagId:
		return true
	case a[i].TagId > a[j].TagId:
		return false
	default:
		return a[i].TimeStamp.Before(a[j].TimeStamp)
	}
}

// readDay reads all records for a single day, and returns two RFIDrecord arrays,
// containing RFIDrecord structs for patients and for providers respectively.
func readDay(year, month, day int) ([]*rfid.RFIDrecord, []*rfid.RFIDrecord) {

	fname := fmt.Sprintf("%4d-%02d-%02d_APD.csv.gz", year, month, day)
	fname = path.Join("/", "home", "kshedden", "RFID", "data", "APD", fname)

	// If the file does not exist, return silently
	if _, err := os.Stat(fname); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
	}
	logger.Print(fmt.Sprintf("Processing file '%s'", fname))

	fid, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	gid, err := gzip.NewReader(fid)
	if err != nil {
		panic(err)
	}

	rdr := csv.NewReader(gid)
	rdr.ReuseRecord = true

	var patrecs, provrecs []*rfid.RFIDrecord
	var n int
	var rfi rfid.RFIDinfo
	var nerr int
	for {
		fields, err := rdr.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			nerr++
			continue
		}

		n++

		r := new(rfid.RFIDrecord)
		if !r.Parse(fields, &rfi) {
			continue
		}

		// Exclude records when clinic is closed
		if r.TimeStamp.Hour() < 7 {
			rfi.TimeEarly++
			continue
		}
		if r.TimeStamp.Hour() > 19 {
			rfi.TimeLate++
			continue
		}

		switch r.PersonCat {
		case rfid.Patient:
			patrecs = append(patrecs, r)
		case rfid.Provider:
			provrecs = append(provrecs, r)
		default:
			panic("Unkown person type\n")
		}
	}

	if nerr > 0 {
		print("Errors parsing CSV file, see log for more information\n")
	}
	logger.Printf("%d errors parsing csv file", nerr)

	// Confirm that it is sorted by time
	sort.Sort(byIdTime(provrecs))
	sort.Sort(byIdTime(patrecs))

	return patrecs, provrecs
}

func setupLog() {
	fid, err := os.Create("process_rfid.log")
	if err != nil {
		panic(err)
	}
	logger = log.New(fid, "", 0)
}

type xr struct {
	room   rfid.RoomCode
	signal float32
}

var xvr []xr

type xrs []xr

func (a xrs) Len() int      { return len(a) }
func (a xrs) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less is backward so we can sort in reversed order
func (a xrs) Less(i, j int) bool { return a[i].signal > a[j].signal }

func processMinute(recs []*rfid.RFIDrecord, signals []float32) time.Time {

	t0 := recs[0].TimeStamp.Truncate(time.Minute)

	if cap(xvr) < len(recs) {
		xvr = make([]xr, len(recs))
	}
	xvr = xvr[0:len(recs)]

	// Get the largest 120 signals
	for j, r := range recs {
		xvr[j].room = r.IP
		xvr[j].signal = float32(math.Exp(float64(r.Signal) / 10))
	}

	sort.Sort(xrs(xvr))

	if len(xvr) > 120 {
		xvr = xvr[0:120]
	}

	for j := range signals {
		signals[j] = 0
	}

	for _, v := range xvr {
		signals[v.room] += v.signal
	}

	return t0
}

func saverec(r *rfid.RFIDrecord, tm time.Time, signals []float32, enc *gob.Encoder) {
	ox := rfid.SignalRec{
		TagId:     r.TagId,
		UMid:      r.UMid,
		CSN:       r.CSN,
		TimeStamp: tm,
		Signals:   signals,
	}
	if err := enc.Encode(&ox); err != nil {
		panic(err)
	}
}

func processPerson(recs []*rfid.RFIDrecord, signals []float32, enc *gob.Encoder) {

	for len(recs) > 0 {
		i, f := 0, false
		for i = range recs {
			if recs[i].TimeStamp.Sub(recs[0].TimeStamp).Truncate(time.Minute) > 0 {
				f = true
				break
			}
		}
		if !f {
			i += 1
		}
		tm := processMinute(recs[0:i], signals)
		saverec(recs[0], tm, signals, enc)
		recs = recs[i:len(recs)]
	}
}

func main() {

	setupLog()

	// Setup encoders for patients and providers
	var enc [2]*gob.Encoder
	for j := 0; j < 2; j++ {
		fname := "patient_signals.gob.gz"
		if j == 1 {
			fname = "provider_signals.gob.gz"
		}
		f, err := os.Create(fname)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		g := gzip.NewWriter(f)
		defer g.Close()

		enc[j] = gob.NewEncoder(g)
	}

	signals := make([]float32, len(rfid.IPcode))

	for year := 2018; year <= 2018; year++ {
		for month := 1; month <= 12; month++ {
			for day := 1; day <= 31; day++ {

				patrecs, provrecs := readDay(year, month, day)
				fmt.Printf("%d-%d-%d %d %d\n", year, month, day, len(provrecs), len(patrecs))

				for j := 0; j < 2; j++ {

					var v []*rfid.RFIDrecord
					if j == 0 {
						v = patrecs
					} else {
						v = provrecs
					}

					for len(v) > 0 {
						tagid := v[0].TagId
						i, f := 0, false
						for i = range v {
							if v[i].TagId != tagid {
								f = true
								break
							}
						}
						if !f {
							i += 1
						}
						processPerson(v[0:i], signals, enc[j])
						v = v[i:len(v)]
					}
				}
			}
		}
	}
}
