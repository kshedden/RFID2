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
	// All the Clarity records
	clarity []*rfid.ClarityRecord

	logger *log.Logger
)

// Sort first by CSN, then by time, used for patients
type byCSNTime []*rfid.RFIDrecord

func (a byCSNTime) Len() int      { return len(a) }
func (a byCSNTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byCSNTime) Less(i, j int) bool {

	switch {
	case a[i].CSN < a[j].CSN:
		return true
	case a[i].CSN > a[j].CSN:
		return false
	default:
		return a[i].TimeStamp.Before(a[j].TimeStamp)
	}
}

// Sort first by UMid, then by time, used for providers.
type byUMidTime []*rfid.RFIDrecord

func (a byUMidTime) Len() int      { return len(a) }
func (a byUMidTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byUMidTime) Less(i, j int) bool {

	switch {
	case a[i].UMid < a[j].UMid:
		return true
	case a[i].UMid > a[j].UMid:
		return false
	default:
		return a[i].TimeStamp.Before(a[j].TimeStamp)
	}
}

// readDay reads all records for a single day, and returns two RFIDrecord arrays,
// containing RFIDrecord structs for patients and for providers respectively.
func readDay(year, month, day int) ([]*rfid.RFIDrecord, []*rfid.RFIDrecord) {

	// Each day of data is in a different file
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
	sort.Sort(byUMidTime(provrecs))
	sort.Sort(byCSNTime(patrecs))

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

	// Keep at most 120 pings per minute (average 2/second)
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

func saverec(r *rfid.RFIDrecord, tm time.Time, signals []float32, clarityRec *rfid.ClarityRecord,
	enc *gob.Encoder) {

	ox := rfid.SignalRec{
		TagId:     r.TagId,
		UMid:      r.UMid,
		CSN:       r.CSN,
		TimeStamp: tm,
		Signals:   signals,
	}

	if clarityRec != nil {
		ox.ClarityStart = clarityRec.CheckInTime
		ox.ClarityEnd = clarityRec.CheckOutTime
	}

	if err := enc.Encode(&ox); err != nil {
		panic(err)
	}
}

func processPerson(recs []*rfid.RFIDrecord, signals []float32, enc *gob.Encoder) {

	// Check if the CSN is in the Clarity data
	var clarityRec *rfid.ClarityRecord
	rec0 := recs[0]
	csn := rec0.CSN
	if csn != 0 {
		ii := sort.Search(len(clarity), func(i int) bool { return csn <= clarity[i].CSN })
		if ii < len(clarity) && clarity[ii].CSN == csn {
			for j := ii; clarity[j].CSN == csn; j++ {
				// We found a CSN match, but also need to check the date.
				if clarity[j].CheckInTime.Truncate(24*time.Hour) == rec0.TimeStamp.Truncate(24*time.Hour) {
					clarityRec = clarity[j]
				}
			}
		}
	}

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
		saverec(recs[0], tm, signals, clarityRec, enc)
		recs = recs[i:len(recs)]
	}
}

func readClarity() {

	fid, err := os.Open("clarity.gob.gz")
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
	dec.Decode(&clarity)
}

func main() {

	setupLog()
	readClarity()

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
						var id uint64
						switch j {
						case 0:
							id = v[0].CSN
						case 1:
							id = v[0].UMid
						default:
							panic("")
						}
						i, f := 0, false
						for i = range v {
							var id1 uint64
							switch j {
							case 0:
								id1 = v[i].CSN
							case 1:
								id1 = v[i].UMid
							default:
								panic("")
							}
							if id1 != id {
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
