package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const NUM_READERS = 100

type MyBytes []byte

func (b MyBytes) Write(p []byte) (int, error) {
	return copy(b, p), nil
}

type MutexFile struct {
	mu *sync.Mutex
	*os.File
}

type FileServer struct {
	root    string
	readers map[string][NUM_READERS]MutexFile
	sizes   map[string]int64
}

func NewFileServer(root string) *FileServer {
	files, err := filepath.Glob(root + "/**/*.pmtiles")
	if err != nil {
		log.Fatal(err)
	}

	readers := make(map[string][NUM_READERS]MutexFile)
	sizes := make(map[string]int64)

	for _, fp := range files {
		f, err := os.Open(fp)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		fi, err := f.Stat()
		if err != nil {
			log.Fatal(err)
		}
		size := fi.Size()
		sizes["/"+fp] = size

		fileArray := [NUM_READERS]MutexFile{}
		for i := 0; i < NUM_READERS; i++ {
			filePtr, err := os.Open(fp)
			if err != nil {
				log.Fatal(err)
			}
			filePtr.Seek(int64(i)*size/NUM_READERS, 0)
			fileArray[i] = MutexFile{File: filePtr, mu: &sync.Mutex{}}
		}
		readers["/"+fp] = fileArray
	}

	return &FileServer{root: root, readers: readers, sizes: sizes}

}

func (fs *FileServer) GetSlow(path string, rangeStr string) (MyBytes, error) {
	// get ranges
	ranges, err := ParseRanges(rangeStr)
	if err != nil {
		return nil, err
	}

	// Open file
	f, err := os.Open(fs.root + path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	bytes := make(MyBytes, 0)

	for _, r := range ranges {
		f.Seek(0, 0)
		b := make(MyBytes, r[1]-r[0]+1)
		_, err := f.Seek(r[0], 0)
		if err != nil {
			return nil, err
		}
		f.Read(b)
		bytes = append(bytes, b...)
	}

	return bytes, nil
}

func (fs *FileServer) GetFast(path string, rangeStr string) (MyBytes, error) {
	// get ranges
	ranges, err := ParseRanges(rangeStr)
	if err != nil {
		return nil, err
	}

	bytes := make(MyBytes, 0)
	arr := fs.readers[path]
	size := fs.sizes[path]

	for _, r := range ranges {
		closest := (r[0] * NUM_READERS) / size              // index of closest reader
		origLoc := (closest * size) / NUM_READERS           // location of closest reader
		forwardOffset := r[0] - origLoc                     // offset from closest reader
		backwardOffset := forwardOffset + (r[1] - r[0] + 1) // amount to seek back

		arr[closest].mu.Lock() // lock closest reader

		b := make(MyBytes, r[1]-r[0]+1)

		// seek to offset
		newLoc, err := arr[closest].Seek(forwardOffset, 1)
		if err != nil {
			return nil, err
		}
		if newLoc != r[0] {
			log.Printf("seek forward failed. started at: %d. want: %d. offset: %d. got: %d", origLoc, r[0], forwardOffset, newLoc)
			return nil, errors.New("seek forward failed")
		}

		// read
		arr[closest].Read(b)

		// seek back to original location
		backLoc, err := arr[closest].Seek(-backwardOffset, 1)
		if err != nil {
			return nil, err
		}
		if backLoc != origLoc {
			log.Printf("seek back failed. started at: %d. want: %d. offset: %d. got: %d", origLoc, r[0], backwardOffset, backLoc)
			return nil, errors.New("seek back failed")
		}

		arr[closest].mu.Unlock() // unlock closest reader

		bytes = append(bytes, b...)
	}

	return bytes, nil
}

func (fs *FileServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bytes, err := fs.GetFast(r.URL.Path, r.Header.Get("Range"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "*")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusPartialContent)
	w.Write(bytes)
}

func ParseRanges(rangeStr string) ([][2]int64, error) {
	// Range: bytes=0-499, 500-999, 1000-1499
	rangeStr, found := strings.CutPrefix(rangeStr, "bytes=")
	if !found {
		return nil, errors.New("invalid range")
	}

	ranges := [][2]int64{}
	for _, r := range strings.Split(rangeStr, ",") {
		interval := strings.Split(r, "-")
		start, err := strconv.ParseInt(interval[0], 10, 64)
		if err != nil {
			return nil, err
		}
		end, err := strconv.ParseInt(interval[1], 10, 64)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, [2]int64{start, end})
	}

	return ranges, nil
}

func TimerHandler(h http.Handler) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		timeStart := time.Now()
		h.ServeHTTP(w, r)
		timeEnd := time.Now()
		log.Printf("%s %s %s %s\n", r.Method, r.URL.Path, r.Header.Get("Range"), timeEnd.Sub(timeStart))
	})
}

func main() {
	fs := NewFileServer(".")
	http.Handle("/", TimerHandler(fs))
	err := http.ListenAndServe(":8080", nil)
	log.Fatal(err)
}
