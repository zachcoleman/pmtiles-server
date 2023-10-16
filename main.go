package main

import (
	"net/http"
	"os"
)

type MyBytes []byte

func (b MyBytes) Write(p []byte) (int, error) {
	return copy(b, p), nil
}

type FileServer struct {
	root string
}

func (fs *FileServer) Get(path string) (chan MyBytes, error) {
	f, err := os.Open(fs.root + path)
	if err != nil {
		return nil, err
	}

	ch := make(chan MyBytes)
	go func() {
		defer f.Close()
		defer close(ch)
		for {
			b := make(MyBytes, 1024)
			n, err := f.Read(b)
			if err != nil || n == 0 {
				return
			}
			ch <- b[:n]
		}
	}()

	return ch, nil
}

func (fs *FileServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ch, err := fs.Get(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	for b := range ch {
		w.Write(b)
	}
}

func main() {
	http.Handle("/", &FileServer{root: "."})
	http.ListenAndServe(":8080", nil)
}
