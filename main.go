package main

import (
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

type batch struct {
	pod5s []pod5
	next  int

	dpath string
	in    string
	out   string
	chunk int
	mp    bool
}

type pod5 struct {
	path string
	name string
}

func main() {

	// Parse flags and check for required input
	in := flag.String("in", "", "Path to pod5s")
	dpath := flag.String("dorado", "", "Path to dorado")
	out := flag.String("out", "", "Output file path")
	chunk := flag.Int("chunk", 50, "chunk size, default 50")
	mp := flag.Bool("monitor-pressure", false, "monitor pipe pressure between dorado and zstd, output to file")
	flag.Parse()

	if *in == "" || *out == "" || *dpath == "" {
		flag.PrintDefaults()
		return
	}

	// build batch
	b := new(batch)
	b.dpath = *dpath
	b.in = *in
	b.out = *out
	b.chunk = *chunk
	b.mp = *mp

	filepath.WalkDir(*in, func(path string, di fs.DirEntry, err error) error {
		if di != nil {
			name := di.Name()
			ext := filepath.Ext(name)
			if ext == ".pod5" {
				b.pod5s = append(b.pod5s, pod5{path: path, name: di.Name()})
			}
		}

		return nil
	})

	if len(b.pod5s) == 0 {
		log.Fatalf("no files found with .pod5 extension")
	}

	// we create symlinks in a tmpdir to avoid the high setup costs in basecalling
	err := os.Mkdir("tmpdir", 0750)
	if err != nil {
		log.Fatalf("error making tmpdir")
	}
	defer os.RemoveAll("tmpdir")

	for done := false; !done; {
		done, err = b.batch()
		if err != nil {
			fmt.Println(err)
			break
		}
		clearTmpDir("tmpdir")
	}
}

// Process a batch of pod5s from the pool
func (b *batch) batch() (bool, error) {

	var i, j int
	for i, j = b.next, 0; i < len(b.pod5s) && j < b.chunk; i, j = i+1, j+1 {
		err := os.Symlink(b.pod5s[i].path, "tmpdir/"+b.pod5s[i].name)
		if err != nil {
			return false, fmt.Errorf("error creating symbolic link %w", err)
		}
	}

	fmt.Println("=============================================")
	fmt.Printf("basecalling from %d to %d files of %d\n", b.next, i, len(b.pod5s))
	fmt.Println("=============================================")

	b.next = i

	err := b.call()
	if err != nil {
		return false, fmt.Errorf("error basecalling: %w", err)
	}

	return i == len(b.pod5s), nil
}

// call all pod5s in tmpdir
func (b *batch) call() error {

	// create commands for dorado and zstd, display stderror
	dorado := exec.Command(b.dpath, "basecaller", "hac", "-r", "--emit-fastq", "tmpdir/")
	zstd := exec.Command("zstd")
	dorado.Stderr = os.Stderr
	zstd.Stderr = os.Stderr

	doradoOut, err := dorado.StdoutPipe()
	if err != nil {
		return fmt.Errorf("could not get dorado stdout %w", err)
	}

	// If monitoring backpressure, we need a writecloser for zstd
	var zstdIn io.WriteCloser
	if b.mp {
		zstdIn, err = zstd.StdinPipe()
		if err != nil {
			return fmt.Errorf("could not get zstd stdin %w", err)
		}
	} else {
		// dorado | zstd
		zstd.Stdin = doradoOut
	}

	// zstd >> b.out
	out, err := os.OpenFile(b.out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening file %w", err)
	}
	defer out.Close()
	zstd.Stdout = out

	if err := dorado.Start(); err != nil {
		return fmt.Errorf("failed to start dorado: %w", err)
	}
	if err := zstd.Start(); err != nil {
		return fmt.Errorf("failed to start zstd: %w", err)
	}

	if b.mp {
		// dorado | monitor | zstd
		go chanMonitor(doradoOut, zstdIn)
	}

	if err := dorado.Wait(); err != nil {
		return fmt.Errorf("dorado error: %w", err)
	}
	doradoOut.Close()

	if err := zstd.Wait(); err != nil {
		return fmt.Errorf("zstd error: %w", err)
	}

	return nil

}

type entry struct {
	readTime  time.Duration
	writeTime time.Duration
	size      int
}

func chanMonitor(rd io.ReadCloser, wr io.WriteCloser) {
	buf := make([]byte, 128*1024) //zstd max block size 128kiB
	pipeStats := make([]entry, 0, 10000)

	var readMark, writeMark time.Time
	var readTime, writeTime time.Duration
	for {
		readMark = time.Now()
		nr, err := rd.Read(buf)
		readTime = time.Since(readMark)
		if err == io.EOF {
			writeAnalysis(pipeStats)
			wr.Close()
			break
		}
		if err != nil {
			log.Fatalf("error reading from dorado %s", err)
		}

		writeMark = time.Now()
		nw, err := wr.Write(buf[:nr])
		if nr != nw {
			log.Fatal("wrote different number of bytes than read")
		}
		if err != nil {
			log.Fatalf("error writing to zstd %s", err)
		}
		writeTime = time.Since(writeMark)

		newEntry := entry{readTime, writeTime, nw}
		pipeStats = append(pipeStats, newEntry)
	}
}

// Write a csv with pipe pressure data
func writeAnalysis(data []entry) {
	stats, err := os.OpenFile("chan_stats.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("error opening file for chan stats %s\n", err)
		return
	}
	defer stats.Close()

	stats.Write([]byte("Read Time (ns), Write Time (ns), Buffer Size (bytes)\n"))
	for i := range data {
		stats.Write([]byte(strconv.FormatInt(int64(data[i].readTime), 10) + "," + strconv.FormatInt(int64(data[i].writeTime), 10) + "," + strconv.Itoa(data[i].size) + "\n"))
	}
}

// Clears the tmpdir without deleting the directory itself
func clearTmpDir(tmp string) {
	entries, err := os.ReadDir(tmp)
	if err != nil {
		log.Fatal(err)
	}
	for _, entry := range entries {
		err := os.RemoveAll(filepath.Join(tmp, entry.Name()))
		if err != nil {
			log.Fatal(err)
		}
	}
}
