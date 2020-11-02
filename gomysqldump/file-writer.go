package gomysqldump

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"text/template"
)

// FileWriter Object
type FileWriter struct {
	basePath string
	fileName string
	fullPath string

	mux sync.Mutex
}

// NewFileWiter - Export
func NewFileWiter(basePath string, fileName string) *FileWriter {
	fw := new(FileWriter)
	fw.basePath = basePath
	fw.fileName = fileName

	fw.fullPath = filepath.Join(basePath, fileName)

	fmt.Printf("Writing to: %s \n", fw.fullPath)

	return fw
}

// WriteContent - Write File content to file
func (fw *FileWriter) WriteContent(content string) {
	fw.mux.Lock()

	f, err := os.OpenFile(fw.fullPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		fmt.Println(err)
		return
	}

	w := bufio.NewWriter(f)

	f.WriteString(content)

	w.Flush()

	fw.mux.Unlock()
}

// WriteTemplatedContent - Write File content to file
func (fw *FileWriter) WriteTemplatedContent(template *template.Template, vars TemplateVars) {
	fw.mux.Lock()

	fmt.Println("Writing Data:", fw.fullPath)

	f, err := os.OpenFile(fw.fullPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		fmt.Println(err)
		return
	}

	w := bufio.NewWriter(f)

	err = template.Execute(w, vars)
	if err != nil {
		fmt.Println(err)
		panic("Failed to write data: WriteTemplatedContent")
	}

	w.Flush()

	fw.mux.Unlock()
}
