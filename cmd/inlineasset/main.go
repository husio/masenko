package main

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"text/template"
	"time"
)

func main() {
	if len(os.Args) != 5 {
		fmt.Fprintf(os.Stderr, "Usage: %s <package> <variable-name> <in-file> <out-file>\n", os.Args[0])
		os.Exit(2)
	}

	packageName := os.Args[1]
	variableName := os.Args[2]
	inFilePath := os.Args[3]
	outFilePath := os.Args[4]

	data, err := ioutil.ReadFile(inFilePath)
	if err != nil {
		log.Fatalf("read: %s", err)
	}

	fd, err := os.Create(outFilePath)
	if err != nil {
		log.Fatalf("create: %s", err)
	}
	defer fd.Close()

	err = tmpl.Execute(fd, struct {
		Now         time.Time
		PackageName string
		VarName     string
		Value       string
	}{
		Now:         time.Now().UTC(),
		PackageName: packageName,
		VarName:     variableName,
		Value:       base64.StdEncoding.EncodeToString(data),
	})
	if err != nil {
		log.Fatalf("render: %s", err)
	}
}

var tmpl = template.Must(template.New("").Parse(`// File generated with inlineasset {{.Now}}.
package {{.PackageName}}

import "encoding/base64"

var {{.VarName}}, _ = base64.StdEncoding.DecodeString("{{.Value}}")
`))
