package main

import (
	"github.com/CorentinB/warc"
)

func main() {
	rotatorSettings := warc.NewRotatorSettings()

	rotatorSettings.Compression = ""
	rotatorSettings.Prefix = "EXAMPLE"
	rotatorSettings.WarcSize = 1000

	recorder, err := warc.NewRecorder(rotatorSettings)
	if err != nil {
		panic(err)
	}
	defer recorder.Close()

	//_, err = recorder.Client().Get("http://google.com")
	_, err = recorder.Client().Get("http://the-eye.eu")
	if err != nil {
		panic(err)
	}
}
