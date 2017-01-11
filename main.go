package main

import (
	"flag"
	"fmt"
	"github.com/uttamgandhi24/whisper-go/whisper"
	"os"
	"path/filepath"
	"strings"
	"encoding/json"
	"io/ioutil"
	"time"
	"regexp"
	"strconv"
	"bufio"
	// "log"
	// "github.com/influxdata/influxdb/client/v2"
	// "github.com/influxdata/influxdb/tsdb/engine/tsm1"
)


type MigrationData struct {
	wspFile      string
	relativePath string
	exportFile   string
	measurement  string
	tags         string
	field        string
	matched      bool
}

type TagKeyValue struct {
	Tagkey   string `json:"tagkey"`
	Tagvalue string `json:"tagvalue"`
}

type MigrationConfig struct {
	Pattern     string        `json:"pattern"`
	Measurement string        `json:"measurement"`
	Tags        []TagKeyValue `json:"tags"`
	Field       string        `json:"field"`
}

var migrationConfig []MigrationConfig
var (
	wspPath      = flag.String("wspPath", "NULL", "Whisper files folder path")
	exportPath   = flag.String("exportPath", "NULL", "Directory to export line protocol files")
	configFile   = flag.String("configFile", "NULL", "Configuration file for measurement and tags")
)




func check(e error) {
    if e != nil {
        panic(e)
    }
}


func main() {
	flag.Parse()

	// List files
	fileList := []string{}
	listWspFiles(&fileList, *wspPath)

	// Open migration config file
	loadConfigFile(&migrationConfig, *configFile)

	// Go through wsp files and figure out the tags and measurements
	var migrationData []MigrationData
	for _, wspFile := range fileList {
		data := MigrationData{}
		data.wspFile = wspFile
		data.relativePath = strings.TrimPrefix(wspFile, *wspPath)
		data.exportFile = *exportPath + strings.Replace(data.relativePath, ".wsp", ".txt", -1)

		// Assign the right measurment, field and tags
		data.assignConfig()

		if data.matched {
			migrationData = append(migrationData, data)
		} else {
			fmt.Println("File didn't match any config patterns: ", data.wspFile)
		}
		// Add the export file path ? Or generate it later ? migrationData.exportFile = migrationData.wspFile
	}

	// Go through wsp files
	from, _ := time.Parse("2006-01-02", "2000-01-01")
	until, _ := time.Parse("2006-01-02", "2100-01-01")

	for _, migration := range migrationData {
		// Open whisper file with driver
		w, err := whisper.Open(migration.wspFile)
		// fmt.Println("Preparing", migration.wspFile, ">>>", migration.exportFile, "(Size", w.Header.Archives[0].Size(), ")")
		check(err)
		// Get all points
		_, wspPoints, err := w.FetchUntilTime(from, until)
		w.Close()

		// Makes sure the directory exists
		os.MkdirAll(migration.exportFile[0:strings.LastIndex(migration.exportFile, "/")] ,0755);

		// Open file and prepare writer
		f, err := os.Create(migration.exportFile)
		check(err)
		defer f.Close()
		writer := bufio.NewWriter(f)

		// Print all points
		for _, point := range wspPoints {
			line := migration.lineprotocol(point) + "\n"

			_, err := writer.WriteString(line)
			check(err)
		}

		// Flush writer
		writer.Flush()

		fmt.Println("Successfully wrote >>>", migration.exportFile)
	}
}


// Create the list of wsp files
func listWspFiles(fileList *[]string, searchDir string) {
	err := filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
		if os.IsNotExist(err) { //search dir does not exist
			return nil
		}
		// Only add wsp files to the list
		if strings.HasSuffix(f.Name(), "wsp") {
			*fileList = append(*fileList, path)
		}
		return nil
	})
	if err != nil {
		fmt.Println("Error listing files:")
		fmt.Println(err)
	}
}


// Read the config file and populate migrartionData.tagConfigs
func loadConfigFile(migrationConfig *[]MigrationConfig, filename string) error {
	raw, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	return json.Unmarshal(raw, &migrationConfig)
}


func (migrationData *MigrationData) lineprotocol(point whisper.Point) string {
	var line string
	line += migrationData.measurement
	line += migrationData.tags
	line += " "
	line += migrationData.field + "=" + strconv.FormatFloat(point.Value, 'f', -1, 64)
	line += " "
	line += strconv.FormatInt(int64(point.Timestamp), 10)
	return line
}


// Get measurement, tags and field by matching the whisper filename with a
// pattern in the config file
func (migrationData *MigrationData) assignConfig() {

	wspMeasurement := strings.TrimPrefix(migrationData.wspFile, *wspPath)
	wspMeasurement = strings.TrimSuffix(wspMeasurement, ".wsp")
	wspMeasurement = strings.Replace(wspMeasurement, "/", ".", -1)
	wspMeasurement = strings.Replace(wspMeasurement, ",", "_", -1)
	wspMeasurement = strings.Replace(wspMeasurement, " ", "_", -1)

	// Filename matching
	// TODO catch strings that don't match until the end
	var tagConfig MigrationConfig
	var matched []string
	var matchedArr [][]string
	var wildcards [][]string
	filenameMatched := false
	for _, tagConfig = range migrationConfig {
		reWild := regexp.MustCompile("{{\\s*([a-zA-Z0-9]+)\\s*}}")

		// Prepare regex pattern
		pattern := strings.Replace(tagConfig.Pattern, ".", "\\.", -1)
		pattern = reWild.ReplaceAllLiteralString(pattern, "([^.]+)")

		// List the matching values (Base and groups)
		re := regexp.MustCompile(pattern)

		matchedArr = re.FindAllStringSubmatch(wspMeasurement, -1)

		if matchedArr != nil {
			filenameMatched = true
			matched = matchedArr[0]
			// List of replacement wildcards like "{{ host }}"
			wildcards = reWild.FindAllStringSubmatch(tagConfig.Pattern, -1)
			break
		}
	}

	// Exit if there was no match
	if filenameMatched == false {
		return
	} else {
		migrationData.matched = true
	}


	// Fill the migrationData object
	migrationData.measurement = tagConfig.Measurement
	migrationData.field = tagConfig.Field
	for j := 0; j < len(tagConfig.Tags); j++ {
		migrationData.tags += ","
		migrationData.tags += tagConfig.Tags[j].Tagkey
		migrationData.tags += "="
		migrationData.tags += tagConfig.Tags[j].Tagvalue
	}


	// In case measurement and field aren't set in the config file
	// if tagConfig.Measurement != "" {
	// 	mtf.Measurement = tagConfig.Measurement
	// } else {
	// 	parts := strings.Split(wspMeasurement, ".")
	// 	mtf.Measurement = parts[len(parts)-1]
	// }
	// if tagConfig.Field != "" {
	// 	mtf.Field = tagConfig.Field
	// } else {
	// 	mtf.Field = "value"
	// }
	// mtf.Tags = make([]TagKeyValue, len(tagConfig.Tags))
	// copy(mtf.Tags, tagConfig.Tags)


	// Replace "{{ wildcard }}" with matched values in order
	// (reversed to avoid overlapping of bigger numbers)
	// TODO issues if matched contains "$n"
	for i := len(matched) - 1; i > 0; i-- {
		re := regexp.MustCompile("{{\\s" + wildcards[i - 1][1] + "\\s}}")

		migrationData.measurement = re.ReplaceAllLiteralString(migrationData.measurement, matched[i])
		migrationData.field = re.ReplaceAllLiteralString(migrationData.field, matched[i])
		migrationData.tags = re.ReplaceAllLiteralString(migrationData.tags, matched[i])
	}
}
