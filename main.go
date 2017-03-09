package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/uttamgandhi24/whisper-go/whisper"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)


type MigrationData struct {
	wspFile         string
	relativePath    string
	exportFileName  string
	measurement     string
	tags            string
	field           string
	matched         bool
	exported        bool
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


// Global vars
var migrationConfig []MigrationConfig
var exportedFileNumber = 0
var (
	verbose           = flag.Bool("verbose", false, "Configuration file for measurement and tags.")
	wspPath           = flag.String("wsp-path", "", "Whisper files folder path.")
	exportPath        = flag.String("export-path", "", "Directory to export line protocol files.")
	configFile        = flag.String("config-file", "", "Configuration file for measurement and tags.")
	fromFlag          = flag.Uint("from", 0, "Configuration file for measurement and tags.")
	untilFlag         = flag.Uint("until", uint(^uint32(0)), "Configuration file for measurement and tags.")
	gzipped           = flag.Bool("gz", false, "Export data in a gzipped file")
)


func main() {
	flag.Parse()

	// List files
	fileList := []string{}
	listWspFiles(&fileList, *wspPath)

	// Open migration config file
	loadConfigFile(&migrationConfig, *configFile)

	// Go through wsp files and figure out tags, measurements, file names, etc.
	var migrationData []MigrationData
	for _, wspFile := range fileList {
		data := MigrationData{}
		data.wspFile = wspFile
		data.relativePath = strings.TrimPrefix(wspFile, *wspPath)

		// Figure exported filename
		exportedFileNumber += 1
		data.exportFileName = fmt.Sprintf("%08d.txt", exportedFileNumber)

		// Add the .gz if the export needs to be compressed
		if *gzipped {
			data.exportFileName += ".gz"
		}

		// Assign the right measurment, field and tags
		data.assignConfig()

		if data.matched {
			migrationData = append(migrationData, data)
		} else {
			fmt.Println("File didn't match any config patterns: ", data.wspFile)
		}
	}

	// Time boundaries
	var from uint32 = uint32(*fromFlag)
	var until uint32 = uint32(*untilFlag)

	// Warning starting exporting
	fmt.Println("----------------")
	fmt.Println("Exporting", len(migrationData), "series to", *exportPath)
	if askForConfirmation("Proceed ?") == false {
		os.Exit(1)
	}
	fmt.Println("----------------")

	// Go through wsp files and export data
	for k, migration := range migrationData {
		// Open whisper file with driver
		w, err := whisper.Open(migration.wspFile)
		check(err)

		for i, archive := range w.Header.Archives {
			var wspPoints []whisper.Point

			// Give a name to the retention
			retentionName := fmt.Sprintf("%d-%d", archive.SecondsPerPoint, archive.Points)

			// Go through points
			points, err := w.DumpArchive(i)
			if err != nil {
				if *verbose {
					fmt.Println("Unexpected EOF:", retentionName, "from", migration.wspFile)
				}
				continue
			}
			for _, point := range points {
				// Skip the point on certain conditions
				if point.Value == 0 {
					continue
				}
				if point.Timestamp < from || point.Timestamp > until {
					continue
				}
				
				wspPoints = append(wspPoints, point)
			}

			// If there is nothing to write, skip file creation
			if len(wspPoints) == 0 {
				if *verbose {
					fmt.Println("Skipped:", retentionName, "from", migration.wspFile)
				}
				continue
			}

			// Makes sure the directory exists
			path := *exportPath + "/" + retentionName
			os.MkdirAll(path ,0755);

			// Write file
			func() {
				// Open file and prepare writer
				f, err := os.Create(path + "/" + migration.exportFileName)
				check(err)
				defer f.Close()

				var gf *gzip.Writer
				var fw *bufio.Writer
				if *gzipped {
					gf = gzip.NewWriter(f)
					fw = bufio.NewWriter(gf)
					defer gf.Close()
				} else {
					fw = bufio.NewWriter(f)
				}

				// Print all points
				for _, point := range wspPoints {
					line := migration.lineprotocol(point, archive.SecondsPerPoint) + "\n"
					_, err := fw.WriteString(line)
					check(err)
				}

				// Flush writer and close file
				fw.Flush()
			}() // <-- function call to defer

			if *verbose {
				fmt.Println("Exported:", path + "/" + migration.exportFileName, "from", migration.wspFile)
			} else {
				fmt.Printf("\rExported: %2d", k)
			}
		}

		w.Close()
	}
}


// Check errors
func check(e error) {
	if e != nil {
		panic(e)
	}
}


// Source: https://gist.github.com/m4ng0squ4sh/3dcbb0c8f6cfe9c66ab8008f55f8f28b
// askForConfirmation asks the user for confirmation. A user must type in "yes" or "no" and
// then press enter. It has fuzzy matching, so "y", "Y", "yes", "YES", and "Yes" all count as
// confirmations. If the input is not recognized, it will ask again. The function does not return
// until it gets a valid response from the user.
func askForConfirmation(s string) bool {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Printf("%s [y/n]: ", s)

		response, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		response = strings.ToLower(strings.TrimSpace(response))

		if response == "y" || response == "yes" {
			return true
		} else if response == "n" || response == "no" {
			return false
		}
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


// Generate the influxdb line protocol string for a given point
func (migrationData *MigrationData) lineprotocol(point whisper.Point, factor uint32) string {
	var line string
	line += migrationData.measurement
	line += migrationData.tags
	line += " "
	line += migrationData.field + "=" + strconv.FormatFloat(math.Ceil(point.Value * float64(factor)), 'f', -1, 64)
	line += " "
	line += strconv.FormatInt(int64(point.Timestamp), 10)
	return line
}


// Get measurement, tags and field by matching the whisper filename with a
// pattern in the config file
// This part is inspired by the project https://github.com/influxdata/whisper-migrator
func (migrationData *MigrationData) assignConfig() {

	wspMeasurement := migrationData.wspFile
	wspMeasurement = strings.TrimSuffix(wspMeasurement, ".wsp")
	wspMeasurement = strings.Replace(wspMeasurement, "/", ".", -1)
	wspMeasurement = strings.Replace(wspMeasurement, ",", "_", -1)
	wspMeasurement = strings.Replace(wspMeasurement, " ", "_", -1)

	// Filename matching
	// TODO catch strings that don't match to the end
	var tagConfig MigrationConfig
	var matched []string
	var matchedArr [][]string
	var wildcards [][]string
	filenameMatched := false
	for _, tagConfig = range migrationConfig {
		reWild := regexp.MustCompile("{{\\s*(\\S+)\\s*}}")

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


	// If measurement or field parameters is not defined
	if tagConfig.Measurement == "" {
		parts := strings.Split(wspMeasurement, ".")
		migrationData.measurement = parts[len(parts)-1]
	}
	if tagConfig.Field == "" {
		migrationData.field = "value"
	}


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
