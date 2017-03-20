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

type MigrationBuffer struct {
	Buffer  *bufio.Writer
	GzBuffer    *gzip.Writer
	File      *os.File
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
var buffers map[uint32]MigrationBuffer = make(map[uint32]MigrationBuffer)
var exportedFileNumber = 0
var (
	verbose        = flag.Bool("verbose", false, "Configuration file for measurement and tags.")
	wspPath        = flag.String("wsp-path", "", "Whisper files folder path.")
	exportPath     = flag.String("export-path", "", "Target directory where line protocol files will be created.")
	configFile     = flag.String("config-file", "", "Configuration file for measurement and tags.")
	fromFlag       = flag.Uint("from", 0, "Only export points after the given timestamp.")
	untilFlag      = flag.Uint("until", uint(^uint32(0)), "Only export points before the given timestamp.")
	gzipped        = flag.Bool("gz", false, "Export data in a gzipped file.")
	exportZeros    = flag.Bool("zeros", false, "Export null values (equal to zero). Those are ignored by default.")
	database       = flag.String("database", "graphite" ,"Name of the influxdb database to use in export context.")
	retentionsStr  = flag.String("retentions", "" ,"Coma-separated retention names to use in export context.")
)

var retentions []string

func main() {
	flag.Parse()
	retentions = strings.Split(*retentionsStr, ",")

	// List wsp files and figure out tags, measurements, file names, etc.
	migrations := ListMigrations(*wspPath, *configFile)

	// Time boundaries
	var from uint32 = uint32(*fromFlag)
	var until uint32 = uint32(*untilFlag)

	// Warning starting exporting
	fmt.Println("----------------")
	fmt.Println("Exporting", len(migrations), "series to", *exportPath)
	if askForConfirmation("Proceed ?") == false {
		os.Exit(1)
	}
	fmt.Println("----------------")

	// Go through wsp files and export data
	for k, migration := range migrations {
		migration.export(from, until)

		// Notify the file was exported
		if *verbose {
			fmt.Println("Exported:", migration.wspFile)
		} else {
			fmt.Printf("\rExported: %2d series", k + 1)
		}
	}
	fmt.Println()

	// Close all buffers
	for _, buffer := range buffers {
		buffer.Buffer.Flush()
		if *gzipped {
			buffer.GzBuffer.Flush()
			buffer.GzBuffer.Close()
		}
		buffer.File.Close()
	}
}


// Check errors
func check(e error) {
	if e != nil {
		panic(e)
	}
}


func RetrieveMigrationBuffer(rate uint32) MigrationBuffer {
	buffer, ok := buffers[rate]

	// Create buffer if it doesnt exist already
	if !ok {
		// Figure out path of the file
		retention := RetentionPolicyName(rate)
		var path string = *exportPath + "/" + fmt.Sprintf("%d", rate) + "-" + retention + ".txt"
		if *gzipped {
			path += ".gz"
		}

		// Make sure directory exist or create it
		os.MkdirAll(*exportPath ,0755);

		// Open the file
		var err error
		buffer.File, err = os.Create(path)
		check(err)

		if *gzipped {
			buffer.GzBuffer = gzip.NewWriter(buffer.File)
			buffer.Buffer = bufio.NewWriter(buffer.GzBuffer)
		} else {
			buffer.Buffer = bufio.NewWriter(buffer.File)
		}

		buffers[rate] = buffer

		// Write the context to the buffer
		buffer.Buffer.WriteString(LineProtocolContext(*database, retention))
	}
	return buffer
}


func RetentionPolicyName(rate uint32) string {
	if len(retentions) > 0 {
		var current string
		current, retentions = retentions[0], retentions[1:]
		return current
	}
	return fmt.Sprintf("%d", rate)
}


func LineProtocolContext(database, retention string) string {
	context := "# DML\n# CONTEXT-DATABASE: " + database
	context += "\n# CONTEXT-RETENTION-POLICY: " + retention + "\n\n"
	return context
}


// Export the series described in the migration object
func (migration *MigrationData) export(from, until uint32) {
	// Open whisper file with driver
	w, err := whisper.Open(migration.wspFile)
	if err != nil {
		fmt.Println("\nError opening file:", err)
		return
	}

	for i, archive := range w.Header.Archives {
		// retrieve the buffer
		buffer := RetrieveMigrationBuffer(archive.SecondsPerPoint)

		// Go through points
		points, err := w.DumpArchive(i)
		if err != nil {
			if *verbose {
				fmt.Println("\nError reading:", migration.wspFile)
			}
			continue
		}
		for _, point := range points {
			// Skip the point on certain conditions
			if !*exportZeros && point.Value == 0 {
				continue
			}
			if point.Timestamp < from || point.Timestamp > until {
				continue
			}

			// Write the point to file
			line := migration.lineprotocol(point, archive.SecondsPerPoint) + "\n"
			_, err := buffer.Buffer.WriteString(line)
			check(err)
		}
	}

	w.Close()
}


// List all the migrations in a migration array
func ListMigrations(wspPath, configFile string) []MigrationData {
	// List files
	fileList := listWspFiles(wspPath)

	// Open migration config file
	config := LoadConfigFile(configFile)

	fmt.Println("Checking files to export...")

	var migrationData []MigrationData
	for _, wspFile := range fileList {
		data := MigrationData{}
		data.wspFile = wspFile
		data.relativePath = strings.TrimPrefix(wspFile, wspPath)

		// Figure exported filename
		exportedFileNumber += 1
		data.exportFileName = fmt.Sprintf("%08d.txt", exportedFileNumber)

		// Add the .gz if the export needs to be compressed
		if *gzipped {
			data.exportFileName += ".gz"
		}

		// Assign the right measurment, field and tags
		data.assignConfig(config)

		if data.matched {
			migrationData = append(migrationData, data)
		} else if *verbose {
			fmt.Println("File didn't match any config patterns: ", data.wspFile)
		}
	}

	return migrationData
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


func AskForText(s string) string {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Printf("%s: []", s)

		response, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		response = strings.ToLower(strings.TrimSpace(response))

		return response
	}
}


// Create the list of wsp files
func listWspFiles(searchDir string) []string {
	var fileList []string

	err := filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
		if os.IsNotExist(err) { //search dir does not exist
			return nil
		}
		// Only add wsp files to the list
		if strings.HasSuffix(f.Name(), "wsp") {
			fileList = append(fileList, path)
		}
		return nil
	})
	if err != nil {
		fmt.Println("Error listing files:")
		fmt.Println(err)
	}

	return fileList
}


// Read the config file and populate migrartionData.tagConfigs
func LoadConfigFile(filename string) []MigrationConfig {
	var migrationConfig []MigrationConfig

	raw, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Println("Can't read config file:", filename)
		panic(err)
	}
	
	err = json.Unmarshal(raw, &migrationConfig)
	if err != nil {
		fmt.Println("Can't unmarshal config file json:")
		panic(err)
	}

	return migrationConfig
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
func (migrationData *MigrationData) assignConfig(migrationConfig []MigrationConfig) bool {

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
		return false
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

	return true
}
