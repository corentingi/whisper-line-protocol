# Whisper to InfluxDB line protocol files

Script to export whisper data to *influxdb line protocol*. See http://docs.influxdata.com/influxdb

This is in part inspired by the project https://github.com/influxdata/whisper-migrator

The migration is for the moment a 2 steps process:
- Filtering and converting Whisper files to line protocol files
- Importing to InfluxDB using the `influx -import` command

## Usage

Usage example:

```
whisper-line-protocol \
  -wsp-path=/whisper/data \
  -export-path=/whisper/export \
  -config-file=config.json \
  -retentions="autogen,one_day,two_month" \
  -database=export \
  -from=1483228800 \
  -gz
```

To simplify the `-retention` argument, it is a list of name in the same order they appear in the Whisper file.

## Config file

The config file needs to be a Json file with the following structure:
```
[
  {
    "pattern": "stats.{{ host }}.system.load.load.{{ field }}",
    "measurement": "load",
    "tags": [
      {
        "tagkey": "host",
        "tagvalue": "{{ host }}"
      }
    ],
    "field": "{{ field }}"
  }
]
```

You can use placeholders like `{{ host }}` that will then be replaces in *measurement*, *tags* and *field* parameters.
There is no error catching for the moment so make sure to add all the parameters for each pattern in the Json file.


## Data format

This version will output integer values in the line protocol format `154i`:
`measurement,tag1=value1 field1=154i,field2=89i 1481515200`

**The timestamp is format using seconds**.
This means you have to use the `s` precision when importing to InfluxDB.



## Import process

The import is done using the following command:

```
influx -port 8086 -import -compressed -precision "s" -pps 0 -path 10-autogen.txt.gz
```


## TODO

- Clean the code
- Input a whisper file list to process (Instead of all the files in the given folder)
- Input whisper files from tar.gz archive
- Stop and restart option (with a state file to save)
