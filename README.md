# Whisper to InfluxDB line protocol files

Script to export whisper data to influxdb line protocol files

## Usage

Go get the sources
```
go get github.com/corentingi/whisper-line-protocol
```


```
whisper-line-protocol -wsp-path=/whisper/data -export-path=/tmp/export -config-file=config.json -database=export -keep-dir-structure=false
```

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
