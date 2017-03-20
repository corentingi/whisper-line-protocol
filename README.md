# Whisper to InfluxDB line protocol files

Script to export whisper data to influxdb line protocol files like the sample data file influxdata provides in the docs: https://s3.amazonaws.com/noaa.water-database/NOAA_data.txt

This is inspired by the project https://github.com/influxdata/whisper-migrator


## Usage

Usage example:

```
whisper-line-protocol \
  -wsp-path=/whisper/data \
  -export-path=/whisper/export \
  -config-file=config.json \
  -retentions="autogen,one_day,two_month" \
  -gz \
  -database=export \
  -from=1483228800
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
There is no error catching for the moment so make sure to add all the parameters for each pattern in the Json file.


## Import process

Once the whisper files have been processed, you can import them to influxdb with this kind of command:
```
mkdir -p "/whisper/export/done/"
for f in /whisper/export/*.txt.gz; do
    echo $f
    influx -import -path=$f -precision=s -compressed > /dev/null
    mv $f "/whisper/export/done/"
done
```


## TODO

- Clean the code
- Input a whisper file list to process
- Input whisper files from tar.gz archive
- Stop and restart option (with a state file to save)
