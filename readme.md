# RzeszowGTFS

## Description
Creates GTFS file for [ZTM Rzeszów](https://ztm.rzeszow.pl/).
Data comes from ZTM Rzeszów's [ownCloud storage](https://chmura.ztm.rzeszow.pl/index.php/s/UY5an6Qk8CZHmCf), which contains zip archives with TranchXChange files.

## Running

Python3](https://www.python.org) (version 3.6 or later) is required with 2 additional libraries:
- [lxml](https://pypi.org/project/lxml/) and
- [requests](https://pypi.org/project/requests/)

Before launching install required libs with `pip3 install -r requirements.txt`.

This script has two modes of operation: single-file and merge.
The script has to be launched with either `python3 rzeszowgtfs.py -s FILE_NAME` or `python3 rzeszowgtfs.py -m`.

### Merge
This will create a GTFS files from all files valid today and in the future.
ZTM Rzeszów uploads a new file every couple days and GTFS-consuming apps usually need 2/3 days to process new file.
This mode makes sure that users always see newest schedules (and even allows them to plan ahead for a bit).

Launch this mode with `python3 rzeszowgtfs.py --merge` or `python3 rzeszowgtfs.py -m`.
The produced GTFS will be called `rzeszow.zip`.

The script will create a directory called `feeds` that will contain individual GTFS files,
in order not to re-parse them each time this script is called.


This mode accepts 2 more arguments:
- **--remerge** / **-rm**: Forces the merging of all feeds/ GTFSs, even if nothing changed;
- **--reparse** / **-rp**: Forces the recreation of each individual file.

### Single-File
This mode simply create a GTFS corresponding to a particular TransXChange file on [ZTM Rzeszów's cloud storage](https://chmura.ztm.rzeszow.pl/index.php/s/UY5an6Qk8CZHmCf).  
The name of the zip file from [ZTM's cloud](https://chmura.ztm.rzeszow.pl/index.php/s/UY5an6Qk8CZHmCf) has to be provided after `-s` or `--single-file`, for example:
`python3 rzeszowgtfs.py -s 09.01.2020.Rzeszow.zip` or `python3 rzeszowgtfs.py --single-file 09.01.2020.Rzeszow.zip`

## License

*TristarGTFS* is provided under the MIT license, included in the `license.md` file.
