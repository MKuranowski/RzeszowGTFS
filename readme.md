# RzeszowGTFS

## Description
Creates GTFS file for [ZTM Rzeszów](https://ztm.rzeszow.pl/).
Data comes from [Rzeszów's open data portal](https://otwartedane.erzeszow.pl/dataset/rozklad-jazdy-transxchange).
which contains zip archives with TranchXChange files.

I assume that those TransXChange files are considered "public sector information" by Polish law,
and are subject to laws described in [ustawa o ponownym wykorzystywaniu informacji sektora publicznego](https://isap.sejm.gov.pl/isap.nsf/DocDetails.xsp?id=WDU20160000352).

Gmina Miasto Rzeszów has expressed their requirements (in accordance with article 11 of the aformentioned legislation) on this website:
<https://bip.erzeszow.pl/pl/319-informacja-publiczna-ponowne-wykorzystywanie-informacji-sektora-publicznego/4570-zasady-dostepu-do-informacji-publicznej.html>.

Exposing info from `attributions.txt` _should_ satisfy the first point of those requirements.

## Running

[Python3](https://www.python.org) (version 3.6 or later) is required with 2 additional libraries:
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
This mode simply creates a GTFS corresponding to a particular TransXChange file in the [TransXChange dataset](https://otwartedane.erzeszow.pl/dataset/rozklad-jazdy-transxchange).  
The name of the target resouce name has to be provided in the `-s`/`--single-file` argument, for example:
`python3 rzeszowgtfs.py -s TransXChange20210201120555` or `python3 rzeszowgtfs.py --single-file TransXChange20210201120555`

## License

*RzeszowGTFS* is provided under the MIT license, included in the `license.md` file.
