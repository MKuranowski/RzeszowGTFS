# RzeszowGTFS

## Description
Creates GTFS file for [ZTM Rzeszów](https://ztm.rzeszow.pl/).
Data comes from [Rzeszów's open data portal](https://otwartedane.erzeszow.pl/dataset/rozklady-jazdy-gtfs).
which contains ugly GTFS data.

I assume that those TransXChange files are considered "public sector information" by Polish law,
and are subject to laws described in [ustawa o ponownym wykorzystywaniu informacji sektora publicznego](https://isap.sejm.gov.pl/isap.nsf/DocDetails.xsp?id=WDU20160000352).

Gmina Miasto Rzeszów has expressed their requirements (in accordance with article 11 of the aformentioned legislation) on this website:
<https://bip.erzeszow.pl/pl/319-informacja-publiczna-ponowne-wykorzystywanie-informacji-sektora-publicznego/4570-zasady-dostepu-do-informacji-publicznej.html>.

Exposing info from `attributions.txt` _should_ satisfy the first point of those requirements.

## Running

[Python3](https://www.python.org) (version 3.12 or later) is required with 2 additional libraries:
- [impuls](https://pypi.org/project/impuls/) and
- [requests](https://pypi.org/project/requests/)

Before launching install required libs with `pip install -r requirements.txt`.

### Modifications to the original GTFS files

- Merging multiple routes.
    ZTM Rzeszów publishes a new file with with every schedule change, violating the GTFS
    specification. This script merges any current and future files to ensure schedules
    are contained in a single GTFS file.
- Route deduplication
- Route color unification
- Stop name prettification
- Trip headsign prettification
- Correct request stop markings in stop_times

## License

*RzeszowGTFS* is provided under the MIT license, included in the `license.md` file.
