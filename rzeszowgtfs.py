from datetime import date, datetime, timedelta
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass
from contextlib import closing
from warnings import warn
from tempfile import TemporaryFile
from lxml import etree
import argparse
import requests
import zipfile
import math
import csv
import os
import io
import re

# METADATA

__title__ = "RzeszowGTFS"
__author__ = "Mikolaj Kuranowski"
__email__ = "mikolaj@mkuran.pl"
__license__ = "MIT"

# CONSTANTS

PARSE_QUEUE = [
    "{http://www.transxchange.org.uk/}StopPoints",
    "{http://www.transxchange.org.uk/}RouteSections",
    "{http://www.transxchange.org.uk/}Routes",
    "{http://www.transxchange.org.uk/}Lines",
    "{http://www.transxchange.org.uk/}VehicleJourneys",
    "{http://www.transxchange.org.uk/}Services",
    "{http://www.transxchange.org.uk/}ServiceCalendars",
]

PARSE_SWITCHCASE = {
    "{http://www.transxchange.org.uk/}RouteSections": "shape_sections",
    "{http://www.transxchange.org.uk/}Routes": "shapes",
    "{http://www.transxchange.org.uk/}StopPoints": "stops",
    "{http://www.transxchange.org.uk/}Lines": "routes",
    "{http://www.transxchange.org.uk/}VehicleJourneys": "trips",
    "{http://www.transxchange.org.uk/}Services": "service_to_daytypes",
    "{http://www.transxchange.org.uk/}ServiceCalendars": "dates",
}

DIRECTION_NAMES = {
    "inbound": "0",
    "clockwise": "0",
    "outbound": "1",
    "antiClockwise": "1"
}

FILES_TO_COPY = {
    "calendar_dates.txt": ["date", "service_id", "exception_type"],

    "shapes.txt": ["shape_id", "shape_pt_sequence",
                   "shape_pt_lat", "shape_pt_lon"],

    "trips.txt": ["route_id", "service_id", "trip_id",
                  "shape_id", "trip_headsign"],

    "stop_times.txt": ["trip_id", "arrival_time", "departure_time",
                       "stop_id", "stop_sequence"]
}

XML_NS = {"d": "DAV:", "t": "http://www.transxchange.org.uk/"}

URL_LIST_ALL = "https://otwartedane.erzeszow.pl/v1/datasets/slug_full_view/?format=json&slug=rozklad-jazdy-transxchange"  # noqa
URL_SINGLE_FILE = "https://otwartedane.erzeszow.pl/media/resources/"
FILE_HOSTNAME = "otwartedane.erzeszow.pl"

# HELPER FUNCTIONS


def _escape_csv(value):
    return '"' + value.replace('"', '""') + '"'


def _clear_dir(dir):
    "Removes every file from `dir`, or creates `dir` if it doesn't exist"
    if not os.path.exists(dir):
        os.mkdir(dir)
    for file in [os.path.join(dir, i) for i in os.listdir(dir)]:
        os.remove(file)


def _runtime_to_sec(runtime):
    "Returns the amound of seconds for txc:RunTime"
    runtime = _txt(runtime)
    total = 0

    if not re.match(r"PT(?:(?:\d+)[HMS])+", runtime):
        raise ValueError(f"illegal value for _runtime_to_sec: {runtime}")

    s = re.search(r"(\d+)S", runtime)
    m = re.search(r"(\d+)M", runtime)
    h = re.search(r"(\d+)H", runtime)

    if s:
        total += int(s[1])

    if m:
        total += int(m[1]) * 60

    if h:
        total += int(h[1]) * 3600

    return total


def _haversine(pt1, pt2):
    "Calculate haversine distance (in km)"
    lat1, lon1 = map(math.radians, pt1)
    lat2, lon2 = map(math.radians, pt2)
    lat = lat2 - lat1
    lon = lon2 - lon1

    d = math.sin(lat * 0.5) ** 2 + math.cos(lat1) * math.cos(lat2) * \
        math.sin(lon * 0.5) ** 2

    return 2 * 6371 * math.asin(math.sqrt(d))


def _txt(item):
    return item.text.strip()


def _true(item):
    return _txt(item) == "true"


def _int(item):
    return int(_txt(item))


class _Time:
    "Represents a time value"

    def __init__(self, seconds):
        self.m, self.s = divmod(int(seconds), 60)
        self.h, self.m = divmod(self.m, 60)

    def __str__(self):
        "Return GTFS-compliant string representation of time"
        return f"{self.h:0>2}:{self.m:0>2}:{self.s:0>2}"

    def __repr__(self): return "<Time " + self.__str__() + ">"
    def __int__(self): return self.h * 3600 + self.m * 60 + self.s
    def __add__(self, other): return _Time(self.__int__() + int(other))
    def __sub__(self, other): return self.__int__() - int(other)
    def __lt__(self, other): return self.__int__() < int(other)
    def __le__(self, other): return self.__int__() <= int(other)
    def __gt__(self, other): return self.__int__() > int(other)
    def __ge__(self, other): return self.__int__() >= int(other)
    def __eq__(self, other): return self.__int__() == int(other)
    def __ne__(self, other): return self.__int__() != int(other)

    @classmethod
    def from_str(cls, string):
        str_split = list(map(int, string.split(":")))
        if len(str_split) == 2:
            return cls(str_split[0]*3600 + str_split[1]*60)
        elif len(str_split) == 3:
            return cls(str_split[0]*3600 + str_split[1]*60 + str_split[2])
        else:
            raise ValueError("invalid string for _Time.from_str(), "
                             "{} (should be HH:MM or HH:MM:SS)".format(string))


@dataclass
class _XmlFile:
    url: str
    ver: str
    mtime: datetime
    start: date = date.min
    end: date = date.max


# DATA PARSING #


class RzeszowGtfs:
    def __init__(self, file_url, feed_version):
        """
        Initialize variables of the parser

        :param str file_url: URL of the zip archive to be downloaded
        :param str feed_veriosn: The value for feed_version
        """
        self.temp_file = TemporaryFile(mode="w+b", prefix="rzeszowgtfs_", suffix=".xml")
        self.xml_parser = None
        self.file_url = file_url

        self.download_time = None
        self.version = feed_version

        self.route_ids = {}
        self.route_dirs = {}
        self.sections = {}
        self.used_routes = set()

        self.shape_headsigns = {}
        self.shape_direction = {}
        self.shape_ignore = set()
        self.daytype_to_services = {}

    def download(self):
        "Download the requested file and extract it"
        print("\033[1A\033[K" f"Requesting {self.file_url!r}")
        req = requests.get(self.file_url, stream=True, verify=False)
        req.raise_for_status()

        self.download_time = datetime.today()
        for chunk in req.iter_content(chunk_size=1024*1024):
            self.temp_file.write(chunk)

    def init_parser(self):
        "Initialize the parser"
        self.temp_file.seek(0)
        self.xml_parser = etree.iterparse(self.temp_file, events={"start", "end"})

    @staticmethod
    def static_files(pub_name, pub_url, version, download_time):
        "Create GTFS files that don't change: feed_info.txt and agency.txt"
        # agency.txt
        file = open("gtfs/agency.txt", "w", encoding="utf-8", newline="\r\n")
        file.write("agency_id,agency_name,agency_url,agency_timezone,agency_lang\n")
        file.write('0,ZTM Rzeszów,"http://ztm.rzeszow.pl",Europe/Warsaw,pl\n')
        file.close()

        # attributions.txt
        dload_timestring = download_time.strftime("%Y-%m-%d %H:%M:%S")

        file = open("gtfs/attributions.txt", mode="w", encoding="utf-8", newline="\r\n")
        file.write("organization_name,is_producer,is_operator,is_authority,"
                   "is_data_source,attribution_url\n")

        file.write(f'"Data provided by: ZTM Rzeszów (retrieved {dload_timestring})",0,1,1,1,'
                   '"https://chmura.ztm.rzeszow.pl/index.php/s/UY5an6Qk8CZHmCf"')

        file.close()

        # feed_info.txt
        if pub_name and pub_url:
            file = open("gtfs/feed_info.txt", "w", encoding="utf-8", newline="\r\n")
            file.write("feed_publisher_name,feed_publisher_url,feed_lang,feed_version\n")
            file.write(",".join([
                _escape_csv(pub_name), _escape_csv(pub_url), "pl", version
            ]) + "\n")
            file.close()

    def service_to_daytypes(self):
        """
        Maps TransXChange service_id to daytype ids.
        The mapping is saved to self.daytype_to_services
        """
        print("\033[1A\033[K" "Mapping services→day_types (txc:Services)")

        for event, elem in self.xml_parser:

            if elem.tag == "{http://www.transxchange.org.uk/}Services" and \
                    event == "end":
                break

            if elem.tag != "{http://www.transxchange.org.uk/}Service" or \
                    event != "end":
                continue

            service_id = _txt(elem.find("t:ServiceCode", XML_NS))

            day_types = elem.findall("t:Extensions/t:DayTypes/t:DayTypeRef", XML_NS)

            if day_types is None:
                raise ValueError(f"empty DayTypes inside file ver {self.version} "
                                 f"(service {service_id!r})")

            for day_type in day_types:
                day_type_id = _txt(day_type)

                if day_type_id not in self.daytype_to_services:
                    self.daytype_to_services[day_type_id] = []

                self.daytype_to_services[day_type_id].append(service_id)

    def shape_sections(self):
        """
        Parses txc:RouteSections and saves the points of each section to
        self.sections
        """
        print("\033[1A\033[K" "Loading shape sections (txc:RouteSections)")

        for event, elem in self.xml_parser:

            if elem.tag == "{http://www.transxchange.org.uk/}RouteSections" and \
                    event == "end":
                break

            if elem.tag != "{http://www.transxchange.org.uk/}RouteSection" or \
                    event != "end":
                continue

            link = elem.find("t:RouteLink", XML_NS)

            section_id = elem.attrib["id"]
            locations = list(link.findall("t:Track/t:Mapping/t:Location", XML_NS))
            points = []

            # First and last points are bus stop coordinates
            for location in locations[1:-1]:
                lon = _txt(location.find("t:Longitude", XML_NS))
                lat = _txt(location.find("t:Latitude", XML_NS))
                points.append((lat, lon))

            self.sections[section_id] = points

    def shapes(self):
        """
        Parses shapes from txc:Routes.
        Route points are fetched from self.sections.
        Also saves some data for trips parsing to:
        self.route_dirs, self.shape_ignore,
        self.shape_headsigns and self.shape_direction
        """
        print("\033[1A\033[K" "Parsing shapes (txc:Routes)")

        file = open("gtfs/shapes.txt", mode="w", encoding="utf-8", newline="")
        wrtr = csv.writer(file)

        wrtr.writerow(["shape_id", "shape_pt_lat", "shape_pt_lon",
                       "shape_pt_sequence"])

        for event, elem in self.xml_parser:

            if elem.tag == "{http://www.transxchange.org.uk/}Routes" and \
                    event == "end":
                break

            if elem.tag != "{http://www.transxchange.org.uk/}Route" or \
                    event != "end":
                continue

            point_enum = -1
            shape_id = elem.attrib["id"]

            print("\033[1A\033[K" f"Parsing shapes (txc:Routes) - {shape_id}")

            route_ref = _txt(elem.find("t:Extensions/t:LineRef", XML_NS))
            default_shape = _true(elem.find("t:Extensions/t:IsDefault", XML_NS))
            technical_shape = _true(elem.find("t:Extensions/t:IsTechnical", XML_NS))
            headsign = _txt(elem.find("t:Extensions/t:DisplayDescription", XML_NS))
            direction_name = _txt(elem.find("t:Extensions/t:Direction", XML_NS))

            if route_ref not in self.route_dirs:
                self.route_dirs[route_ref] = {}

            if technical_shape:
                self.shape_ignore.add(shape_id)
                continue

            # Direction name for route
            direction = DIRECTION_NAMES.get(direction_name, "")

            if direction and default_shape:
                self.route_dirs[route_ref][direction] = headsign

            elif direction and direction not in self.route_dirs[route_ref]:
                self.route_dirs[route_ref][direction] = headsign

            elif direction == "":
                warn(f"invalid direction name: {direction_name!r}")

            # Headsign for trips
            self.shape_headsigns[shape_id] = headsign

            # direction_id for trips
            self.shape_direction[shape_id] = direction

            # Dump into shapes.txt
            section_ids = [_txt(i) for i in
                           elem.findall("t:RouteSectionRef", XML_NS)]

            for section_id in section_ids:
                section_points = self.sections[section_id]
                for point_lat, point_lon in section_points:
                    point_enum += 1
                    wrtr.writerow([shape_id, point_lat, point_lon, point_enum])

        print("\033[1A\033[K" "Parsing shapes (txc:Routes) - done")

        file.close()

    def stops(self):
        "Parses stops from txc:StopPoints"
        print("\033[1A\033[K" "Parsing stops (txc:StopPoints)")

        file = open("gtfs/stops.txt", mode="w", encoding="utf-8", newline="")
        wrtr = csv.writer(file)
        wrtr.writerow(["stop_id", "stop_name", "stop_lat", "stop_lon"])

        for event, elem in self.xml_parser:

            if elem.tag == "{http://www.transxchange.org.uk/}StopPoints" and \
                    event == "end":
                break

            if elem.tag != "{http://www.transxchange.org.uk/}StopPoint" or \
                    event != "end":
                continue

            stop_id = int(elem.attrib["id"])
            stop_name = _txt(elem.find("t:Descriptor/t:CommonName", XML_NS))
            stop_lat = _txt(elem.find("t:Place/t:Location/t:Latitude", XML_NS))
            stop_lon = _txt(elem.find("t:Place/t:Location/t:Longitude", XML_NS))

            wrtr.writerow([stop_id, stop_name, stop_lat, stop_lon])

        file.close()

    def routes(self):
        """
        Parses routes from txc:Lines with data from self.route_dirs
        Also maps txc:RouteRef to GTFS route_id in self.route_ids
        """
        print("\033[1A\033[K" "Parsing routes (txc:Lines)")

        file = open("gtfs/routes.txt", mode="w", encoding="utf-8", newline="")
        wrtr = csv.writer(file)

        wrtr.writerow(["agency_id", "route_id", "route_short_name",
                       "route_long_name", "route_type",
                       "route_color", "route_text_color"])

        for event, elem in self.xml_parser:

            if elem.tag == "{http://www.transxchange.org.uk/}Lines" and \
                    event == "end":
                break

            if elem.tag != "{http://www.transxchange.org.uk/}Line" or \
                    event != "end":
                continue

            # get ids and check elements
            route_ref = elem.attrib["id"]
            if route_ref is None:
                raise ValueError(f"no id of Line inside {self.version}")

            route_id = elem.find("t:MarketingName", XML_NS)

            if route_id is None:
                etree.dump(elem)
                raise ValueError("no MarketingName in the above route element")

            # parse data into strings
            route_id = _txt(route_id).upper()
            route_color = "000000" if route_id.startswith("N") else "DD3300"
            route_text_color = "FFFFFF"

            # route_long_name based an 'IsDefault' Routes
            route_name_0 = self.route_dirs.get(route_ref, {}).get("0", "") or \
                self.route_dirs.get(route_ref, {}).get("1", "")

            route_name_1 = self.route_dirs.get(route_ref, {}).get("1", "") or \
                self.route_dirs.get(route_ref, {}).get("0", "")

            # put name parts into one
            if route_name_0 and route_name_1:
                route_name = f"{route_name_0} — {route_name_1}"

            else:
                route_name = ""

            if route_id not in self.route_ids.values():
                wrtr.writerow(["0", route_id, route_id, route_name, "3",
                               route_color, route_text_color])

            self.route_ids[route_ref] = route_id

        file.close()

    def trips(self):
        """
        Parses trips from txc:VehicleJourneys
        Requires data from parsing routes and shapes:
        self.route_ids, self.shape_ignore, self.shape_headsigns and
        self.shape_direction
        """
        print("\033[1A\033[K" "Parsing trips (txc:VehicleJourneys)")

        file_trips = open("gtfs/trips.txt", mode="w", encoding="utf-8", newline="")
        wrtr_trips = csv.writer(file_trips)
        wrtr_trips.writerow(["route_id", "service_id", "trip_id",
                             "shape_id", "direction_id", "trip_headsign"])

        file_times = open("gtfs/stop_times.txt", mode="w", encoding="utf-8", newline="")
        wrtr_times = csv.writer(file_times)
        wrtr_times.writerow(["trip_id", "arrival_time", "departure_time",
                             "stop_id", "stop_sequence"])

        for event, elem in self.xml_parser:

            if elem.tag == "{http://www.transxchange.org.uk/}VehicleJourneys" and \
                    event == "end":
                break

            if elem.tag != "{http://www.transxchange.org.uk/}VehicleJourney" or \
                    event != "end":
                continue

            # Trip metadata
            route_ref = _txt(elem.find("t:LineRef", XML_NS))
            route_id = self.route_ids[route_ref]

            trip_id = _txt(elem.find("t:VehicleJourneyCode", XML_NS))
            service_id = _txt(elem.find("t:ServiceRef", XML_NS))
            shape_id = _txt(elem.find("t:JourneyPatternRef", XML_NS))

            print("\033[1A\033[K" f"Parsing trips (txc:VehicleJourneys) - {trip_id}")

            if shape_id in self.shape_ignore:
                continue

            headsign = self.shape_headsigns[shape_id]
            direction = self.shape_direction[shape_id]

            # Stop IDs
            stops = []

            stop_points = elem.findall(
                "t:Extensions/t:VehicleJourneyStopPoints/t:VehicleJourneyStopPoint",
                XML_NS,
            )

            for stop_point in stop_points:
                idx = _int(stop_point.find("t:Sequence", XML_NS))
                id = _int(stop_point.find("t:StopPointRef", XML_NS))
                stops.insert(idx, id)

            # Times
            time = _Time.from_str(_txt(elem.find("t:DepartureTime", XML_NS)))

            # Initialize `times` with the first timepoint
            times = [[trip_id, str(time), str(time), stops[0], 0]]

            timing_links = elem.findall("t:VehicleJourneyTimingLink", XML_NS)
            for idx, timing_link in enumerate(timing_links):

                time += _runtime_to_sec(timing_link.find("t:RunTime", XML_NS))
                times.append([trip_id, str(time), str(time), stops[idx + 1], idx + 1])

            # Ignore one-stop trips
            if len(times) == 1:
                continue

            # Dump to gtfs
            wrtr_trips.writerow([route_id, service_id, trip_id, shape_id,
                                direction, headsign])
            wrtr_times.writerows(times)

            # Save used route_id
            self.used_routes.add(route_id)

        print("\033[1A\033[K" "Parsing trips (txc:VehicleJourneys) - done")

        file_trips.close()
        file_times.close()

    def dates(self):
        "Parses calendar_dates. Requires self.daytype_to_services."
        print("\033[1A\033[K" "Parsing calendar_dates (txc:ServiceCalendars)")

        file = open("gtfs/calendar_dates.txt", mode="w", encoding="utf-8", newline="")
        wrtr = csv.writer(file)
        wrtr.writerow(["date", "service_id", "exception_type"])

        for event, elem in self.xml_parser:

            if elem.tag == "{http://www.transxchange.org.uk/}ServiceCalendars" and \
                    event == "end":
                break

            if elem.tag != "{http://www.transxchange.org.uk/}OperatingDay" or \
                    event != "end":
                continue

            date = _txt(elem.find("t:Date", XML_NS)).replace("-", "")
            for day_type in elem.findall("t:ServiceDayAssignment/t:DayTypeRef", XML_NS):
                day_type_id = _txt(day_type)
                for service_id in self.daytype_to_services[day_type_id]:
                    wrtr.writerow([date, service_id, "1"])

        file.close()

    def remove_unused_routes(self):
        print("\033[1A\033[K" "Removing unused routes")
        os.rename("gtfs/routes.txt", "gtfs/routes.txt.old")
        with open("gtfs/routes.txt.old", mode="r", encoding="utf-8", newline="") as in_buff, \
                open("gtfs/routes.txt", mode="w", encoding="utf-8", newline="") as out_file:

            # Wrap csv readers writers around file buffers
            in_csv = csv.reader(in_buff)
            out_csv = csv.writer(out_file)

            # Re-write header
            header = next(in_csv)
            out_csv.writerow(header)

            # Get route_id column index
            route_id_idx = header.index("route_id")

            # Rewrite rows with route_id in self.used_routes
            for row in in_csv:
                if row[route_id_idx] in self.used_routes:
                    out_csv.writerow(row)

    def close(self):
        """Close the underlaying temporary file"""
        self.temp_file.close()

    @staticmethod
    def compress(target="rzeszow.zip"):
        "Compress all created files to `target`"
        archive = zipfile.ZipFile(target, mode="w", compression=zipfile.ZIP_DEFLATED)
        for file in os.listdir("gtfs"):
            if file.endswith(".txt"):
                archive.write(os.path.join("gtfs", file), arcname=file)
        archive.close()

    @classmethod
    def parse(cls, file_name="", file_url="", file_version="", target="rzeszow.zip",
              pub_name="", pub_url=""):
        """Automatically creates a GTFS file"""
        print("\033[1A\033[K" "Clearing the gtfs/ directory")
        _clear_dir("gtfs")

        # Normalize file_name
        if file_name:
            file_name = file_name.casefold()
            if not file_name.endswith(".xml"):
                file_name += ".xml"

        # Ensure a valid version
        if file_url and not file_version:
            raise ValueError("RzeszowGTFS.parse requires file_version argument if using file_url")
        elif file_name:
            file_version = datetime.strptime(file_name, "transxchange%Y%m%d%H%M%S.xml") \
                           .strftime("%Y-%m-%d %H:%M:%S")

        # Ensure a valid file_url
        if file_url:
            # If url was provided, check the hostname
            if urlparse(file_url).hostname != FILE_HOSTNAME:
                raise ValueError(f"Rzeszow.parse can only parse files form {FILE_HOSTNAME!r}, "
                                 f"but this url was provided: {file_url!r}")
        elif file_name:
            file_url = urljoin(URL_SINGLE_FILE, file_name.casefold())

        else:
            raise ValueError("RzeszowGTFS.parse expects either a file_name or file_url!")

        # Parse the file
        local_parse_queue = PARSE_QUEUE.copy()

        print("\033[1A\033[K" "Downloading requested XML file")
        with closing(cls(file_url, file_version)) as self:
            self.download()
            self.init_parser()

            while local_parse_queue:

                look_for_tag = local_parse_queue.pop(0)
                inside_services = False

                event, elem = None, None

                while event != "start" or elem.tag != look_for_tag or inside_services:

                    try:
                        event, elem = next(self.xml_parser)

                    except StopIteration:
                        self.init_parser()
                        continue

                    # HOTFIX: <Services> tag contains <Lines> tags, which is different
                    #         then <Lines> tag containing route data
                    if not look_for_tag.endswith("Services"):
                        if event == "start" and elem.tag.endswith("Services"):
                            inside_services = True

                        elif event == "end" and elem.tag.endswith("Services"):
                            inside_services = False

                parse_function = PARSE_SWITCHCASE.get(look_for_tag)

                if parse_function:
                    getattr(self, parse_function)()

                else:
                    raise RuntimeError(f"Unable to handle XML section {look_for_tag!r}")

        self.remove_unused_routes()

        print("\033[1A\033[K" "Creating agency & feed_info files")
        self.static_files(pub_name, pub_url, self.version, self.download_time)

        print("\033[1A\033[K" f"Compressing into {target}")
        self.compress(target)


class MultiRzeszow:
    def __init__(self):
        "Initializes variables for multi-day parsing"
        self.today = date.today()
        self.changed = False
        self.files_xml: list[_XmlFile] = []

        self.version = ""
        self.download_time = None

        self.routes = None
        self.stops = None
        self.stop_conversion = None

        self.arch = None
        self.arch_v = None
        self.arch_feed = None
        self.arch_services = set()
        self.arch_shapes = set()

    def list_xml_files(self):
        "Lists all files available at ZTM Rzeszów WebDAV"
        req = requests.get(URL_LIST_ALL, verify=False)
        req.raise_for_status()
        resources = req.json()["resources"]

        if not resources:
            raise ValueError("empty repsonse for PROPFIND on Rzeszów WebDAV")

        for res in resources:
            # Check if this resource points to an XML file
            if res.get("extension", "").casefold() != "xml":
                continue

            # File version
            version_match = re.match(r"(\d?\d)\.(\d?\d)\.(\d{4})", res["description"])
            if not version_match:
                raise ValueError("One of TeansXChange resources doesn't contain version in the "
                                 f"description: {res['description']!r}")
            file_version = version_match[3] \
                + "-" + version_match[2].ljust(2, "0") \
                + "-" + version_match[1].ljust(2, "0")

            # File modification time
            file_mtime = datetime.strptime(
                res["name"].casefold(),
                "TransXChange%Y%m%d%H%M%S"
            )

            self.files_xml.append(_XmlFile(
                url=resources["file"],
                ver=file_version,
                mtime=file_mtime,
            ))

    def cleanup_xml_files(self):
        """
        Cleans the self.files_xml list:
        1. Removes files active in the future,
        2. Sorts them by dates,
        3. Assigns each file the start and end dates.
        """
        # Sort files
        self.files_xml = sorted(self.files_xml, key=lambda i: i.ver)

        # Generate start and end dates
        for idx, xml_file in enumerate(self.files_xml):
            file_date = datetime.strptime(xml_file.ver, "%Y-%m-%d").date()

            xml_file.start = file_date
            xml_file.end = file_date + timedelta(90)

            # Set the previous file's end date
            if idx > 0:
                self.files_xml[idx - 1].end = file_date - timedelta(1)

        # Remove files only applicable in the future
        self.files_xml = [i for i in self.files_xml if i.end >= self.today]

    def sync_files(self, reparse=False):
        """
        Makes sure that each entry in self.files_xml has a corresponding
        GTFS file inside the feeds/ directory.
        Also removes any excess files from feeds/.

        :param bool reparse: Force parsing of each file
        """
        if not os.path.exists("feeds"):
            os.mkdir("feeds")

        if reparse:
            _clear_dir("feeds")

        recreate_files = {i.ver: i for i in self.files_xml}

        self.version = "/".join([i.ver for i in self.files_xml])
        self.download_time = datetime.today()

        for file in os.scandir("feeds"):
            # Only GTFS files, so those matching YYYY-MM-DD.zip
            if not re.match(r"\d{4}-\d{2}-\d{2}\.zip", file.name):
                continue

            # Some basic data on file
            file_version = file.name[:10]
            file_mtime = datetime.fromtimestamp(file.stat().st_mtime)

            # File not required
            if file_version not in recreate_files:
                os.remove(file.path)

            # XML file was modified after creating the corresponding GTFS
            elif recreate_files[file_version].mtime > file_mtime:
                os.remove(file.path)

            else:
                del recreate_files[file_version]

        # If there are files that need to be recreated, create them
        if recreate_files:
            self.changed = True
            total_files = len(recreate_files)

            print()

            for idx, (version, file) in enumerate(recreate_files.items()):

                print("\033[2A\033[K" "Creating GTFS for missing version: "
                      f"{version} (file {idx+1}/{total_files})", end="\n\n")

                print("\033[1A\033[K" "Calling RzeszowGtfs.parse()")

                RzeszowGtfs.parse(
                    file_url=file.url,
                    target=os.path.join("feeds", version+".zip"),
                )

            print("\033[2A\033[K" + "All missing files created")

    @staticmethod
    def create_headers():
        "Creates CSV headers for files that will be appended multiple times"
        for filename, headers in FILES_TO_COPY.items():
            f = open(os.path.join("gtfs", filename),
                     mode="w", encoding="utf-8", newline="\r\n")

            f.write(",".join(headers) + "\n")
            f.close()

    def load_stops(self):
        "Loads into self.stops GTFS stops from self.arch"
        with self.arch.open("stops.txt") as buff:
            wrapped_buff = io.TextIOWrapper(buff, encoding="utf8", newline="")
            for row in csv.DictReader(wrapped_buff):

                # If it's the first time we see this stop_id, just save it and continue
                if row["stop_id"] not in self.stops:
                    self.stops[row["stop_id"]] = row
                    continue

                stop_pos = float(row["stop_lat"]), float(row["stop_lon"])

                # List all stops with same original stop_id
                # If any of them is closer then 10 meters to the one
                # we're considering we'll say it's the same
                # This also kinda assumes that all stop attributes are the same
                similar_stops = [(i, j) for (i, j) in self.stops.items()
                                 if j["stop_id"] == row["stop_id"]]

                for similar_stop_id, similar_stop_data in similar_stops:
                    similar_stop_pos = float(similar_stop_data["stop_lat"]), \
                                       float(similar_stop_data["stop_lon"])

                    distance = _haversine(stop_pos, similar_stop_pos)

                    if distance <= 0.01:
                        stop_key = self.arch_v, row["stop_id"]
                        self.stop_conversion[stop_key] = similar_stop_id
                        break

                # If there's no stop closer then 10m create a new entry
                else:
                    # Get a unused suffix for stop_id
                    stop_id_suffix = 1
                    while row["stop_id"] + ":" + str(stop_id_suffix) in self.stops:
                        stop_id_suffix += 1

                    # Save the stop under a different id
                    stop_id = row["stop_id"] + ":" + str(stop_id_suffix)
                    stop_key = self.arch_v, row["stop_id"]
                    self.stops[stop_id] = row
                    self.stop_conversion[stop_key] = stop_id

    def load_routes(self):
        "Loads into self.routes GTFS routes from self.arch"
        with self.arch.open("routes.txt") as buff:
            wrapped_buff = io.TextIOWrapper(buff, encoding="utf8", newline="")
            for row in csv.DictReader(wrapped_buff):
                if row["route_id"] not in self.routes:
                    self.routes[row["route_id"]] = row

    def copy_calendars(self):
        "Copies calendar_dates from self.arch GTFS"
        file = open("gtfs/calendar_dates.txt", mode="a", encoding="utf8", newline="")

        writer = csv.DictWriter(
            file,
            FILES_TO_COPY["calendar_dates.txt"],
            extrasaction="ignore"
        )

        with self.arch.open("calendar_dates.txt") as buff:
            wrapped_buff = io.TextIOWrapper(buff, encoding="utf8", newline="")
            for row in csv.DictReader(wrapped_buff):

                active_date = datetime.strptime(row["date"], "%Y%m%d").date()

                if self.arch_feed.start <= active_date <= self.arch_feed.end:
                    self.arch_services.add(row["service_id"])

                    writer.writerow({
                        "date": row["date"],
                        "service_id": self.arch_v + ":" + row["service_id"],
                        "exception_type": row["exception_type"]
                    })

        file.close()

    def copy_trips(self):
        "Copies trips from self.arch GTFS"

        file = open("gtfs/trips.txt", mode="a", encoding="utf8", newline="")
        writer = csv.DictWriter(file, FILES_TO_COPY["trips.txt"], extrasaction="ignore")

        with self.arch.open("trips.txt") as buff:
            wrapped_buff = io.TextIOWrapper(buff, encoding="utf8", newline="")
            for row in csv.DictReader(wrapped_buff):

                # Ignore trips which service is not active in version's effective range
                if row["service_id"] not in self.arch_services:
                    continue

                if row.get("shape_id"):
                    self.arch_shapes.add(row["shape_id"])
                    row["shape_id"] = self.arch_v + ":" + row["shape_id"]

                self.arch_trips.add(row["trip_id"])

                row["service_id"] = self.arch_v + ":" + row["service_id"]
                row["trip_id"] = self.arch_v + ":" + row["trip_id"]

                writer.writerow(row)

        file.close()

    def copy_times(self):
        "Copies stop_times from self.arch GTFS"
        file = open("gtfs/stop_times.txt", mode="a", encoding="utf8", newline="")
        writer = csv.DictWriter(file, FILES_TO_COPY["stop_times.txt"],
                                extrasaction="ignore")

        with self.arch.open("stop_times.txt") as buff:
            wrapped_buff = io.TextIOWrapper(buff, encoding="utf8", newline="")
            for row in csv.DictReader(wrapped_buff):

                # Ignore inactive trips
                if row["trip_id"] not in self.arch_trips:
                    continue

                row["trip_id"] = self.arch_v + ":" + row["trip_id"]

                stop_key = self.arch_v, row["stop_id"]
                row["stop_id"] = self.stop_conversion.get(stop_key, row["stop_id"])

                writer.writerow(row)

        file.close()

    def copy_shapes(self):
        "Copies shapes from self.arch GTFS"
        file = open("gtfs/shapes.txt", mode="a", encoding="utf8", newline="")
        writer = csv.DictWriter(file, FILES_TO_COPY["shapes.txt"],
                                extrasaction="ignore")

        with self.arch.open("shapes.txt") as buff:
            wrapped_buff = io.TextIOWrapper(buff, encoding="utf8", newline="")
            for row in csv.DictReader(wrapped_buff):

                if row["shape_id"] not in self.arch_shapes:
                    continue

                row["shape_id"] = self.arch_v + ":" + row["shape_id"]
                writer.writerow(row)

        file.close()

    def create_routes(self):
        "Dumps self.routes into the merged GTFS"
        # Open file
        file = open("gtfs/routes.txt", mode="w", encoding="utf8", newline="")

        writer = csv.DictWriter(
            file,
            ["agency_id", "route_id", "route_short_name",
             "route_long_name", "route_type", "route_color",
             "route_text_color"],
            extrasaction="ignore",
        )

        writer.writeheader()

        # Export routes to GTFS
        for route_id, row in self.routes.items():
            row["agency_id"] = "0"
            writer.writerow(row)

        file.close()

    def create_stops(self):
        "Dumps self.stops into the merged GTFS"
        # Open file
        file = open("gtfs/stops.txt", mode="w", encoding="utf8", newline="")

        writer = csv.DictWriter(
            file,
            ["stop_id", "stop_name", "stop_lat", "stop_lon"],
            extrasaction="ignore",
        )

        writer.writeheader()

        # Export stops to GTFS
        for stop_id, row in self.stops.items():
            row["stop_id"] = stop_id
            writer.writerow(row)

        file.close()

    def merge(self):
        "Merges files from files/ directory, according to self.files_xml"
        _clear_dir("gtfs")

        self.routes = {}
        self.stops = {}

        # (version, stop_id): merged_stop_id, but only when merged_stop_id != stop_id
        self.stop_conversion = {}

        # Create files which will be copied line-by-line
        self.create_headers()

        # Read feeds
        for feed in self.files_xml:

            print("\033[1A\033[K" f"Merging version {feed.ver}")

            feed_gtfs = os.path.join("feeds", feed.ver + ".zip")

            self.arch = zipfile.ZipFile(feed_gtfs, mode="r")
            self.arch_v = feed.ver
            self.arch_feed = feed
            self.arch_services = set()
            self.arch_shapes = set()
            self.arch_trips = set()

            # STOPS
            print("\033[1A\033[K" f"Merging version {self.arch_v}: stops.txt")
            self.load_stops()

            # ROUTES
            print("\033[1A\033[K" f"Merging version {self.arch_v}: routes.txt")
            self.load_routes()

            # CALENDARS
            print("\033[1A\033[K" f"Merging version {self.arch_v}: calendar_dates.txt")
            self.copy_calendars()

            # TRIPS
            print("\033[1A\033[K" f"Merging version {self.arch_v}: trips.txt")
            self.copy_trips()

            # TIMES
            print("\033[1A\033[K" f"Merging version {self.arch_v}: stop_times.txt")
            self.copy_times()

            # SHAPES
            print("\033[1A\033[K" f"Merging version {self.arch_v}: shapes.txt")
            self.copy_shapes()

            self.arch.close()

    @classmethod
    def create(cls, target="rzeszow.zip", remerge=False, reparse=False, pub_name="", pub_url=""):
        """
        Automatically creates a GTFS for Rzeszów from all current and future
        files avaiable at ZTM Rzeszów's WebDAV.

        :param str target: Target GTFS zip file name
        :param bool remerge: Force re-merging of the files, even if nothing
                             changed in the feeds/ directory
        :param bool reparse: Force recreation if each individual GTFS file
        """
        print("Listing required files")
        self = cls()
        self.list_xml_files()
        self.cleanup_xml_files()

        if reparse:
            print("\033[1A\033[K" "Clearing local files")
            _clear_dir("feeds")

        print("\033[1A\033[K" "Updating local files")
        self.sync_files(reparse)

        if self.changed is True or remerge:
            print("\033[1A\033[K" "Merging feeds")
            self.merge()

            print("\033[1A\033[K" "Outputing merged routes")
            self.create_routes()

            print("\033[1A\033[K" "Outputing merged stops")
            self.create_stops()

            print("\033[1A\033[K" "Creating static files")
            RzeszowGtfs.static_files(pub_name, pub_url, self.version, self.download_time)

            print("\033[1A\033[K" + "Compressing")
            RzeszowGtfs.compress(target)

        else:
            print("\033[1A\033[K" + "No new files found, no GTFS was created!")
            self.version = None

        return self.version


if __name__ == "__main__":
    argprs = argparse.ArgumentParser()

    argprs.add_argument(
        "--single-file", "-s", action="store", metavar="FILENAME_ON_RZESZOW_OPENDATA",
        help="parse only one file by the provided name. "
             "this file has to exist on otwartedane.erzeszow.pl"
    )

    argprs.add_argument(
        "--merge", "-m", action="store_true",
        required=False, dest="merge",
        help="automatically create and merge all future schedules"
    )

    argprs.add_argument(
        "--remerge", "-rm", action="store_true",
        required=False, dest="remerge",
        help="force merge of multi-day file (only valid with --merge)"
    )

    argprs.add_argument(
        "--reparse", "-rp", action="store_true",
        required=False, dest="reparse",
        help="force re-creation of individual GTFS files (only valid with --merge)"
    )

    argprs.add_argument(
        "-pn", "--publisher-name",
        required=False,
        metavar="NAME",
        help="value of feed_publisher_name (--publisher-url is also required to create feed_info)",
        default="",
    )

    argprs.add_argument(
        "-pu", "--publisher-url",
        required=False,
        metavar="URL",
        help="value of feed_publisher_url (--publisher-name is also required to create feed_info)",
        default="",
    )

    args = argprs.parse_args()

    if args.merge and args.single_file:
        argprs.error("--merge and --single-file are mutually exclusive!")

    elif args.single_file:
        print()
        RzeszowGtfs.parse(file_name=args.single_file, pub_name=args.publisher_name,
                          pub_url=args.publisher_url)

    elif args.merge:
        MultiRzeszow.create(remerge=args.remerge, reparse=args.reparse,
                            pub_name=args.publisher_name, pub_url=args.publisher_url)

    else:
        argprs.error("nothing to do! provide --merge or --single-file")
