# Copyright 2020, 2024 Mikołaj Kuranowski
# SPDX-License-Identifier: MIT

import re
from argparse import ArgumentParser, Namespace
from datetime import datetime
from collections import defaultdict
from typing import Any, Sequence, cast

from impuls.multi_file import IntermediateFeed, IntermediateFeedProvider, MultiFile, prune_outdated_feeds
from impuls.model import Attribution, Date, FeedInfo
import impuls
import requests


class RzeszowFeedProvider(IntermediateFeedProvider[impuls.HTTPResource]):
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.verify = False

    def needed(self) -> list[IntermediateFeed[impuls.HTTPResource]]:
        with self.session.get("https://otwartedane.erzeszow.pl/v1/datasets/slug_full_view/?format=json&slug=rozklady-jazdy-gtfs") as r:
            r.raise_for_status()
            data = r.json()
        feeds = [self.feed_from_json_resource(i) for i in data["resources"]]
        prune_outdated_feeds(feeds, Date.today())
        return feeds

    def feed_from_json_resource(self, r: Any) -> IntermediateFeed[impuls.HTTPResource]:
        if m := re.search(r"od ([0-9]{2})\.([0-9]{2})\.([0-9]{4})", r["description"]):
            start_date = Date(int(m[3]), int(m[2]), int(m[1]))
        elif m := re.search(r"\[([0-9]{2})-([0-9]{2})-([0-9]{4})", r["name"]):
            start_date = Date(int(m[3]), int(m[2]), int(m[1]))
        else:
            raise ValueError(f"failed to extract start date from {r['name']=} {r['description']=}")

        return IntermediateFeed(
            resource=impuls.HTTPResource(requests.Request("GET", r["file"]), self.session),
            resource_name=f"gtfs_{start_date.isoformat()}.zip",
            version=start_date.isoformat(),
            start_date=start_date,
        )


class MergeRoutes(impuls.Task):
    def execute(self, r: impuls.TaskRuntime) -> None:
        short_name_to_ids = self.map_short_names_to_ids(r.db)
        with r.db.transaction():
            for short_name, ids in short_name_to_ids.items():
                self.merge_route(r.db, short_name, ids)

    @staticmethod
    def map_short_names_to_ids(db: impuls.DBConnection) -> defaultdict[str, list[str]]:
        m = defaultdict[str, list[str]](list)
        q = db.raw_execute("SELECT route_id, short_name FROM routes")
        for id, short_name in q:
            m[cast(str, short_name)].append(cast(str, id))
        return m

    @staticmethod
    def merge_route(db: impuls.DBConnection, short_name: str, old_ids: Sequence[str]) -> None:
        new_id = f"1_{short_name}"
        db.raw_execute(
            (
                "INSERT INTO routes (agency_id, route_id, short_name, long_name, type) "
                "VALUES ('1', ?, ?, '', 3)"
            ),
            (new_id, short_name),
        )
        db.raw_execute_many(
            "UPDATE trips SET route_id = ? WHERE route_id = ?",
            ((new_id, old_id) for old_id in old_ids),
        )
        db.raw_execute_many(
            "DELETE FROM routes WHERE route_id = ?",
            ((old_id,) for old_id in old_ids),
        )


class RzeszowGTFS(impuls.App):
    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument("-o", "--output", default="rzeszow.zip", help="output GTFS path")

    def prepare(
        self,
        args: Namespace,
        options: impuls.PipelineOptions,
    ) -> MultiFile[impuls.HTTPResource]:
        download_time = datetime.now()  # close enough
        return MultiFile[impuls.HTTPResource](
            options=options,
            intermediate_provider=RzeszowFeedProvider(),
            intermediate_pipeline_tasks_factory=lambda feed: [
                impuls.tasks.LoadGTFS(feed.resource_name),
                MergeRoutes(),
                impuls.tasks.ExecuteSQL(
                    task_name="SetRouteColor",
                    statement=(
                        "UPDATE ROUTES set color = iif(short_name LIKE 'N%', '000000', 'DD3300'),"
                        " text_color = 'FFFFFF'"
                    ),
                ),
                impuls.tasks.ExecuteSQL(
                    task_name="FlagRequestStopsInStopTimes",
                    statement=(
                        "WITH request_stops AS (SELECT stop_id FROM stops WHERE name LIKE '%nż') "
                        "UPDATE stop_times SET "
                        "  pickup_type = iif(stop_id IN request_stops, 3, 0),"
                        "  drop_off_type = iif(stop_id IN request_stops, 3, 0)"
                    )
                ),
                impuls.tasks.ExecuteSQL(
                    task_name="ClearStopCode",
                    statement="UPDATE stops SET code = ''",
                ),
                impuls.tasks.ExecuteSQL(
                    task_name="RemoveNzFromStopName",
                    statement=(
                        "UPDATE stops SET name = substr(name, 1, length(name)-3) "
                        "WHERE name LIKE '% nż'"
                    ),
                ),
                impuls.tasks.GenerateTripHeadsign(),
                impuls.tasks.ExecuteSQL(
                    task_name="RemoveStopIndicatorFromHeadsign",
                    statement=r"UPDATE trips SET headsign = re_sub('\s*\d+$', '', headsign)",
                ),
                impuls.tasks.ExecuteSQL(
                    task_name="RemovePetlaFromHeadsign",
                    statement=r"UPDATE trips SET headsign = re_sub('\s*pętla$', '', headsign)",
                ),
            ],
            final_pipeline_tasks_factory=lambda feeds: [
                impuls.tasks.AddEntity(
                    task_name="AddAttribution",
                    entity=Attribution(
                        id="1",
                        organization_name=(
                            "Data provided by: ZTM Rzeszów (retrieved "
                            f"{download_time.strftime('%Y-%m-%d %H:%M:%S')})"
                        ),
                        is_producer=False,
                        is_operator=True,
                        is_authority=True,
                        is_data_source=True,
                        url="https://otwartedane.erzeszow.pl/dataset/rozklady-jazdy-gtfs",
                    ),
                ),
                impuls.tasks.ExecuteSQL("RemoveFeedInfo", "DELETE FROM feed_info"),
                impuls.tasks.AddEntity(
                    task_name="AddFeedInfo",
                    entity=FeedInfo(
                        publisher_name="Mikołaj Kuranowski",
                        publisher_url="https://mkuran.pl/gtfs/",
                        lang="pl",
                        version="/".join(feed.version for feed in feeds),
                    ),
                ),
                impuls.tasks.SaveGTFS(
                    headers={
                        "agency": (
                            "agency_id",
                            "agency_name",
                            "agency_url",
                            "agency_timezone",
                            "agency_lang",
                            "agency_phone",
                        ),
                        "routes": (
                            "agency_id",
                            "route_id",
                            "route_short_name",
                            "route_long_name",
                            "route_type",
                            "route_color",
                            "route_text_color",
                        ),
                        "stops": (
                            "stop_id",
                            "stop_name",
                            "stop_lat",
                            "stop_lon",
                        ),
                        "calendar_dates": ("date", "service_id", "exception_type"),
                        "trips": (
                            "trip_id",
                            "route_id",
                            "service_id",
                            "trip_headsign",
                            "direction_id",
                            "block_id",
                            "shape_id",
                        ),
                        "shapes": ("shape_id", "shape_pt_sequence", "shape_pt_lat", "shape_pt_lon"),
                        "stop_times": (
                            "trip_id",
                            "stop_sequence",
                            "stop_id",
                            "arrival_time",
                            "departure_time",
                            "pickup_type",
                            "drop_off_type",
                        ),
                        "feed_info": (
                            "feed_publisher_name",
                            "feed_publisher_url",
                            "feed_lang",
                            "feed_version",
                        ),
                        "attributions": (
                            "attribution_id",
                            "organization_name",
                            "is_producer",
                            "is_operator",
                            "is_authority",
                            "is_data_source",
                            "attribution_url",
                        ),
                    },
                    target=args.output,
                )
            ],
            merge_separator="_",
        )


if __name__ == "__main__":
    RzeszowGTFS().run()
