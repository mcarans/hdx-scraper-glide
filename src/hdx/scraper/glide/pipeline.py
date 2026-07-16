#!/usr/bin/python
"""Glide scraper"""

import calendar
import json
import logging
from os.path import join

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)

_MAINTAINER = "196196be-6037-4488-8b71-d786adf4c081"
_OWNER_ORG = "ebcfe377-bad0-46d0-b68f-cca8e6b54e33"
_EXCLUDED_FIELDS = frozenset({"features", "id", "idsource", "time"})
_HEADERS = [
    "glidenumber",
    "number",
    "docid",
    "event",
    "event_name",
    "geocode",
    "location",
    "latitude",
    "longitude",
    "latitude2",
    "longitude2",
    "shape_type",
    "year",
    "month",
    "day",
    "time",
    "status",
    "killed",
    "injured",
    "homeless",
    "affected",
    "magnitude",
    "duration",
    "comments",
    "source",
    "id",
    "idsource",
    "features",
]


class Pipeline:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        today,
        folder: str,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._today = today
        self._folder = folder
        self._events = {}
        self._country_startdate = {}
        self._country_enddate = {}
        self._headers = None
        self._geojson_by_country = {}
        self._global_geojson = None

    def get_countriesdata(self) -> list[dict]:
        url = self._configuration["url"]
        json_data = self._retriever.download_json(url, "glide.json")
        glideset = json_data["glideset"]
        min_date = parse_date(f"{self._today.year - 2}-01-01")
        all_country_events = {}
        country_has_recent = set()

        for event in glideset:
            countryiso3 = event.get("geocode", "")
            if not countryiso3 or len(countryiso3) != 3:
                continue
            year = event["year"]
            month = event["month"]
            day = event["day"]
            try:
                event_date = parse_date(f"{year:04d}-{month:02d}-{day:02d}")
            except ValueError:
                day = max(day, 1)
                month = max(month, 1)
                _, last_day = calendar.monthrange(year, month)
                day = min(day, last_day)
                try:
                    event_date = parse_date(f"{year:04d}-{month:02d}-{day:02d}")
                except ValueError:
                    logger.warning(
                        f"Skipping event with invalid date: {year}-{month}-{day}"
                    )
                    continue
            clean_event = {k: v for k, v in event.items() if k not in _EXCLUDED_FIELDS}
            dict_of_lists_add(
                all_country_events, countryiso3, (event_date, clean_event)
            )
            if event_date >= min_date:
                country_has_recent.add(countryiso3)

        for countryiso3, dated_events in all_country_events.items():
            if countryiso3 not in country_has_recent:
                continue
            for event_date, clean_event in dated_events:
                existing_min = self._country_startdate.get(countryiso3)
                if not existing_min or event_date < existing_min:
                    if event_date.year > 1900:
                        self._country_startdate[countryiso3] = event_date
                existing_max = self._country_enddate.get(countryiso3)
                if not existing_max or event_date > existing_max:
                    self._country_enddate[countryiso3] = event_date
                dict_of_lists_add(self._events, countryiso3, clean_event)

        if not self._events:
            raise ValueError(f"No countries with events since {min_date}")

        self._headers = [h for h in _HEADERS if h not in _EXCLUDED_FIELDS]

        geojson_url = self._configuration["geojson_url"]
        self._global_geojson = self._retriever.download_json(
            geojson_url, "glide_global.geojson"
        )
        for iso3 in country_has_recent:
            self._geojson_by_country[iso3] = self._retriever.download_json(
                f"{geojson_url}?level1={iso3}", f"glide_{iso3.lower()}.geojson"
            )

        return [{"iso3": iso3} for iso3 in sorted(country_has_recent)]

    def _add_geojson_resource(self, dataset, filename, geojson_data, description):
        filepath = join(self._folder, filename)
        with open(filepath, "w") as f:
            json.dump(geojson_data, f)
        resource = Resource(
            {"name": filename, "description": description, "format": "geojson"}
        )
        resource.set_file_to_upload(filepath)
        dataset.add_update_resource(resource)

    def generate_global_dataset(self) -> Dataset | None:
        if not self._events:
            return None

        dataset = Dataset(
            {"name": "global-glide-events", "title": "Global - GLIDE Disaster Events"}
        )
        dataset.add_other_location("world")
        dataset.set_maintainer(_MAINTAINER)
        dataset.set_organization(_OWNER_ORG)
        dataset.set_expected_update_frequency("As needed")
        dataset.set_time_period(
            min(self._country_startdate.values()),
            max(self._country_enddate.values()),
        )
        dataset.set_subnational(True)

        event_types = self._configuration["event_types"]
        event_tags = self._configuration["event_tags"]
        tags = set()
        all_rows = []
        for countryiso in sorted(self._events):
            for row in self._events[countryiso]:
                event_code = row.get("event", "")
                row["event_name"] = event_types.get(event_code, "")
                tags.update(event_tags.get(event_code, []))
                all_rows.append(row)
        dataset.add_tags(sorted(tags))

        filename = "glide_events_global.csv"
        resourcedata = {
            "name": filename,
            "description": "GLIDE disaster events for all countries",
        }
        dataset.generate_resource(
            self._folder,
            filename,
            all_rows,
            resourcedata,
            headers=self._headers,
            no_empty=False,
        )
        if self._global_geojson:
            self._add_geojson_resource(
                dataset,
                "glide_events_global.geojson",
                self._global_geojson,
                "GLIDE disaster events GeoJSON for all countries",
            )
        return dataset

    def generate_dataset_and_showcase(self, countryiso: str) -> tuple:
        countryname = Country.get_country_name_from_iso3(countryiso)
        if not countryname:
            logger.warning(f"No country name for {countryiso}, skipping")
            return None, None, False

        countryiso_lower = countryiso.lower()
        name = f"{countryiso_lower}-glide-events"
        title = f"{countryname} - GLIDE Disaster Events"
        dataset = Dataset({"name": name, "title": title})
        try:
            dataset.add_country_location(countryiso)
        except HDXError:
            logger.error(f"Couldn't find country {countryiso}, skipping")
            return None, None, False

        if countryiso not in self._events:
            return dataset, None, False

        dataset.set_maintainer(_MAINTAINER)
        dataset.set_organization(_OWNER_ORG)
        dataset.set_expected_update_frequency("As needed")
        dataset.set_time_period(
            self._country_startdate[countryiso],
            self._country_enddate[countryiso],
        )
        dataset.set_subnational(True)

        rows = self._events[countryiso]
        event_types = self._configuration["event_types"]
        event_tags = self._configuration["event_tags"]
        tags = set()
        for row in rows:
            event_code = row.get("event", "")
            row["event_name"] = event_types.get(event_code, "")
            tags.update(event_tags.get(event_code, []))
        dataset.add_tags(sorted(tags))

        filename = f"{countryiso_lower}_glide_events.csv"
        resourcedata = {
            "name": filename,
            "description": f"GLIDE disaster events for {countryname}",
        }
        dataset.generate_resource(
            self._folder,
            filename,
            rows,
            resourcedata,
            headers=self._headers,
            no_empty=False,
        )
        if countryiso in self._geojson_by_country:
            self._add_geojson_resource(
                dataset,
                f"{countryiso_lower}_glide_events.geojson",
                self._geojson_by_country[countryiso],
                f"GLIDE disaster events GeoJSON for {countryname}",
            )
        return dataset, None, True
