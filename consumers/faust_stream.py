"""Defines trends calculations for stations"""
from dataclasses import dataclass
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("com.udacity.topics.connect-pgsql.stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1, key_type=int, value_type=TransformedStation)
table = app.Table(
   "com.udacity.tables.transformed_stations",
   default=None,
   partitions=1,
   changelog_topic=out_topic,
)

def color_to_line(station):
    """Transform column value to color name"""
    color = "n/a"
    if station.red:
        color = "red"
    elif station.green:
        color = "green"
    elif station.blue:
        color = "blue"
    else:
        logger.warn(f"Invalid line/color provided for station: {station.station_name}")

    return color


@app.agent(topic)
async def transform_stations(stations):
    async for station in stations:
        transformed_station = TransformedStation(station_id=station.station_id, station_name=station.station_name, order=station.order, line=color_to_line(station))
        table[transformed_station.station_id] = transformed_station


if __name__ == "__main__":
    app.main()
