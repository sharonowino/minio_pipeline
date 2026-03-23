import os
import requests
import pandas as pd
from google.transit import gtfs_realtime_pb2
from datetime import datetime
import io
from minio import Minio

# GTFS Realtime URLs
VEHICLE_URL = os.getenv("GTFS_VEHICLE_URL")
TRIP_UPDATES_URL = os.getenv("GTFS_TRIP_UPDATES_URL")
SERVICE_ALERTS_URL = os.getenv("GTFS_SERVICE_ALERTS_URL")

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "gtfs-data")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"


def get_minio_client():
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise ValueError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables are required")
    
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )


def ensure_bucket_exists(client, bucket_name):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def fetch_vehicle_positions():
    """
    Fetch GTFS Realtime VehiclePosition data.
    Schema: https://gtfs.org/documentation/realtime/proto/
    """
    if not VEHICLE_URL:
        raise ValueError("GTFS_VEHICLE_URL environment variable is required")

    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(VEHICLE_URL)
    feed.ParseFromString(response.content)

    rows = []

    for entity in feed.entity:
        if entity.HasField("vehicle"):
            v = entity.vehicle

            # TripDescriptor
            trip = v.trip if v.HasField("trip") else None

            # VehicleDescriptor
            vehicle = v.vehicle if v.HasField("vehicle") else None

            # Position
            pos = v.position if v.HasField("position") else None

            rows.append({
                # Entity
                "entity_id": entity.id,

                # TripDescriptor
                "trip_id": trip.trip_id if trip and trip.trip_id else None,
                "route_id": trip.route_id if trip and trip.route_id else None,
                "direction_id": trip.direction_id if trip and trip.HasField("direction_id") else None,
                "start_time": trip.start_time if trip and trip.start_time else None,
                "start_date": trip.start_date if trip and trip.start_date else None,
                "schedule_relationship": trip.schedule_relationship if trip and trip.HasField("schedule_relationship") else None,

                # VehicleDescriptor
                "vehicle_id": vehicle.id if vehicle and vehicle.id else None,
                "vehicle_label": vehicle.label if vehicle and vehicle.label else None,
                "license_plate": vehicle.license_plate if vehicle and vehicle.license_plate else None,
                "wheelchair_accessible": vehicle.wheelchair_accessible if vehicle and vehicle.HasField("wheelchair_accessible") else None,

                # Position
                "latitude": pos.latitude if pos else None,
                "longitude": pos.longitude if pos else None,
                "bearing": pos.bearing if pos and pos.HasField("bearing") else None,
                "odometer": pos.odometer if pos and pos.HasField("odometer") else None,
                "speed": pos.speed if pos and pos.HasField("speed") else None,

                # VehiclePosition fields
                "current_stop_sequence": v.current_stop_sequence if v.HasField("current_stop_sequence") else None,
                "stop_id": v.stop_id if v.stop_id else None,
                "current_status": v.current_status if v.HasField("current_status") else None,
                "timestamp": v.timestamp if v.HasField("timestamp") else None,
                "congestion_level": v.congestion_level if v.HasField("congestion_level") else None,
                "occupancy_status": v.occupancy_status if v.HasField("occupancy_status") else None,
                "occupancy_percentage": v.occupancy_percentage if v.HasField("occupancy_percentage") else None,

                # Multi-carriage details (flattened as JSON string)
                "multi_carriage_details": str([
                    {
                        "id": c.id,
                        "label": c.label,
                        "occupancy_status": c.occupancy_status,
                        "occupancy_percentage": c.occupancy_percentage,
                        "carriage_sequence": c.carriage_sequence
                    } for c in v.multi_carriage_details
                ]) if v.multi_carriage_details else None,

                "retrieved_at": datetime.utcnow()
            })

    return pd.DataFrame(rows)


def fetch_trip_updates():
    """
    Fetch GTFS Realtime TripUpdate data.
    Schema: https://gtfs.org/documentation/realtime/proto/
    """
    if not TRIP_UPDATES_URL:
        raise ValueError("GTFS_TRIP_UPDATES_URL environment variable is required")

    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(TRIP_UPDATES_URL)
    feed.ParseFromString(response.content)

    rows = []

    for entity in feed.entity:
        if entity.HasField("trip_update"):
            tu = entity.trip_update
            trip = tu.trip
            vehicle = tu.vehicle if tu.HasField("vehicle") else None

            for stu in tu.stop_time_update:
                # StopTimeEvent - Arrival
                arrival = stu.arrival if stu.HasField("arrival") else None
                # StopTimeEvent - Departure
                departure = stu.departure if stu.HasField("departure") else None

                rows.append({
                    # Entity
                    "entity_id": entity.id,

                    # TripDescriptor
                    "trip_id": trip.trip_id if trip.trip_id else None,
                    "route_id": trip.route_id if trip.route_id else None,
                    "direction_id": trip.direction_id if trip.HasField("direction_id") else None,
                    "start_time": trip.start_time if trip.start_time else None,
                    "start_date": trip.start_date if trip.start_date else None,
                    "schedule_relationship": trip.schedule_relationship if trip.HasField("schedule_relationship") else None,

                    # VehicleDescriptor
                    "vehicle_id": vehicle.id if vehicle and vehicle.id else None,
                    "vehicle_label": vehicle.label if vehicle and vehicle.label else None,
                    "license_plate": vehicle.license_plate if vehicle and vehicle.license_plate else None,

                    # TripUpdate fields
                    "trip_update_timestamp": tu.timestamp if tu.HasField("timestamp") else None,
                    "delay": tu.delay if tu.HasField("delay") else None,

                    # TripProperties
                    "trip_properties_trip_id": tu.trip_properties.trip_id if tu.HasField("trip_properties") and tu.trip_properties.trip_id else None,
                    "trip_properties_start_date": tu.trip_properties.start_date if tu.HasField("trip_properties") and tu.trip_properties.start_date else None,
                    "trip_properties_start_time": tu.trip_properties.start_time if tu.HasField("trip_properties") and tu.trip_properties.start_time else None,
                    "trip_properties_shape_id": tu.trip_properties.shape_id if tu.HasField("trip_properties") and tu.trip_properties.shape_id else None,

                    # StopTimeUpdate
                    "stop_sequence": stu.stop_sequence if stu.HasField("stop_sequence") else None,
                    "stop_id": stu.stop_id if stu.stop_id else None,
                    "stop_time_schedule_relationship": stu.schedule_relationship if stu.HasField("schedule_relationship") else None,

                    # Arrival StopTimeEvent
                    "arrival_delay": arrival.delay if arrival and arrival.HasField("delay") else None,
                    "arrival_time": arrival.time if arrival and arrival.HasField("time") else None,
                    "arrival_uncertainty": arrival.uncertainty if arrival and arrival.HasField("uncertainty") else None,

                    # Departure StopTimeEvent
                    "departure_delay": departure.delay if departure and departure.HasField("delay") else None,
                    "departure_time": departure.time if departure and departure.HasField("time") else None,
                    "departure_uncertainty": departure.uncertainty if departure and departure.HasField("uncertainty") else None,

                    # StopTimeProperties
                    "assigned_stop_id": stu.stop_time_properties.assigned_stop_id if stu.HasField("stop_time_properties") and stu.stop_time_properties.assigned_stop_id else None,

                    # Departure/Arrival Occupancy
                    "departure_occupancy_status": stu.departure_occupancy_status if stu.HasField("departure_occupancy_status") else None,

                    "retrieved_at": datetime.utcnow()
                })

    return pd.DataFrame(rows)


def fetch_service_alerts():
    """
    Fetch GTFS Realtime Alert data.
    Schema: https://gtfs.org/documentation/realtime/proto/
    """
    if not SERVICE_ALERTS_URL:
        raise ValueError("GTFS_SERVICE_ALERTS_URL environment variable is required")

    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(SERVICE_ALERTS_URL)
    feed.ParseFromString(response.content)

    rows = []

    for entity in feed.entity:
        if entity.HasField("alert"):
            alert = entity.alert

            # Active periods
            active_periods = [
                {
                    "start": period.start if period.HasField("start") else None,
                    "end": period.end if period.HasField("end") else None
                } for period in alert.active_period
            ]

            # Informed entities (EntitySelector)
            informed_entities = []
            for ie in alert.informed_entity:
                informed_entities.append({
                    "agency_id": ie.agency_id if ie.agency_id else None,
                    "route_id": ie.route_id if ie.route_id else None,
                    "route_type": ie.route_type if ie.HasField("route_type") else None,
                    "direction_id": ie.direction_id if ie.HasField("direction_id") else None,
                    "stop_id": ie.stop_id if ie.stop_id else None,
                    "trip_id": ie.trip.trip_id if ie.HasField("trip") and ie.trip.trip_id else None,
                    "trip_route_id": ie.trip.route_id if ie.HasField("trip") and ie.trip.route_id else None,
                    "trip_direction_id": ie.trip.direction_id if ie.HasField("trip") and ie.trip.HasField("direction_id") else None,
                    "trip_start_time": ie.trip.start_time if ie.HasField("trip") and ie.trip.start_time else None,
                    "trip_start_date": ie.trip.start_date if ie.HasField("trip") and ie.trip.start_date else None,
                })

            # TranslatedString - extract all translations
            def extract_translations(translated_string):
                return [
                    {
                        "text": t.text,
                        "language": t.language if t.language else None
                    } for t in translated_string.translation
                ] if translated_string.translation else None

            # TranslatedImage
            def extract_images(translated_image):
                return [
                    {
                        "url": img.localized_image[0].url if img.localized_image else None,
                        "media_type": img.localized_image[0].media_type if img.localized_image else None,
                        "language": img.localized_image[0].language if img.localized_image else None
                    } for img in translated_image
                ] if translated_image else None

            rows.append({
                # Entity
                "entity_id": entity.id,

                # Alert fields
                "cause": alert.cause if alert.HasField("cause") else None,
                "effect": alert.effect if alert.HasField("effect") else None,
                "severity_level": alert.severity_level if alert.HasField("severity_level") else None,

                # Active periods
                "active_periods": str(active_periods) if active_periods else None,

                # Informed entities
                "informed_entities": str(informed_entities) if informed_entities else None,

                # TranslatedString fields
                "header_text": str(extract_translations(alert.header_text)),
                "description_text": str(extract_translations(alert.description_text)),
                "url": str(extract_translations(alert.url)) if alert.url.translation else None,
                "tts_header_text": str(extract_translations(alert.tts_header_text)) if alert.tts_header_text.translation else None,
                "tts_description_text": str(extract_translations(alert.tts_description_text)) if alert.tts_description_text.translation else None,

                # TranslatedImage
                "image": str(extract_images(alert.image.localized_image)) if alert.image.localized_image else None,
                "image_alternative_text": str(extract_translations(alert.image_alternative_text)) if alert.image_alternative_text.translation else None,

                # Cause/Effect detail
                "cause_detail": str(extract_translations(alert.cause_detail)) if alert.cause_detail.translation else None,
                "effect_detail": str(extract_translations(alert.effect_detail)) if alert.effect_detail.translation else None,

                "retrieved_at": datetime.utcnow()
            })

    return pd.DataFrame(rows)


def save_to_minio(df, client, bucket_name, data_type):
    """Save DataFrame as Parquet to MinIO."""
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    object_name = f"{data_type}/{timestamp}.parquet"

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    client.put_object(
        bucket_name,
        object_name,
        buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )

    print(f"Uploaded {object_name} to MinIO bucket '{bucket_name}'")


if __name__ == "__main__":
    client = get_minio_client()
    ensure_bucket_exists(client, MINIO_BUCKET)

    # Fetch and store vehicle positions
    vehicle_df = fetch_vehicle_positions()
    if len(vehicle_df) > 0:
        save_to_minio(vehicle_df, client, MINIO_BUCKET, "vehicle_positions")

    # Fetch and store trip updates
    trip_updates_df = fetch_trip_updates()
    if len(trip_updates_df) > 0:
        save_to_minio(trip_updates_df, client, MINIO_BUCKET, "trip_updates")

    # Fetch and store service alerts
    alerts_df = fetch_service_alerts()
    if len(alerts_df) > 0:
        save_to_minio(alerts_df, client, MINIO_BUCKET, "service_alerts")
