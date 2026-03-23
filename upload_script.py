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


def safe_get_field(obj, field_name, default=None):
    """Safely get a field from a protobuf message, handling missing fields."""
    if obj is None:
        return default
    if not hasattr(obj, field_name):
        return default
    try:
        if obj.HasField(field_name):
            return getattr(obj, field_name)
    except ValueError:
        # Field is not a singular field (might be repeated or not exist)
        value = getattr(obj, field_name, default)
        return value if value else default
    return default


def safe_get_attr(obj, attr_name, default=None):
    """Safely get an attribute value from a protobuf message."""
    if obj is None:
        return default
    if not hasattr(obj, attr_name):
        return default
    return getattr(obj, attr_name, default) or default


def get_minio_client():
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise ValueError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables are required")

    if not MINIO_ENDPOINT:
        raise ValueError("MINIO_ENDPOINT environment variable is required")

    endpoint = MINIO_ENDPOINT
    if endpoint.startswith(("http://", "https://")):
        raise ValueError(
            f"MINIO_ENDPOINT should not include scheme (http/https). "
            f"Got: {endpoint}. Use format: hostname:port"
        )
    if "/" in endpoint:
        raise ValueError(
            f"MINIO_ENDPOINT should not include a path. "
            f"Got: {endpoint}. Use format: hostname:port"
        )

    return Minio(
        endpoint,
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
            trip = safe_get_field(v, "trip")

            # VehicleDescriptor
            vehicle = safe_get_field(v, "vehicle")

            # Position
            pos = safe_get_field(v, "position")

            rows.append({
                # Entity
                "entity_id": entity.id,

                # TripDescriptor
                "trip_id": safe_get_attr(trip, "trip_id"),
                "route_id": safe_get_attr(trip, "route_id"),
                "direction_id": safe_get_field(trip, "direction_id"),
                "start_time": safe_get_attr(trip, "start_time"),
                "start_date": safe_get_attr(trip, "start_date"),
                "schedule_relationship": safe_get_field(trip, "schedule_relationship"),

                # VehicleDescriptor
                "vehicle_id": safe_get_attr(vehicle, "id"),
                "vehicle_label": safe_get_attr(vehicle, "label"),
                "license_plate": safe_get_attr(vehicle, "license_plate"),
                "wheelchair_accessible": safe_get_field(vehicle, "wheelchair_accessible"),

                # Position
                "latitude": pos.latitude if pos else None,
                "longitude": pos.longitude if pos else None,
                "bearing": safe_get_field(pos, "bearing"),
                "odometer": safe_get_field(pos, "odometer"),
                "speed": safe_get_field(pos, "speed"),

                # VehiclePosition fields
                "current_stop_sequence": safe_get_field(v, "current_stop_sequence"),
                "stop_id": safe_get_attr(v, "stop_id"),
                "current_status": safe_get_field(v, "current_status"),
                "timestamp": safe_get_field(v, "timestamp"),
                "congestion_level": safe_get_field(v, "congestion_level"),
                "occupancy_status": safe_get_field(v, "occupancy_status"),
                "occupancy_percentage": safe_get_field(v, "occupancy_percentage"),

                # Multi-carriage details (newer field)
                "multi_carriage_details": str([
                    {
                        "id": safe_get_attr(c, "id"),
                        "label": safe_get_attr(c, "label"),
                        "occupancy_status": safe_get_field(c, "occupancy_status"),
                        "occupancy_percentage": safe_get_field(c, "occupancy_percentage"),
                        "carriage_sequence": safe_get_field(c, "carriage_sequence")
                    } for c in v.multi_carriage_details
                ]) if hasattr(v, "multi_carriage_details") and v.multi_carriage_details else None,

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
            vehicle = safe_get_field(tu, "vehicle")

            # TripProperties (newer field)
            trip_properties = safe_get_field(tu, "trip_properties")

            for stu in tu.stop_time_update:
                # StopTimeEvent - Arrival
                arrival = safe_get_field(stu, "arrival")
                # StopTimeEvent - Departure
                departure = safe_get_field(stu, "departure")
                # StopTimeProperties (newer field)
                stop_time_properties = safe_get_field(stu, "stop_time_properties")

                rows.append({
                    # Entity
                    "entity_id": entity.id,

                    # TripDescriptor
                    "trip_id": safe_get_attr(trip, "trip_id"),
                    "route_id": safe_get_attr(trip, "route_id"),
                    "direction_id": safe_get_field(trip, "direction_id"),
                    "start_time": safe_get_attr(trip, "start_time"),
                    "start_date": safe_get_attr(trip, "start_date"),
                    "schedule_relationship": safe_get_field(trip, "schedule_relationship"),

                    # VehicleDescriptor
                    "vehicle_id": safe_get_attr(vehicle, "id"),
                    "vehicle_label": safe_get_attr(vehicle, "label"),
                    "license_plate": safe_get_attr(vehicle, "license_plate"),

                    # TripUpdate fields
                    "trip_update_timestamp": safe_get_field(tu, "timestamp"),
                    "delay": safe_get_field(tu, "delay"),

                    # TripProperties (newer fields)
                    "trip_properties_trip_id": safe_get_attr(trip_properties, "trip_id"),
                    "trip_properties_start_date": safe_get_attr(trip_properties, "start_date"),
                    "trip_properties_start_time": safe_get_attr(trip_properties, "start_time"),
                    "trip_properties_shape_id": safe_get_attr(trip_properties, "shape_id"),

                    # StopTimeUpdate
                    "stop_sequence": safe_get_field(stu, "stop_sequence"),
                    "stop_id": safe_get_attr(stu, "stop_id"),
                    "stop_time_schedule_relationship": safe_get_field(stu, "schedule_relationship"),

                    # Arrival StopTimeEvent
                    "arrival_delay": safe_get_field(arrival, "delay"),
                    "arrival_time": safe_get_field(arrival, "time"),
                    "arrival_uncertainty": safe_get_field(arrival, "uncertainty"),

                    # Departure StopTimeEvent
                    "departure_delay": safe_get_field(departure, "delay"),
                    "departure_time": safe_get_field(departure, "time"),
                    "departure_uncertainty": safe_get_field(departure, "uncertainty"),

                    # StopTimeProperties (newer field)
                    "assigned_stop_id": safe_get_attr(stop_time_properties, "assigned_stop_id"),

                    # Departure/Arrival Occupancy (newer field)
                    "departure_occupancy_status": safe_get_field(stu, "departure_occupancy_status"),

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
                    "start": safe_get_field(period, "start"),
                    "end": safe_get_field(period, "end")
                } for period in alert.active_period
            ] if hasattr(alert, "active_period") else []

            # Informed entities (EntitySelector)
            informed_entities = []
            if hasattr(alert, "informed_entity"):
                for ie in alert.informed_entity:
                    ie_trip = safe_get_field(ie, "trip")
                    informed_entities.append({
                        "agency_id": safe_get_attr(ie, "agency_id"),
                        "route_id": safe_get_attr(ie, "route_id"),
                        "route_type": safe_get_field(ie, "route_type"),
                        "direction_id": safe_get_field(ie, "direction_id"),
                        "stop_id": safe_get_attr(ie, "stop_id"),
                        "trip_id": safe_get_attr(ie_trip, "trip_id"),
                        "trip_route_id": safe_get_attr(ie_trip, "route_id"),
                        "trip_direction_id": safe_get_field(ie_trip, "direction_id"),
                        "trip_start_time": safe_get_attr(ie_trip, "start_time"),
                        "trip_start_date": safe_get_attr(ie_trip, "start_date"),
                    })

            # TranslatedString - extract all translations
            def extract_translations(translated_string):
                if not translated_string or not hasattr(translated_string, "translation"):
                    return None
                translations = [
                    {
                        "text": t.text,
                        "language": safe_get_attr(t, "language")
                    } for t in translated_string.translation
                ]
                return translations if translations else None

            # TranslatedImage (newer field)
            def extract_images(image_field):
                if not image_field or not hasattr(image_field, "localized_image"):
                    return None
                if not image_field.localized_image:
                    return None
                images = []
                for img in image_field.localized_image:
                    images.append({
                        "url": safe_get_attr(img, "url"),
                        "media_type": safe_get_attr(img, "media_type"),
                        "language": safe_get_attr(img, "language")
                    })
                return images if images else None

            # Safely get translated string fields
            header_text = safe_get_field(alert, "header_text")
            description_text = safe_get_field(alert, "description_text")
            url_field = safe_get_field(alert, "url")
            tts_header_text = safe_get_field(alert, "tts_header_text")
            tts_description_text = safe_get_field(alert, "tts_description_text")
            image_field = safe_get_field(alert, "image")
            image_alternative_text = safe_get_field(alert, "image_alternative_text")
            cause_detail = safe_get_field(alert, "cause_detail")
            effect_detail = safe_get_field(alert, "effect_detail")

            rows.append({
                # Entity
                "entity_id": entity.id,

                # Alert fields
                "cause": safe_get_field(alert, "cause"),
                "effect": safe_get_field(alert, "effect"),
                "severity_level": safe_get_field(alert, "severity_level"),

                # Active periods
                "active_periods": str(active_periods) if active_periods else None,

                # Informed entities
                "informed_entities": str(informed_entities) if informed_entities else None,

                # TranslatedString fields
                "header_text": str(extract_translations(header_text)) if extract_translations(header_text) else None,
                "description_text": str(extract_translations(description_text)) if extract_translations(description_text) else None,
                "url": str(extract_translations(url_field)) if extract_translations(url_field) else None,
                "tts_header_text": str(extract_translations(tts_header_text)) if extract_translations(tts_header_text) else None,
                "tts_description_text": str(extract_translations(tts_description_text)) if extract_translations(tts_description_text) else None,

                # TranslatedImage (newer fields)
                "image": str(extract_images(image_field)) if extract_images(image_field) else None,
                "image_alternative_text": str(extract_translations(image_alternative_text)) if extract_translations(image_alternative_text) else None,

                # Cause/Effect detail (newer fields)
                "cause_detail": str(extract_translations(cause_detail)) if extract_translations(cause_detail) else None,
                "effect_detail": str(extract_translations(effect_detail)) if extract_translations(effect_detail) else None,

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
