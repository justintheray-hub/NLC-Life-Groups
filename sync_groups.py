import os
import base64
import requests
from typing import List, Dict, Any
from supabase import create_client, Client

# Environment variables provided by GitHub Actions secrets
PCO_APP_ID = os.environ["PCO_APP_ID"]
PCO_SECRET = os.environ["PCO_SECRET"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

BASE_URL = "https://api.planningcenteronline.com/groups/v2/groups"


def basic_auth_header(app_id: str, secret: str) -> str:
    token = base64.b64encode(f"{app_id}:{secret}".encode("utf-8")).decode("utf-8")
    return f"Basic {token}"


def fetch_all_groups() -> tuple[list[dict], list[dict]]:
    """
    Fetch all groups from Planning Center, including tags, handling pagination.
    Returns (data, included).
    """
    headers = {
        "Authorization": basic_auth_header(PCO_APP_ID, PCO_SECRET),
        "Content-Type": "application/json",
    }

    url = f"{BASE_URL}?include=tags&per_page=100"
    all_data: List[Dict[str, Any]] = []
    included: List[Dict[str, Any]] = []

    while url:
        print(f"Requesting: {url}")
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        json_data = resp.json()

        all_data.extend(json_data.get("data", []))
        included.extend(json_data.get("included", []))

        links = json_data.get("links", {}) or {}
        url = links.get("next")

    print(f"Fetched {len(all_data)} groups from PCO")
    return all_data, included


def build_tag_lookup(included: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Build a map of tag id -> tag name from included resources.
    """
    tag_lookup: Dict[str, str] = {}
    for item in included:
        t = item.get("type")
        if t in ("Tag", "tag"):  # handle either capitalization
            tag_id = item.get("id")
            attrs = item.get("attributes", {}) or {}
            name = attrs.get("name")
            if tag_id and name:
                tag_lookup[tag_id] = name
    print(f"Built tag lookup with {len(tag_lookup)} tags")
    return tag_lookup


def parse_tags_for_group(group: Dict[str, Any], tag_lookup: Dict[str, str]) -> Dict[str, Any]:
    """
    Given a group and tag lookup, extract structured fields based on tag naming convention.
    Convention examples in PCO:
      Campus: Conway
      Stage: Young Adults
      Type: Bible Study
      Day: Monday
    """
    rel = group.get("relationships", {}) or {}
    tags_rel = rel.get("tags", {}) or {}
    tag_data = tags_rel.get("data", []) or []

    tag_names: List[str] = []
    for t in tag_data:
        tag_id = t.get("id")
        if tag_id in tag_lookup:
            tag_names.append(tag_lookup[tag_id])

    campus = None
    stage_of_life = None
    group_type = None
    days_of_week: List[str] = []

    for name in tag_names:
        if name.startswith("Campus:"):
            campus = name.split(":", 1)[1].strip()
        elif name.startswith("Stage:"):
            stage_of_life = name.split(":", 1)[1].strip()
        elif name.startswith("Type:"):
            group_type = name.split(":", 1)[1].strip()
        elif name.startswith("Day:"):
            day = name.split(":", 1)[1].strip()
            days_of_week.append(day)

    return {
        "campus": campus,
        "stage_of_life": stage_of_life,
        "group_type": group_type,
        "days_of_week": days_of_week,
        "tag_names": tag_names,
    }


def transform_group(group: Dict[str, Any], tag_lookup: Dict[str, str]) -> Dict[str, Any]:
    """
    Transform a raw PCO group into the shape of our Supabase 'groups' table.
    """
    attrs = group.get("attributes", {}) or {}
    tag_info = parse_tags_for_group(group, tag_lookup)

    return {
        "pco_group_id": group.get("id"),
        "name": attrs.get("name"),
        "description": attrs.get("description"),
        "campus": tag_info["campus"],
        "days_of_week": tag_info["days_of_week"] or None,
        "time_of_day": attrs.get("meeting_time"),
        "stage_of_life": tag_info["stage_of_life"],
        "group_type": tag_info["group_type"],
        # You may need to adjust this based on actual PCO attributes
        "is_open": not bool(attrs.get("archived_at")),
        "max_size": attrs.get("capacity"),
        "current_size": attrs.get("enrollment"),
        "church_center_url": attrs.get("url"),
        "tags": tag_info["tag_names"],
    }


def sync() -> None:
    print("Starting sync from Planning Center to Supabase...")
    data, included = fetch_all_groups()
    tag_lookup = build_tag_lookup(included)

    rows = [transform_group(g, tag_lookup) for g in data]

    print(f"Prepared {len(rows)} rows to upsert into Supabase")

    # Upsert in batches so we don't send a giant payload
    batch_size = 200
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        print(f"Upserting batch {i // batch_size + 1} ({len(batch)} rows)...")
        response = (
            supabase.table("groups")
            .upsert(batch, on_conflict="pco_group_id")
            .execute()
        )
        # supabase-py Response has .data and .error
        if hasattr(response, "error") and response.error:
            print("Supabase error:", response.error)
            raise RuntimeError(response.error)
        else:
            print("Batch upserted successfully")

    print("Sync complete.")


if __name__ == "__main__":
    sync()
