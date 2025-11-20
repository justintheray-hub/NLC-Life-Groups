import os
import base64
from typing import List, Dict, Any, Tuple

import requests
from supabase import create_client, Client

"""
Sync Planning Center Online Groups -> Supabase `groups` table.

Env vars required (set via GitHub Actions secrets or local env):
- PCO_APP_ID
- PCO_SECRET
- SUPABASE_URL
- SUPABASE_SERVICE_KEY

Supabase table expected (public.groups):

create table if not exists public.groups (
  id                bigserial primary key,
  pco_group_id      text not null unique,
  name              text not null,
  description       text,
  campus            text,
  days_of_week      text[],
  time_of_day       text,
  stage_of_life     text,
  group_type        text,
  is_open           boolean default true,
  max_size          integer,
  current_size      integer,
  church_center_url text,
  tags              jsonb,
  updated_at        timestamptz default now()
);
"""

# --- Config from environment ---

PCO_APP_ID = os.environ["PCO_APP_ID"]
PCO_SECRET = os.environ["PCO_SECRET"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# NOTE: If your Pipedream script used a more specific endpoint (like a group_type),
# you can swap this BASE_URL to match that.
BASE_URL = "https://api.planningcenteronline.com/groups/v2/groups"


# --- Helpers ---

def basic_auth_header(app_id: str, secret: str) -> str:
    token = base64.b64encode(f"{app_id}:{secret}".encode("utf-8")).decode("utf-8")
    return f"Basic {token}"


def fetch_all_groups() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Fetch all groups from PCO with pagination.

    Returns (all_groups_data, all_included_resources).
    """
    headers = {
        "Authorization": basic_auth_header(PCO_APP_ID, PCO_SECRET),
        "Content-Type": "application/json",
    }

    all_data: List[Dict[str, Any]] = []
    all_included: List[Dict[str, Any]] = []

    url = BASE_URL
    params = {"per_page": 100, "include": "tags"}

    page = 0
    while url:
        page += 1
        print(f"Requesting page {page}: {url}")

        # Only send params on first request. `links.next` already has its own query string.
        if page == 1:
            resp = requests.get(url, headers=headers, params=params)
        else:
            resp = requests.get(url, headers=headers)

        resp.raise_for_status()
        json_data = resp.json()

        data = json_data.get("data", []) or []
        included = json_data.get("included", []) or []
        links = json_data.get("links", {}) or {}

        print(f"  Page {page} returned {len(data)} groups")
        print(f"  links: {links}")

        all_data.extend(data)
        all_included.extend(included)

        next_url = links.get("next")

        # Fallback in case some variants put cursor info elsewhere
        if not next_url:
            meta = json_data.get("meta", {}) or {}
            if meta:
                print(f"  meta: {meta}")
            next_url = meta.get("next") or meta.get("next_page_url")

        url = next_url

    print(f"Fetched {len(all_data)} groups total from PCO")
    print(f"Collected {len(all_included)} included resources (tags etc.)")
    return all_data, all_included


def build_tag_lookup(included: List[Dict[str, Any]]) -> Dict[str, str]:
    """Build a map of tag_id -> tag_name from `included` resources.

    This is intentionally permissive on the `type` field, since PCO may use
    different strings (e.g. "Tag", "tag", "group_tag"). Anything that has
    an `id` and `attributes.name` will be mapped.
    """
    tag_lookup: Dict[str, str] = {}

    for item in included:
        attrs = item.get("attributes", {}) or {}
        tag_id = item.get("id")
        name = attrs.get("name")
        if not tag_id or not name:
            continue

        t = (item.get("type") or "").lower()
        if "tag" in t:  # e.g. "tag", "Tag", "group_tag"
            tag_lookup[tag_id] = name

    print(f"Built tag lookup with {len(tag_lookup)} tags")
    return tag_lookup


def parse_tags_for_group(group: Dict[str, Any], tag_lookup: Dict[str, str]) -> Dict[str, Any]:
    """Extract structured fields + raw tag ids/names from a group.

    Recommended PCO tag naming convention (examples):
      Campus: Conway
      Stage: Young Adults
      Type: Bible Study
      Day: Monday
    """
    rel = group.get("relationships", {}) or {}
    tags_rel = rel.get("tags", {}) or {}
    tag_data = tags_rel.get("data", []) or []

    tag_ids: List[str] = []
    tag_names: List[str] = []

    for t in tag_data:
        tag_id = t.get("id")
        if not tag_id:
            continue
        tag_ids.append(tag_id)
        if tag_id in tag_lookup:
            tag_names.append(tag_lookup[tag_id])

    campus = None
    stage_of_life = None
    group_type = None
    days_of_week: List[str] = []

    # Only parse structured info if we have names
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
        "tag_ids": tag_ids,
        "tag_names": tag_names,
    }


def transform_group(group: Dict[str, Any], tag_lookup: Dict[str, str]) -> Dict[str, Any]:
    """Transform a raw PCO group into one Supabase `groups` row.

    Adjust attribute names here if your PCO account uses different fields
    (e.g., meeting time, capacity, enrollment, URL).
    """
    attrs = group.get("attributes", {}) or {}
    tag_info = parse_tags_for_group(group, tag_lookup)

    return {
        "pco_group_id": group.get("id"),
        "name": attrs.get("name"),
        "description": attrs.get("description"),
        "campus": tag_info["campus"],
        "days_of_week": tag_info["days_of_week"] or None,
        "time_of_day": attrs.get("meeting_time"),  # adjust if needed
        "stage_of_life": tag_info["stage_of_life"],
        "group_type": tag_info["group_type"],
        "is_open": not bool(attrs.get("archived_at")),
        "max_size": attrs.get("capacity"),        # adjust if needed
        "current_size": attrs.get("enrollment"),  # adjust if needed
        "church_center_url": attrs.get("url"),    # adjust if needed
        "tags": {
            "ids": tag_info["tag_ids"],
            "names": tag_info["tag_names"],
        },
    }


def clear_groups_table() -> None:
    """Delete all existing rows from public.groups before re-inserting."""
    print("Clearing existing groups from Supabase...")
    response = supabase.table("groups").delete().neq("pco_group_id", "").execute()
    error = getattr(response, "error", None)
    if error:
        print("Error clearing groups:", error)
        raise RuntimeError(error)
    print("Existing groups cleared.")


def sync() -> None:
    print("Starting sync from Planning Center to Supabase...")

    data, included = fetch_all_groups()
    tag_lookup = build_tag_lookup(included)

    rows = [transform_group(g, tag_lookup) for g in data]

    print(f"Prepared {len(rows)} rows to insert into Supabase")

    # Clear table first
    clear_groups_table()

    batch_size = 200
    for i in range(0, len(rows), batch_size):
        batch = rows[i: i + batch_size]
        print(f"Inserting batch {i // batch_size + 1} ({len(batch)} rows)...")

        response = (
            supabase.table("groups")
            .insert(batch)
            .execute()
        )

        error = getattr(response, "error", None)
        if error:
            print("Supabase error:", error)
            raise RuntimeError(error)
        else:
            print("Batch inserted successfully")

    print("Sync complete.")


if __name__ == "__main__":
    sync()
