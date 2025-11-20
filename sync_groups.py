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

# If your old Pipedream script used a more specific endpoint (like a group_type),
# you can swap this BASE_URL to match that.
BASE_URL = "https://api.planningcenteronline.com/groups/v2/groups"


# --- Helpers ---------------------------------------------------------------

def basic_auth_header(app_id: str, secret: str) -> str:
    token = base64.b64encode(f"{app_id}:{secret}".encode("utf-8")).decode("utf-8")
    return f"Basic {token}"


def fetch_all_groups() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Fetch all groups from PCO with pagination.

    Returns (all_groups_data, all_included_resources).
    We ignore `included` now since we're not using tags, but keep the
    return shape in case you want it later.
    """
    headers = {
        "Authorization": basic_auth_header(PCO_APP_ID, PCO_SECRET),
        "Content-Type": "application/json",
    }

    all_data: List[Dict[str, Any]] = []
    all_included: List[Dict[str, Any]] = []

    url = BASE_URL
    # We can drop include=tags since we don't need them anymore
    params = {"per_page": 100}

    page = 0
    while url:
        page += 1
        print(f"Requesting page {page}: {url}")

        # Only send params on first request. `links.next` has its own query string.
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

        # Fallback, just in case
        if not next_url:
            meta = json_data.get("meta", {}) or {}
            if meta:
                print(f"  meta: {meta}")
            next_url = meta.get("next") or meta.get("next_page_url")

        url = next_url

    print(f"Fetched {len(all_data)} groups total from PCO")
    return all_data, all_included


def transform_group(group: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a raw PCO group into one Supabase `groups` row using attributes only."""

    attrs = group.get("attributes", {}) or {}

    # Try a few likely attribute names for each field; if a key doesn't exist,
    # .get() will just return None, which is fine.
    name = attrs.get("name")
    description = attrs.get("description") or attrs.get("short_description")

    # Campus-ish
    campus = (
        attrs.get("campus_name")
        or attrs.get("campus")
        or attrs.get("location_name")
    )

    # Meeting day / days_of_week
    meeting_day = attrs.get("meeting_day") or attrs.get("meets_on")
    days_of_week = [meeting_day] if meeting_day else None

    # Time of day
    time_of_day = (
        attrs.get("meeting_time")
        or attrs.get("time")
        or attrs.get("starts_at")
    )

    # Stage of life
    stage_of_life = (
        attrs.get("life_stage")
        or attrs.get("group_lifestage")
        or attrs.get("age_range")
    )

    # Group type
    group_type = (
        attrs.get("group_type")
        or attrs.get("type")
        or attrs.get("category")
    )

    # Capacity / enrollment
    max_size = attrs.get("capacity") or attrs.get("max_participants")
    current_size = attrs.get("enrollment") or attrs.get("current_participants")

    # URLs
    church_center_url = (
        attrs.get("url")
        or attrs.get("web_url")
        or attrs.get("public_url")
    )

    # Open / archived
    is_open = not bool(attrs.get("archived_at"))

    return {
        "pco_group_id": group.get("id"),
        "name": name,
        "description": description,
        "campus": campus,
        "days_of_week": days_of_week,
        "time_of_day": time_of_day,
        "stage_of_life": stage_of_life,
        "group_type": group_type,
        "is_open": is_open,
        "max_size": max_size,
        "current_size": current_size,
        "church_center_url": church_center_url,
        # You said you don't need tags; we'll just store an empty object.
        "tags": {},
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

    data, _included = fetch_all_groups()

    rows = [transform_group(g) for g in data]

    print(f"Prepared {len(rows
