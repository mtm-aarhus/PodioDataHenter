from __future__ import annotations

import html
import json
import re
import unicodedata
from datetime import datetime, timezone, timedelta
from typing import Any

import pandas as pd
import requests
import os
import openpyxl
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

def process(orchestrator_connection: OrchestratorConnection) -> None:


    # ============================================================
    # Konfiguration
    # ============================================================

    API_BASE = orchestrator_connection.get_constant('PodioApiUrl').value

    client = orchestrator_connection.get_credential('PodioClient')
    CLIENT_ID = client.username
    CLIENT_SECRET = client.password

    anlæg = orchestrator_connection.get_credential('PodioAppAnlæg')
    mobilitet = orchestrator_connection.get_credential('PodioAppMobilitet')

    APP_ID_mobilitet = mobilitet.username
    APP_ID_anlæg = anlæg.username
    APP_TOKEN_mobilitet = mobilitet.password
    APP_TOKEN_anlæg = anlæg.password

    TARGET_RELATION_FIELDS: set[str] | None = {
        "Anlægsprojekter i indsatsen",
        "anlaegsprojekter-i-indsatsen",
    }

    HYDRATE_FULL_ITEMS = True
    HTTP_TIMEOUT = 600

    # Navn på de konstanter i OpenOrchestrator der gemmer token-cache.
    # Opret dem manuelt første gang med værdien "{}" eller lad koden håndtere det.
    TOKEN_CONSTANT_MOBILITET = "PodioToken_Mobilitet"
    TOKEN_CONSTANT_ANLÆG = "PodioToken_Anlæg"

    # Antal sekunder FØR udløb vi anser token'et for at være ved at udløbe.
    # 300 sekunder = 5 minutter buffer.
    TOKEN_EXPIRY_BUFFER_SECONDS = 300


    # ============================================================
    # Token-håndtering
    # ============================================================

    def _load_token_cache(constant_name: str) -> dict[str, Any]:
        """
        Henter token-cachen fra OpenOrchestrator som en dict.
        Returnerer tom dict hvis konstanten ikke findes eller er tom/ugyldig JSON.
        """
        try:
            raw = orchestrator_connection.get_credential(constant_name).password
            if raw and raw.strip() not in ("", "{}"):
                return json.loads(raw)
        except (ValueError, json.JSONDecodeError):
            pass
        return {}


    def _save_token_cache(constant_name: str, cache: dict[str, Any]) -> None:
        existing = orchestrator_connection.get_credential(constant_name)
        orchestrator_connection.update_credential(constant_name, existing.username, json.dumps(cache))


    def _is_token_valid(cache: dict[str, Any]) -> bool:
        """
        Returnerer True hvis cachen indeholder et access_token der ikke er udløbet
        (med buffer).
        """
        if not cache.get("access_token"):
            return False
        expires_at_str = cache.get("expires_at")
        if not expires_at_str:
            return False
        try:
            expires_at = datetime.fromisoformat(expires_at_str)
            now = datetime.now(timezone.utc)
            return expires_at > now + timedelta(seconds=TOKEN_EXPIRY_BUFFER_SECONDS)
        except ValueError:
            return False


    def _fetch_new_token(app_id: str, app_token: str) -> dict[str, Any]:
        """
        Henter et nyt token fra Podio via app-grant.
        Returnerer en dict med access_token, refresh_token og expires_at (ISO-streng i UTC).
        """
        r = requests.post(
            f"{API_BASE}/oauth/token/v2",
            headers={"Content-Type": "application/json"},
            json={
                "grant_type": "app",
                "app_id": app_id,
                "app_token": app_token,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
            timeout=HTTP_TIMEOUT,
        )
        _raise_for_status_with_body(r)

        data = r.json()
        expires_in = int(data.get("expires_in", 0))
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

        return {
            "access_token": data["access_token"],
            "refresh_token": data.get("refresh_token"),
            "expires_at": expires_at.isoformat(),
        }


    def _refresh_token(refresh_token: str) -> dict[str, Any]:
        """
        Fornyer et eksisterende token via Podios refresh_token-flow.
        Returnerer en ny cache-dict med access_token, refresh_token og expires_at.
        """
        r = requests.post(
            f"{API_BASE}/oauth/token/v2",
            headers={"Content-Type": "application/json"},
            json={
                "grant_type": "refresh_token",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "refresh_token": refresh_token,
            },
            timeout=HTTP_TIMEOUT,
        )
    
        _raise_for_status_with_body(r)

        data = r.json()
        expires_in = int(data.get("expires_in", 0))
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

        return {
            "access_token": data["access_token"],
            # Podio sender ikke altid et nyt refresh_token ved refresh —
            # behold det gamle hvis der ikke kommer et nyt.
            "refresh_token": data.get("refresh_token", refresh_token),
            "expires_at": expires_at.isoformat(),
        }


    def get_token(constant_name: str, app_id: str, app_token: str) -> str:
        """
        Returnerer et gyldigt access_token til den givne Podio-app.

        Logik:
        1. Indlæs token-cache fra OpenOrchestrator.
        2. Hvis cachen er gyldig (ikke udløbet), brug den som den er.
        3. Hvis cachen er udløbet men har et refresh_token, forsøg refresh.
        4. Hvis refresh fejler eller der slet ikke er noget gemt, hent et nyt token.
        5. Gem den opdaterede cache tilbage til OpenOrchestrator.
        """
        cache = _load_token_cache(constant_name)

        if _is_token_valid(cache):
            return cache["access_token"]

        # Token er udløbet eller mangler — forsøg refresh hvis vi har et refresh_token
        if cache.get("refresh_token"):
            orchestrator_connection.log_info(f"Token udløbet for '{constant_name}', forsøger refresh...")
            try:
                new_cache = _refresh_token(cache["refresh_token"])
                _save_token_cache(constant_name, new_cache)
                return new_cache["access_token"]
            except requests.HTTPError as e:
                orchestrator_connection.log_info(f"Refresh fejlede ({e}), henter nyt token fra bunden.")

        # Ingen brugbar cache eller refresh slog fejl — hent frisk token
        new_cache = _fetch_new_token(app_id, app_token)
        _save_token_cache(constant_name, new_cache)
        return new_cache["access_token"]


    # ============================================================
    # Hjælpefunktioner
    # ============================================================

    def strip_html(raw: str | None) -> str | None:
        if raw is None:
            return None

        text = raw
        text = re.sub(r"<\s*br\s*/?>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"</p>\s*<p>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", "", text)
        text = html.unescape(text)
        text = text.strip()
        return text or None


    def _join_nonempty(parts: list[str | None], sep: str = " ") -> str | None:
        values = [p for p in parts if p not in (None, "")]
        return sep.join(values) if values else None


    def _safe_str(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return strip_html(value)
        return str(value)


    def parse_numeric(value: Any) -> float | int | None:
        if value is None:
            return None

        if isinstance(value, (int, float)):
            return int(value) if isinstance(value, float) and value.is_integer() else value

        if isinstance(value, str):
            v = value.strip()
            if not v:
                return None
            try:
                num = float(v.replace(",", "."))
            except ValueError:
                return None
            return int(num) if num.is_integer() else num

        return None


    def make_column_name(field: dict[str, Any]) -> str:
        raw_name = (
            field.get("label")
            or (field.get("config") or {}).get("label")
            or field.get("external_id")
            or "ukendt-felt"
        )
        text = str(raw_name).strip().lower()

        replacements = {"æ": "ae", "ø": "oe", "å": "aa"}
        for old, new in replacements.items():
            text = text.replace(old, new)

        text = unicodedata.normalize("NFKD", text)
        text = "".join(c for c in text if not unicodedata.combining(c))
        text = re.sub(r"[^a-z0-9]+", "-", text)
        text = re.sub(r"-+", "-", text).strip("-")

        return text or "ukendt-felt"


    def field_label(field: dict[str, Any]) -> str | None:
        return field.get("label") or (field.get("config") or {}).get("label")


    def field_external_id(field: dict[str, Any]) -> str | None:
        return field.get("external_id")


    def field_settings(field: dict[str, Any]) -> dict[str, Any]:
        return (field.get("config") or {}).get("settings") or {}


    # ============================================================
    # Parsing
    # ============================================================

    def parse_date_values(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "start": v.get("start"),
                "end": v.get("end"),
                "start_date": v.get("start_date"),
                "start_time": v.get("start_time"),
                "end_date": v.get("end_date"),
                "end_time": v.get("end_time"),
            }
            for v in values
        ]


    def format_date_display(date_values: list[dict[str, Any]] | None) -> str | None:
        if not date_values:
            return None

        parts: list[str] = []
        for v in date_values:
            start = v.get("start") or _join_nonempty([v.get("start_date"), v.get("start_time")])
            end = v.get("end") or _join_nonempty([v.get("end_date"), v.get("end_time")])

            if start and end:
                parts.append(f"{start} - {end}")
            elif start:
                parts.append(start)
            elif end:
                parts.append(end)

        return ", ".join(parts) or None


    def parse_category_values(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
        parsed: list[dict[str, Any]] = []
        for v in values:
            raw = v.get("value")
            if isinstance(raw, dict):
                parsed.append({"text": strip_html(raw.get("text")) if raw.get("text") else None})
            elif "label" in v:
                parsed.append({"text": strip_html(str(v["label"]))})
        return parsed


    def _extract_reference_id_bundle(raw: dict[str, Any]) -> dict[str, Any]:
        item_id = raw.get("item_id")
        app_item_id = raw.get("app_item_id")
        if app_item_id is None:
            app_item_id = raw.get("app_item_id_formatted")

        app_obj = raw.get("app") if isinstance(raw.get("app"), dict) else {}
        app_id = raw.get("app_id") or app_obj.get("app_id")

        return {
            "item_id": item_id,
            "app_item_id": app_item_id,
            "app_id": app_id,
            "title": _safe_str(raw.get("title")),
            "link": raw.get("link"),
            "raw": raw,
        }


    def parse_app_reference_values(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
        parsed: list[dict[str, Any]] = []

        for v in values:
            raw = v.get("value")

            if isinstance(raw, dict):
                parsed.append(_extract_reference_id_bundle(raw))
            elif raw is not None:
                parsed.append(
                    {
                        "item_id": None,
                        "app_item_id": None,
                        "app_id": None,
                        "title": _safe_str(raw),
                        "link": None,
                        "raw": raw,
                    }
                )

        return parsed


    def parse_contact_values(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
        parsed: list[dict[str, Any]] = []
        for v in values:
            raw = v.get("value")
            if isinstance(raw, dict):
                parsed.append(
                    {
                        "name": _safe_str(raw.get("name")),
                        "mail": _safe_str(raw.get("mail")),
                    }
                )
            elif raw is not None:
                parsed.append({"name": _safe_str(raw), "mail": None})
        return parsed


    def parse_simple_values(values: list[dict[str, Any]]) -> list[Any]:
        parsed: list[Any] = []

        for v in values:
            raw = v.get("value")

            if isinstance(raw, dict):
                if isinstance(raw.get("text"), str):
                    parsed.append(strip_html(raw["text"]))
                elif isinstance(raw.get("value"), str):
                    parsed.append(strip_html(raw["value"]))
                else:
                    parsed.append(raw)
            elif isinstance(raw, str):
                parsed.append(strip_html(raw))
            elif raw is not None:
                parsed.append(raw)

        return parsed


    def parse_podio_field(field: dict[str, Any]) -> dict[str, Any]:
        column_name = make_column_name(field)
        ftype = field.get("type")
        values = field.get("values") or []

        result: dict[str, Any] = {
            "column_name": column_name,
            "external_id": field_external_id(field),
            "label": field_label(field),
            "type": ftype,
            "field_id": field.get("field_id"),
            "value": None,
            "display": None,
            "settings": field_settings(field),
        }

        if not values:
            return result

        if ftype == "date":
            parsed = parse_date_values(values)
            result["value"] = parsed
            result["display"] = format_date_display(parsed)
            return result

        if ftype in {"category", "question"}:
            parsed = parse_category_values(values)
            result["value"] = parsed
            result["display"] = ";; ".join(x["text"] for x in parsed if x.get("text")) or None
            return result

        if ftype == "app":
            parsed = parse_app_reference_values(values)
            result["value"] = parsed
            result["display"] = ";; ".join(x["title"] for x in parsed if x.get("title")) or None
            return result

        if ftype == "contact":
            parsed = parse_contact_values(values)
            result["value"] = parsed
            result["display"] = ";; ".join(x["name"] for x in parsed if x.get("name")) or None
            return result

        if ftype in {"number", "money", "progress"}:
            parsed_numbers: list[float | int | None] = []

            for v in values:
                raw = v.get("value")
                if isinstance(raw, dict):
                    raw = raw.get("value")
                parsed_numbers.append(parse_numeric(raw))

            result["value"] = parsed_numbers
            result["display"] = ";; ".join(str(x) for x in parsed_numbers if x is not None) or None
            return result

        if ftype == "calculation":
            parsed_values: list[Any] = []

            for v in values:
                raw = v.get("value")

                if isinstance(raw, dict):
                    if raw.get("value") is not None:
                        raw = raw.get("value")
                    elif raw.get("text") is not None:
                        raw = raw.get("text")

                num = parse_numeric(raw)
                if num is not None:
                    parsed_values.append(num)
                else:
                    parsed_values.append(_safe_str(raw))

            result["value"] = parsed_values
            result["display"] = ";; ".join(str(x) for x in parsed_values if x not in (None, "")) or None
            return result

        parsed = parse_simple_values(values)
        result["value"] = parsed
        result["display"] = ";; ".join(str(x) for x in parsed if x not in (None, "")) or None
        return result


    # ============================================================
    # Flatten til Excel
    # ============================================================

    def flatten_field_for_excel(parsed_field: dict[str, Any]) -> dict[str, Any]:
        column_name = (
            parsed_field.get("column_name")
            or parsed_field.get("external_id")
            or "ukendt-felt"
        )

        field_type = parsed_field["type"]
        value = parsed_field["value"]
        display = parsed_field["display"]

        out: dict[str, Any] = {}

        if field_type == "date":
            first = value[0] if value else {}
            out[f"{column_name}__start"] = first.get("start") or _join_nonempty(
                [first.get("start_date"), first.get("start_time")]
            )
            out[f"{column_name}__end"] = first.get("end") or _join_nonempty(
                [first.get("end_date"), first.get("end_time")]
            )
            out[f"{column_name}__display"] = display
            return out

        if field_type in {"category", "question"}:
            out[f"{column_name}__display"] = display
            return out

        if field_type == "app":
            refs = value or []
            out[column_name] = ";; ".join(
                str(x["item_id"]) for x in refs if x.get("item_id") is not None
            ) or None
            out[f"{column_name}__app_item_ids"] = ";; ".join(
                str(x["app_item_id"]) for x in refs if x.get("app_item_id") is not None
            ) or None
            out[f"{column_name}__titles"] = ";; ".join(
                x["title"] for x in refs if x.get("title")
            ) or None
            out[f"{column_name}__display"] = out[f"{column_name}__titles"]
            out[f"{column_name}__links"] = ";; ".join(
                x["link"] for x in refs if x.get("link")
            ) or None
            return out

        if field_type == "contact":
            out[f"{column_name}__display"] = display
            out[f"{column_name}__emails"] = ";; ".join(
                x["mail"] for x in value if x.get("mail")
            ) or None
            return out

        first = value[0] if value else None
        out[column_name] = first if first is not None else display
        return out


    def flatten_item_for_excel(item: dict[str, Any]) -> dict[str, Any]:
        row: dict[str, Any] = {
            "item_id": item.get("item_id"),
            "app_item_id": item.get("app_item_id") or item.get("app_item_id_formatted"),
            "title": item.get("title") or item.get("label"),
            "link": item.get("link"),
        }

        for field in item.get("fields", []):
            parsed = parse_podio_field(field)
            row.update(flatten_field_for_excel(parsed))

        if not row.get("projektnavn"):
            row["projektnavn"] = item.get("title") or item.get("label")

        return row


    # ============================================================
    # Relationstabel
    # ============================================================

    def extract_app_relations(
        items: list[dict[str, Any]],
        target_fields: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        relation_rows: list[dict[str, Any]] = []

        for item in items:
            parent_item_id = item.get("item_id")
            parent_app_item_id = item.get("app_item_id") or item.get("app_item_id_formatted")
            parent_title = item.get("title") or item.get("label")
            parent_link = item.get("link")

            for field in item.get("fields", []):
                if field.get("type") != "app":
                    continue

                f_label = field_label(field)
                f_external_id = field_external_id(field)
                f_column_name = make_column_name(field)
                f_settings = field_settings(field)

                if target_fields:
                    if (
                        f_label not in target_fields
                        and f_external_id not in target_fields
                        and f_column_name not in target_fields
                    ):
                        continue

                referenced_apps = f_settings.get("referenced_apps") or []
                referenced_app_ids = [
                    x.get("app_id")
                    for x in referenced_apps
                    if isinstance(x, dict) and x.get("app_id") is not None
                ]

                parsed = parse_app_reference_values(field.get("values") or [])

                for ref in parsed:
                    relation_rows.append(
                        {
                            "parent_item_id": parent_item_id,
                            "parent_app_item_id": parent_app_item_id,
                            "parent_title": parent_title,
                            "parent_link": parent_link,
                            "relation_field_id": field.get("field_id"),
                            "relation_field_label": f_label,
                            "relation_field_external_id": f_external_id,
                            "relation_field_column_name": f_column_name,
                            "relation_referenced_app_ids": ";; ".join(str(x) for x in referenced_app_ids) or None,
                            "child_item_id": ref.get("item_id"),
                            "child_app_item_id": ref.get("app_item_id"),
                            "child_app_id": ref.get("app_id"),
                            "child_title": ref.get("title"),
                            "child_link": ref.get("link"),
                        }
                    )

        return relation_rows


    # ============================================================
    # Datakvalitet
    # ============================================================

    def summarize_missing_child_ids(relations_df: pd.DataFrame) -> pd.DataFrame:
        if relations_df.empty:
            return pd.DataFrame()

        mask = relations_df["child_item_id"].isna()
        cols = [
            "relation_field_label",
            "relation_field_external_id",
            "relation_field_column_name",
        ]
        return (
            relations_df.loc[mask, cols]
            .value_counts(dropna=False)
            .rename("antal_manglende_child_item_id")
            .reset_index()
        )


    # ============================================================
    # API
    # ============================================================

    def _raise_for_status_with_body(r: requests.Response) -> None:
        if not r.ok:
            orchestrator_connection.log_info(f"RESPONSE: {r.text[:2000]}")
        r.raise_for_status()


    def fetch_items_basic(token: str, app_id: str, batch_size: int = 500) -> list[dict[str, Any]]:
        headers = {"Authorization": f"OAuth2 {token}"}
        all_items: list[dict[str, Any]] = []
        offset = 0

        while True:
            r = requests.get(
                f"{API_BASE}/item/app/{app_id}/",
                headers=headers,
                params={"limit": batch_size, "offset": offset},
                timeout=HTTP_TIMEOUT,
            )
            _raise_for_status_with_body(r)

            data = r.json()
            items = data.get("items", [])

            if not items:
                break

            all_items.extend(items)
            offset += len(items)

            if len(items) < batch_size:
                break

        return all_items


    def fetch_item_full_by_item_id(token: str, item_id: int) -> dict[str, Any]:
        headers = {"Authorization": f"OAuth2 {token}"}
        r = requests.get(
            f"{API_BASE}/item/{item_id}",
            headers=headers,
            timeout=HTTP_TIMEOUT,
        )
        _raise_for_status_with_body(r)
        return r.json()


    def fetch_items(
        token: str,
        app_id: str,
        batch_size: int = 500,
        hydrate_full_items: bool = False,
    ) -> list[dict[str, Any]]:
        basic_items = fetch_items_basic(token, app_id=app_id, batch_size=batch_size)

        if not hydrate_full_items:
            return basic_items

        full_items: list[dict[str, Any]] = []
        for item in basic_items:
            item_id = item.get("item_id")
            if item_id is None:
                full_items.append(item)
                continue
            full_items.append(fetch_item_full_by_item_id(token, int(item_id)))

        return full_items


    # ============================================================
    # Excel-eksport
    # ============================================================

    def items_to_excel(
        items: list[dict[str, Any]],
        filename: str,
        target_relation_fields: set[str] | None = None,
    ) -> None:
        main_rows = [flatten_item_for_excel(item) for item in items]
        main_df = pd.DataFrame(main_rows)

        relation_rows = extract_app_relations(items, target_fields=target_relation_fields)
        relations_df = pd.DataFrame(relation_rows)

        quality_df = summarize_missing_child_ids(relations_df)

        with pd.ExcelWriter(filename, engine="openpyxl") as writer:
            main_df.to_excel(writer, sheet_name="Projekter", index=False)
            relations_df.to_excel(writer, sheet_name="Relationer", index=False)
            quality_df.to_excel(writer, sheet_name="Datakvalitet", index=False)


    def delete_files(*filenames: str) -> None:
        for filename in filenames:
            try:
                os.remove(filename)
            except FileNotFoundError:
                orchestrator_connection.log_info(f"Fil ikke fundet, springer over: {filename}")

    def sharepoint_uploader(base_url: str, folder_url: str, *filenames: str) -> None:
        certification = orchestrator_connection.get_credential("SharePointCert")
        api = orchestrator_connection.get_credential("SharePointAPI")

        cert_credentials = {
            "tenant": api.username,
            "client_id": api.password,
            "thumbprint": certification.username,
            "cert_path": certification.password,
        }
        ctx = ClientContext(base_url).with_client_certificate(**cert_credentials)
        ctx.load(ctx.web)
        ctx.execute_query()

        target_folder = ctx.web.get_folder_by_server_relative_url(folder_url)

        for filename in filenames:
            with open(filename, "rb") as f:
                target_folder.upload_file(filename, f.read()).execute_query()
            orchestrator_connection.log_info(f"Uploadet: {filename} → {folder_url}")
    # ============================================================
    # Main
    # ============================================================

    def main() -> None:
        # Hent tokens — én per app. Genbruger cachet token hvis det stadig er gyldigt,
        # refresher hvis det er udløbet, og henter helt nyt hvis ingen cache findes.
        token_mobilitet = get_token(
            constant_name=TOKEN_CONSTANT_MOBILITET,
            app_id=APP_ID_mobilitet,
            app_token=APP_TOKEN_mobilitet,
        )
        token_anlæg = get_token(
            constant_name=TOKEN_CONSTANT_ANLÆG,
            app_id=APP_ID_anlæg,
            app_token=APP_TOKEN_anlæg,
        )

        # Hent og eksportér mobilitetsdata
        items_mobilitet = fetch_items(
            token=token_mobilitet,
            app_id=APP_ID_mobilitet,
            batch_size=500,
            hydrate_full_items=HYDRATE_FULL_ITEMS,
        )
        items_to_excel(
            items=items_mobilitet,
            filename="podio_mobilitet.xlsx",
            target_relation_fields=TARGET_RELATION_FIELDS,
        )

        # Hent og eksportér anlægsdata
        items_anlæg = fetch_items(
            token=token_anlæg,
            app_id=APP_ID_anlæg,
            batch_size=500,
            hydrate_full_items=HYDRATE_FULL_ITEMS,
        )
        items_to_excel(
            items=items_anlæg,
            filename="podio_anlaeg.xlsx",
            target_relation_fields=TARGET_RELATION_FIELDS,
        )
        sharepoint_uploader(
            f'{orchestrator_connection.get_constant("AarhusKommuneSharePoint").value}/teams/PowerBI-gruppe',
            "/teams/PowerBI-gruppe/Delte dokumenter/Data/GMP Projekter",
            "podio_mobilitet.xlsx",
            "podio_anlaeg.xlsx",
        )
        delete_files("podio_mobilitet.xlsx", "podio_anlaeg.xlsx")



    main()
