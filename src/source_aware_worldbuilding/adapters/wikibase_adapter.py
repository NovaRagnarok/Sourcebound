from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
from pydantic import BaseModel

from source_aware_worldbuilding.domain.enums import ClaimKind, ClaimStatus
from source_aware_worldbuilding.domain.errors import CanonUnavailableError, WikibaseSyncError
from source_aware_worldbuilding.domain.models import ApprovedClaim, EvidenceSnippet, utc_now
from source_aware_worldbuilding.storage.json_store import JsonListStore


class WikibaseTruthStore:
    """Cache-backed truth store with authenticated remote Wikibase sync."""

    def __init__(
        self,
        base_url: str | None,
        api_url: str | None,
        username: str | None,
        password: str | None,
        property_map_raw: str | None,
        cache_dir: Path,
    ):
        self.base_url = base_url
        self.api_url = self._resolve_api_url(base_url, api_url)
        self.username = username
        self.password = password
        self.property_map = json.loads(property_map_raw) if property_map_raw else {}
        self.cache = JsonListStore(cache_dir / "wikibase_claims_cache.json")
        self.entity_map_store = JsonListStore(cache_dir / "wikibase_entity_map.json")
        self._client: httpx.Client | None = None
        self._csrf_token: str | None = None

    def list_claims(self) -> list[ApprovedClaim]:
        self._ensure_canon_available()
        entity_map = self._entity_map()
        entity_ids = [entry["entity_id"] for entry in entity_map.values() if entry.get("entity_id")]
        if not entity_ids:
            return []

        try:
            claims = self._fetch_remote_claims(entity_ids)
        except Exception as exc:
            raise WikibaseSyncError(f"Wikibase read failed: {exc}") from exc
        self.cache.write_models(claims)
        return claims

    def get_claim(self, claim_id: str) -> ApprovedClaim | None:
        self._ensure_canon_available()
        cached = self._get_cached_claim(claim_id)

        entity_map = self._entity_map()
        entity_entry = entity_map.get(claim_id)
        if not entity_entry or not entity_entry.get("entity_id"):
            return None

        try:
            remote_claim = self._fetch_remote_claim(entity_entry["entity_id"], cached)
        except Exception as exc:
            raise WikibaseSyncError(f"Wikibase read failed: {exc}") from exc
        if remote_claim is not None:
            self._upsert_cache(remote_claim)
        return remote_claim

    def save_claim(
        self,
        claim: ApprovedClaim,
        evidence: list[EvidenceSnippet] | None = None,
    ) -> None:
        self._ensure_canon_available()

        sync_record = self._sync_claim(claim, evidence or [])
        entity_map = self._entity_map()
        entity_map[claim.claim_id] = sync_record.model_dump(mode="json")
        self._save_entity_map(entity_map)
        self._upsert_cache(claim)

    def _sync_claim(
        self,
        claim: ApprovedClaim,
        evidence: list[EvidenceSnippet],
    ) -> _EntityMapRecord:
        entity_map = self._entity_map()
        existing_entry = entity_map.get(claim.claim_id, {})
        existing_entity_id = existing_entry.get("entity_id")
        existing_statement_id = existing_entry.get("statement_id")
        payload: dict[str, Any] = {
            "labels": {"en": {"language": "en", "value": claim.subject}},
            "descriptions": {
                "en": {"language": "en", "value": f"{claim.predicate}: {claim.value}"}
            },
            "aliases": {"en": self._aliases_for_claim(claim)},
        }
        statements = self._statements_for_claim(claim, evidence, statement_id=existing_statement_id)
        if statements:
            payload["claims"] = statements

        params = {
            "action": "wbeditentity",
            "format": "json",
            "token": self._csrf(),
            "data": json.dumps(payload),
            "summary": f"Sync Sourcebound claim {claim.claim_id}",
        }
        if existing_entity_id:
            params["id"] = existing_entity_id
        else:
            params["new"] = "item"

        try:
            response = self._request("POST", params)
        except Exception as exc:
            raise WikibaseSyncError(f"Wikibase sync failed: {exc}") from exc
        entity = response.get("entity") or {}
        entity_id = entity.get("id") or existing_entity_id or claim.claim_id
        if not entity_id:
            raise WikibaseSyncError("Wikibase sync failed: entity id was not returned.")

        statement = self._find_claim_statement(entity, claim.claim_id, existing_statement_id)
        if statement is None and self.property_map:
            statement = self._find_claim_statement(
                self._fetch_raw_entity(entity_id),
                claim.claim_id,
                existing_statement_id,
            )

        return _EntityMapRecord(
            claim_id=claim.claim_id,
            entity_id=entity_id,
            statement_id=statement.get("id") if statement else None,
            statement_property=(
                (statement.get("mainsnak") or {}).get("property") if statement else None
            ),
            last_synced_at=utc_now(),
        )

    def _fetch_remote_claims(self, entity_ids: list[str]) -> list[ApprovedClaim]:
        response = self._request(
            "GET",
            {
                "action": "wbgetentities",
                "format": "json",
                "ids": "|".join(entity_ids),
                "languages": "en",
            },
            auth_required=False,
        )
        entities = response.get("entities") or {}
        entity_map = self._entity_map()
        by_entity_id = {
            entry["entity_id"]: claim_id
            for claim_id, entry in entity_map.items()
            if entry.get("entity_id")
        }
        claims: list[ApprovedClaim] = []
        for entity_id, entity in entities.items():
            claim_id = by_entity_id.get(entity_id, "")
            cached = self._get_cached_claim(claim_id)
            entity_entry = entity_map.get(claim_id)
            parsed = self._parse_entity_to_claim(
                entity,
                cached,
                claim_id=claim_id,
                entity_entry=entity_entry,
            )
            if parsed is not None:
                claims.append(parsed)
        return claims

    def _fetch_remote_claim(
        self,
        entity_id: str,
        cached: ApprovedClaim | None,
    ) -> ApprovedClaim | None:
        response = self._request(
            "GET",
            {
                "action": "wbgetentities",
                "format": "json",
                "ids": entity_id,
                "languages": "en",
            },
            auth_required=False,
        )
        entity = (response.get("entities") or {}).get(entity_id)
        if not entity:
            return cached
        claim_id = cached.claim_id if cached is not None else ""
        entity_entry = self._entity_map().get(claim_id)
        return self._parse_entity_to_claim(
            entity,
            cached,
            claim_id=claim_id,
            entity_entry=entity_entry,
        )

    def _fetch_raw_entity(self, entity_id: str) -> dict[str, Any]:
        response = self._request(
            "GET",
            {
                "action": "wbgetentities",
                "format": "json",
                "ids": entity_id,
                "languages": "en",
            },
            auth_required=False,
        )
        return (response.get("entities") or {}).get(entity_id) or {}

    def _parse_entity_to_claim(
        self,
        entity: dict[str, Any],
        cached: ApprovedClaim | None,
        *,
        claim_id: str,
        entity_entry: dict[str, Any] | None,
    ) -> ApprovedClaim | None:
        if not self.property_map:
            return cached

        statement = self._find_claim_statement(
            entity,
            claim_id,
            (entity_entry or {}).get("statement_id"),
        )
        if statement is None:
            return cached

        status_value = self._statement_qualifier_value(statement, "status")
        kind_value = self._statement_qualifier_value(statement, "claim_kind")

        try:
            status = (
                ClaimStatus(status_value)
                if status_value
                else (cached.status if cached is not None else ClaimStatus.PROBABLE)
            )
        except ValueError:
            status = cached.status if cached is not None else ClaimStatus.PROBABLE

        try:
            claim_kind = (
                ClaimKind(kind_value)
                if kind_value
                else (cached.claim_kind if cached is not None else ClaimKind.OBJECT)
            )
        except ValueError:
            claim_kind = cached.claim_kind if cached is not None else ClaimKind.OBJECT

        base_claim = cached or ApprovedClaim(
            claim_id=claim_id,
            subject=self._entity_term(entity, "labels") or "Unnamed claim",
            predicate=self._statement_qualifier_value(statement, "predicate") or "described_as",
            value=self._extract_string_value(statement.get("mainsnak")) or "",
            claim_kind=claim_kind,
            status=status,
        )
        evidence_ids = self._reference_values(statement, "evidence_id")
        return base_claim.model_copy(
            update={
                "subject": self._entity_term(entity, "labels") or base_claim.subject,
                "predicate": self._statement_qualifier_value(statement, "predicate")
                or base_claim.predicate,
                "value": self._extract_string_value(statement.get("mainsnak")) or base_claim.value,
                "status": status,
                "claim_kind": claim_kind,
                "place": self._statement_qualifier_value(statement, "place") or base_claim.place,
                "time_start": self._statement_qualifier_value(statement, "time_start")
                or base_claim.time_start,
                "time_end": self._statement_qualifier_value(statement, "time_end")
                or base_claim.time_end,
                "viewpoint_scope": self._statement_qualifier_value(statement, "viewpoint_scope")
                or base_claim.viewpoint_scope,
                "notes": self._statement_qualifier_value(statement, "notes") or base_claim.notes,
                "evidence_ids": evidence_ids or base_claim.evidence_ids,
            }
        )

    def _statements_for_claim(
        self,
        claim: ApprovedClaim,
        evidence: list[EvidenceSnippet],
        *,
        statement_id: str | None = None,
    ) -> list[dict[str, Any]]:
        main_property = self.property_map.get("main_value")
        if not main_property:
            return []

        statement = {
            "mainsnak": self._string_snak(main_property, claim.value),
            "type": "statement",
            "rank": "normal",
        }
        if statement_id:
            statement["id"] = statement_id

        qualifiers = self._qualifiers_for_claim(claim)
        if qualifiers:
            statement["qualifiers"] = qualifiers

        references = self._references_for_evidence(evidence)
        if references:
            statement["references"] = references

        return [statement]

    def _qualifiers_for_claim(self, claim: ApprovedClaim) -> dict[str, list[dict[str, Any]]]:
        qualifiers: dict[str, list[dict[str, Any]]] = {}
        for key, value in {
            "predicate": claim.predicate,
            "status": claim.status.value,
            "claim_kind": claim.claim_kind.value,
            "place": claim.place,
            "time_start": claim.time_start,
            "time_end": claim.time_end,
            "viewpoint_scope": claim.viewpoint_scope,
            "notes": claim.notes,
            "app_claim_id": claim.claim_id,
        }.items():
            property_id = self.property_map.get(key)
            if property_id and value:
                qualifiers[property_id] = [self._string_snak(property_id, str(value))]
        return qualifiers

    def _references_for_evidence(
        self,
        evidence: list[EvidenceSnippet],
    ) -> list[dict[str, dict[str, list[dict[str, Any]]]]]:
        if not evidence:
            return []

        references: list[dict[str, dict[str, list[dict[str, Any]]]]] = []
        for snippet in evidence:
            snaks: dict[str, list[dict[str, Any]]] = {}
            for key, value in {
                "source_id": snippet.source_id,
                "locator": snippet.locator,
                "evidence_text": snippet.text,
                "evidence_id": snippet.evidence_id,
            }.items():
                property_id = self.property_map.get(key)
                if property_id and value:
                    snaks[property_id] = [self._string_snak(property_id, str(value))]
            if snaks:
                references.append({"snaks": snaks})
        return references

    def _aliases_for_claim(self, claim: ApprovedClaim) -> list[dict[str, str]]:
        alias_values = [
            claim.value,
            claim.predicate,
            claim.status.value,
            claim.claim_kind.value,
            *(claim.evidence_ids or []),
        ]
        return [{"language": "en", "value": value} for value in alias_values if value]

    def _entity_term(self, entity: dict[str, Any], key: str) -> str | None:
        term = (entity.get(key) or {}).get("en")
        if not term:
            return None
        return term.get("value")

    def _extract_string_value(self, snak: dict[str, Any] | None) -> str | None:
        if not snak:
            return None
        datavalue = snak.get("datavalue") or {}
        value = datavalue.get("value")
        if isinstance(value, str):
            return value
        if isinstance(value, dict) and "text" in value:
            return value["text"]
        return None

    def _string_snak(self, property_id: str, value: str) -> dict[str, Any]:
        return {
            "snaktype": "value",
            "property": property_id,
            "datatype": "string",
            "datavalue": {"value": value, "type": "string"},
        }

    def _request(
        self,
        method: str,
        params: dict[str, Any],
        *,
        auth_required: bool = True,
    ) -> dict[str, Any]:
        client = self._session()
        if auth_required and self.username and self.password and not self._csrf_token:
            self._login()
        if method == "GET":
            response = client.get(self.api_url, params=params)
        else:
            response = client.post(self.api_url, data=params)
        response.raise_for_status()
        return response.json()

    def _session(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(timeout=20.0, follow_redirects=True)
        return self._client

    def _login(self) -> None:
        if not self.username or not self.password or not self.api_url:
            return

        session = self._session()
        token_response = session.get(
            self.api_url,
            params={"action": "query", "meta": "tokens", "type": "login", "format": "json"},
        )
        token_response.raise_for_status()
        login_token = token_response.json()["query"]["tokens"]["logintoken"]

        login_response = session.post(
            self.api_url,
            data={
                "action": "login",
                "lgname": self.username,
                "lgpassword": self.password,
                "lgtoken": login_token,
                "format": "json",
            },
        )
        login_response.raise_for_status()

        csrf_response = session.get(
            self.api_url,
            params={"action": "query", "meta": "tokens", "format": "json"},
        )
        csrf_response.raise_for_status()
        self._csrf_token = csrf_response.json()["query"]["tokens"]["csrftoken"]

    def _csrf(self) -> str:
        if self._csrf_token is None:
            self._login()
        if self._csrf_token is None:
            raise WikibaseSyncError("Wikibase CSRF token is unavailable.")
        return self._csrf_token

    def _entity_map(self) -> dict[str, dict[str, str | None]]:
        raw = self.entity_map_store.read_models(_EntityMapRecord)
        return {record.claim_id: record.model_dump(mode="json") for record in raw}

    def _save_entity_map(self, entity_map: dict[str, dict[str, str | None]]) -> None:
        records = [
            _EntityMapRecord(
                claim_id=claim_id,
                entity_id=str(entry["entity_id"]),
                statement_id=entry.get("statement_id"),
                statement_property=entry.get("statement_property"),
                last_synced_at=entry.get("last_synced_at") or utc_now(),
            )
            for claim_id, entry in entity_map.items()
            if entry.get("entity_id")
        ]
        self.entity_map_store.write_models(records)

    def _get_cached_claim(self, claim_id: str) -> ApprovedClaim | None:
        return next(
            (item for item in self.cache.read_models(ApprovedClaim) if item.claim_id == claim_id),
            None,
        )

    def _upsert_cache(self, claim: ApprovedClaim) -> None:
        claims = {item.claim_id: item for item in self.cache.read_models(ApprovedClaim)}
        claims[claim.claim_id] = claim
        self.cache.write_models(claims.values())

    def _can_sync(self) -> bool:
        return bool(
            self.api_url and self.username and self.password and self.property_map
        )

    def _ensure_canon_available(self) -> None:
        if self._can_sync():
            return
        raise CanonUnavailableError(
            "Canonical Wikibase truth store is not configured."
        )

    def _find_claim_statement(
        self,
        entity: dict[str, Any],
        claim_id: str,
        statement_id: str | None,
    ) -> dict[str, Any] | None:
        claims_by_property = entity.get("claims") or {}
        for statements in claims_by_property.values():
            for statement in statements:
                if statement_id and statement.get("id") == statement_id:
                    return statement

        app_claim_property = self.property_map.get("app_claim_id")
        if not app_claim_property or not claim_id:
            return None
        for statements in claims_by_property.values():
            for statement in statements:
                qualifier_values = statement.get("qualifiers") or {}
                for snak in qualifier_values.get(app_claim_property, []):
                    if self._extract_string_value(snak) == claim_id:
                        return statement
        return None

    def _statement_qualifier_value(self, statement: dict[str, Any], key: str) -> str | None:
        property_id = self.property_map.get(key)
        if not property_id:
            return None
        qualifiers = statement.get("qualifiers") or {}
        values = qualifiers.get(property_id) or []
        if not values:
            return None
        return self._extract_string_value(values[0])

    def _reference_values(self, statement: dict[str, Any], key: str) -> list[str]:
        property_id = self.property_map.get(key)
        if not property_id:
            return []
        values: list[str] = []
        for reference in statement.get("references") or []:
            snaks = reference.get("snaks") or {}
            for snak in snaks.get(property_id, []):
                value = self._extract_string_value(snak)
                if value and value not in values:
                    values.append(value)
        return values

    def _resolve_api_url(self, base_url: str | None, api_url: str | None) -> str | None:
        if api_url:
            return api_url
        if not base_url:
            return None
        normalized = base_url.rstrip("/")
        if normalized.endswith("api.php"):
            return normalized
        if normalized.endswith("/w"):
            return f"{normalized}/api.php"
        return f"{normalized}/api.php"


class _EntityMapRecord(BaseModel):
    claim_id: str
    entity_id: str
    statement_id: str | None = None
    statement_property: str | None = None
    last_synced_at: str
