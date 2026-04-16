from __future__ import annotations

from fastapi import APIRouter

from source_aware_worldbuilding.api.dependencies import (
    get_bible_workspace_service,
    get_candidate_store,
    get_evidence_store,
    get_job_service,
    get_truth_store,
)
from source_aware_worldbuilding.domain.enums import UNRESOLVED_REVIEW_STATES
from source_aware_worldbuilding.domain.models import (
    BibleSection,
    JobSummary,
    WorkspaceAction,
    WorkspaceBackgroundItem,
    WorkspaceSectionSnapshot,
    WorkspaceSummary,
)
from source_aware_worldbuilding.services.status import build_runtime_status

router = APIRouter(prefix="/v1/workspace", tags=["workspace"])


@router.get("/summary")
def get_workspace_summary(project_id: str | None = None) -> dict:
    runtime_status = build_runtime_status()
    if _workspace_requires_setup(runtime_status):
        return _build_setup_summary(runtime_status).model_dump(mode="json")

    bible_service = get_bible_workspace_service()
    candidate_store = get_candidate_store()
    evidence_store = get_evidence_store()
    truth_store = get_truth_store()
    job_service = get_job_service()

    profiles = bible_service.list_profiles()
    profile = None
    if project_id:
        profile = bible_service.get_profile(project_id)
    elif profiles:
        profile = profiles[0]
    resolved_project_id = project_id or (profile.project_id if profile else None)
    sections = bible_service.list_sections(resolved_project_id) if resolved_project_id else []
    candidates = candidate_store.list_candidates()
    claims = truth_store.list_claims()
    pending_candidates = [
        item for item in candidates if item.review_state in UNRESOLVED_REVIEW_STATES
    ]
    thin_sections = [
        section
        for section in sections
        if section.generation_status.value != "ready" or not section.ready_for_writer
    ]
    current_section = _select_current_section(sections)
    current_snapshot = (
        _section_snapshot(
            current_section,
            job_service.summarize_latest_for_section(current_section.section_id),
        )
        if current_section is not None
        else None
    )
    handoff_section = _select_handoff_section(sections)
    handoff_snapshot = (
        _section_snapshot(
            handoff_section,
            job_service.summarize_latest_for_section(handoff_section.section_id),
        )
        if handoff_section is not None
        else None
    )
    jobs = job_service.list_jobs()
    background_items = _build_background_items(jobs, sections)
    summary = WorkspaceSummary(
        project=profile,
        current_section=current_snapshot,
        next_actions=_build_actions(
            runtime_status=runtime_status,
            profile=profile,
            pending_review_count=len(pending_candidates),
            reviewed_canon_count=len(claims),
            evidence_count=len(evidence_store.list_evidence()),
            sections=sections,
            current_section=current_snapshot,
            handoff_section=handoff_snapshot,
            background_items=background_items,
        ),
        background_items=background_items[:4],
        pending_review_count=len(pending_candidates),
        reviewed_canon_count=len(claims),
        bible_section_count=len(sections),
        evidence_count=len(evidence_store.list_evidence()),
        active_background_count=len(
            [
                job
                for job in jobs
                if (job.status_label or job.status.value) in {"queued", "running", "partial"}
            ]
        ),
        thin_section_count=len(thin_sections),
    )
    return summary.model_dump(mode="json")


def _workspace_requires_setup(runtime_status) -> bool:
    services = {service.name: service for service in runtime_status.services}
    return any(
        services[name].ready is False
        for name in ("app_state", "truth_store", "bible_workspace", "job_worker")
    )


def _build_setup_summary(runtime_status) -> WorkspaceSummary:
    return WorkspaceSummary(
        next_actions=_build_setup_actions(runtime_status),
    )


def _build_setup_actions(runtime_status) -> list[WorkspaceAction]:
    services = {service.name: service for service in runtime_status.services}
    actions: list[WorkspaceAction] = []

    if any(
        services[name].ready is False
        for name in ("app_state", "truth_store")
        if name in services
    ):
        actions.append(
            WorkspaceAction(
                action_id="start-postgres",
                title="Start Postgres",
                summary=(
                    "Blocking the default workspace path until Sourcebound can persist workflow "
                    "state and canonical claims."
                ),
                screen="workspace",
                tone="queued",
                badge="blocking",
                command="docker compose up -d postgres",
            )
        )

    projection = services.get("projection")
    if projection is not None and projection.ready is False:
        if projection.mode == "qdrant:uninitialized":
            actions.append(
                WorkspaceAction(
                    action_id="seed-dev-data",
                    title="Seed dev data",
                    summary=(
                        "Required before the sample workspace can load with the default "
                        "project and initialized Qdrant projection."
                    ),
                    screen="workspace",
                    tone="queued",
                    badge="required next",
                    command=".venv/bin/saw seed-dev-data",
                )
            )
        else:
            actions.append(
                WorkspaceAction(
                    action_id="start-qdrant",
                    title="Start Qdrant",
                    summary=(
                        "Blocking the default workspace path until query and composition can "
                        "use projection-backed retrieval."
                    ),
                    screen="workspace",
                    tone="queued",
                    badge="required next",
                    command="docker compose up -d qdrant",
                )
            )

    if services["job_worker"].ready is False:
        actions.append(
            WorkspaceAction(
                action_id="enable-worker",
                title="Enable the job worker",
                summary=(
                    "Blocking the full workspace loop until research, bible regeneration, and "
                    "export jobs can finish without manual intervention."
                ),
                screen="workspace",
                tone="queued",
                badge="blocking",
                command="APP_JOB_WORKER_ENABLED=true .venv/bin/saw serve --reload",
            )
        )

    if not actions:
        actions.extend(
            WorkspaceAction(
                action_id=f"runtime-step-{index}",
                title="Blocking runtime step",
                summary=step,
                screen="workspace",
                tone="queued",
                badge="blocking",
            )
            for index, step in enumerate(_blocking_runtime_steps(runtime_status)[:3], start=1)
        )

    return actions[:4]


def _blocking_runtime_steps(runtime_status) -> list[str]:
    return [
        step
        for step in runtime_status.next_steps
        if step.startswith("Required") or step.startswith("Non-default local mode detected")
    ]


def _select_current_section(sections: list[BibleSection]) -> BibleSection | None:
    if not sections:
        return None

    def sort_key(section: BibleSection) -> tuple[int, int, str]:
        return (
            1 if section.generation_status.value != "ready" or not section.ready_for_writer else 0,
            1 if section.has_manual_edits else 0,
            section.updated_at,
        )

    return sorted(sections, key=sort_key, reverse=True)[0]


def _select_handoff_section(sections: list[BibleSection]) -> BibleSection | None:
    handoff_ready = [
        section for section in sections if section.ready_for_writer and section.has_manual_edits
    ]
    if not handoff_ready:
        return None
    return sorted(handoff_ready, key=lambda section: section.updated_at, reverse=True)[0]


def _section_snapshot(
    section: BibleSection, latest_job: JobSummary | None
) -> WorkspaceSectionSnapshot:
    if section.generation_status.value == "ready" and section.ready_for_writer:
        summary = (
            f"{len(section.references.claim_ids)} reviewed claims are supporting the current draft."
        )
    elif section.coverage_gaps:
        summary = section.coverage_gaps[0]
    elif section.recommended_next_research:
        summary = section.recommended_next_research[0]
    else:
        summary = "This section still needs stronger reviewed support before it feels dependable."
    return WorkspaceSectionSnapshot(
        section_id=section.section_id,
        title=section.title,
        section_type=section.section_type,
        generation_status=section.generation_status,
        ready_for_writer=section.ready_for_writer,
        has_manual_edits=section.has_manual_edits,
        claim_count=len(section.references.claim_ids),
        source_count=len(section.references.source_ids),
        evidence_count=len(section.references.evidence_ids),
        summary=summary,
        coverage_gaps=section.coverage_gaps,
        recommended_next_research=section.recommended_next_research,
        latest_job=latest_job,
    )


def _build_actions(
    *,
    runtime_status,
    profile,
    pending_review_count: int,
    reviewed_canon_count: int,
    evidence_count: int,
    sections: list[BibleSection],
    current_section: WorkspaceSectionSnapshot | None,
    handoff_section: WorkspaceSectionSnapshot | None,
    background_items: list[WorkspaceBackgroundItem],
) -> list[WorkspaceAction]:
    actions: list[WorkspaceAction] = []
    if (
        profile is None
        and reviewed_canon_count == 0
        and evidence_count == 0
        and not sections
    ):
        actions.extend(_build_setup_actions(runtime_status))
    if profile is None:
        actions.append(
            WorkspaceAction(
                action_id="setup-project",
                title="Set the project frame",
                summary=(
                    "Writer step: define place, era, and narrative focus so the writer and "
                    "operator stay aimed at the same book."
                ),
                screen="bible",
                tone="queued",
                badge="setup",
            )
        )
    if handoff_section is not None:
        actions.append(
            WorkspaceAction(
                action_id="operator-handoff",
                title="Hand off the live section",
                summary=(
                    f"Writer edits are in place for {handoff_section.title}. An operator can "
                    "regenerate the canon-backed draft or queue an export without overwriting "
                    "manual text."
                ),
                screen="bible",
                tone="author_choice",
                badge="operator handoff",
            )
        )
    if current_section is not None and (
        handoff_section is None or current_section.section_id != handoff_section.section_id
    ):
        if current_section.ready_for_writer:
            title = "Shape the live section"
            summary = (
                f"{current_section.title} has a dependable generated baseline. Writer can "
                "refine it now, then hand it to an operator for regeneration or export."
            )
            tone = "verified"
            badge = "writer step"
        else:
            title = "Open the current section"
            summary = (
                f"{current_section.title} is the live writing surface, but it still needs "
                "stronger support before the writer-to-operator handoff is dependable."
            )
            tone = "contested"
            badge = "needs support"
        actions.append(
            WorkspaceAction(
                action_id="open-current-section",
                title=title,
                summary=summary,
                screen="bible",
                tone=tone,
                badge=badge,
            )
        )
    if pending_review_count:
        actions.append(
            WorkspaceAction(
                action_id="review-facts",
                title="Review new facts",
                summary=(
                    f"Writer step: {pending_review_count} candidate facts are waiting at the "
                    "trust boundary before they can support Bible drafting."
                ),
                screen="review",
                tone="probable",
                badge=f"{pending_review_count} pending",
            )
        )
    thin_count = len(
        [
            section
            for section in sections
            if section.generation_status.value != "ready" or not section.ready_for_writer
        ]
    )
    if current_section is not None and (
        current_section.generation_status.value != "ready" or not current_section.ready_for_writer
    ):
        actions.append(
            WorkspaceAction(
                action_id="fill-current-gap",
                title="Fill the current gap",
                summary=current_section.summary,
                screen="research",
                tone="contested",
                badge="gap",
            )
        )
    elif not sections and reviewed_canon_count:
        actions.append(
            WorkspaceAction(
                action_id="compose-first-section",
                title="Compose the first section",
                summary=(
                    "Writer step: turn reviewed canon into an editable Bible section with "
                    "provenance and uncertainty still visible."
                ),
                screen="bible",
                tone="queued",
                badge="compose",
            )
        )
    elif thin_count:
        actions.append(
            WorkspaceAction(
                action_id="close-thin-sections",
                title="Strengthen thin sections",
                summary=(
                    f"{thin_count} bible section{'s are' if thin_count != 1 else ' is'} "
                    "still too thin for dependable drafting."
                ),
                screen="research",
                tone="contested",
                badge=f"{thin_count} thin",
            )
        )
    if reviewed_canon_count:
        actions.append(
            WorkspaceAction(
                action_id="ask-canon",
                title="Ask canon about the next scene",
                summary=(
                    "Pressure-test the approved record before you commit a scene detail to "
                    "the manuscript."
                ),
                screen="ask",
                tone="verified",
                badge="ask",
            )
        )
    if background_items:
        actions.append(
            WorkspaceAction(
                action_id="background-work",
                title="Check background work",
                summary=background_items[0].summary,
                screen="runs",
                tone="queued",
                badge="background",
            )
        )
    return actions[:4]


def _build_background_items(
    jobs,
    sections: list[BibleSection],
) -> list[WorkspaceBackgroundItem]:
    section_titles = {section.section_id: section.title for section in sections}
    items: list[WorkspaceBackgroundItem] = []
    seen: set[str] = set()
    for job in jobs:
        if job.job_id in seen:
            continue
        seen.add(job.job_id)
        label = _job_title(job.job_type, section_titles.get(job.result_ref.section_id))
        summary = (
            job.progress_message or job.error_detail or job.error or "Background work is available."
        )
        status_label = job.status_label or job.status.value
        items.append(
            WorkspaceBackgroundItem(
                item_id=job.job_id,
                title=label,
                summary=summary,
                status_label=status_label,
                screen="bible" if job.result_ref.section_id else "runs",
            )
        )
    return sorted(
        items,
        key=lambda item: item.status_label in {"running", "queued", "partial"},
        reverse=True,
    )


def _job_title(job_type: str, section_title: str | None) -> str:
    if section_title and "bible_section" in job_type:
        return f"{section_title} background update"
    return job_type.replace("_", " ").title()
