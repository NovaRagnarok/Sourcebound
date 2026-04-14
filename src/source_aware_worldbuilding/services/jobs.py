from __future__ import annotations

import threading
from collections.abc import Callable
from uuid import uuid4

from source_aware_worldbuilding.domain.enums import JobStatus
from source_aware_worldbuilding.domain.models import (
    BibleSectionCreateRequest,
    BibleSectionRegenerateRequest,
    JobRecord,
    JobResultRef,
    JobSummary,
    ResearchRunRequest,
    utc_now,
)
from source_aware_worldbuilding.ports import JobStorePort
from source_aware_worldbuilding.services.bible import BibleWorkspaceService
from source_aware_worldbuilding.services.research import ResearchService


class JobService:
    def __init__(
        self,
        job_store: JobStorePort,
        research_service: ResearchService,
        bible_service: BibleWorkspaceService,
        *,
        poll_interval_seconds: float = 0.25,
    ) -> None:
        self.job_store = job_store
        self.research_service = research_service
        self.bible_service = bible_service
        self.poll_interval_seconds = poll_interval_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def list_jobs(self, *, status: str | None = None) -> list[JobRecord]:
        return self.job_store.list_jobs(status=status)

    def get_job(self, job_id: str) -> JobRecord | None:
        return self.job_store.get_job(job_id)

    def summarize_latest_for_run(self, run_id: str) -> JobSummary | None:
        for job in self.job_store.list_jobs():
            if job.result_ref.run_id == run_id or job.payload.get("run_id") == run_id:
                return self._to_summary(job)
        return None

    def summarize_latest_for_section(self, section_id: str) -> JobSummary | None:
        for job in self.job_store.list_jobs():
            if job.result_ref.section_id == section_id or job.payload.get("section_id") == section_id:
                return self._to_summary(job)
        return None

    def latest_completed_export_for_project(self, project_id: str) -> dict[str, object] | None:
        for job in self.job_store.list_jobs():
            if job.job_type != "bible_project_export":
                continue
            if job.result_ref.project_id != project_id and job.payload.get("project_id") != project_id:
                continue
            if job.status not in {JobStatus.COMPLETED, JobStatus.PARTIAL}:
                continue
            if isinstance(job.result_payload, dict):
                return job.result_payload
        return None

    def cancel_job(self, job_id: str) -> JobRecord:
        job = self.get_job(job_id)
        if job is None:
            raise ValueError("Job not found.")
        now = utc_now()
        if job.status == JobStatus.PENDING:
            job.status = JobStatus.CANCELLED
            job.status_label = "cancelled"
            job.progress_stage = "cancelled"
            job.cancel_requested_at = now
            job.completed_at = now
            job.updated_at = now
            job.warnings.append("Cancelled before execution started.")
            self.job_store.update_job(job)
            return job
        if job.status in {JobStatus.RUNNING, JobStatus.PENDING}:
            job.cancel_requested_at = now
            job.updated_at = now
            job.warnings.append("Cancellation requested; the worker will stop at the next safe checkpoint.")
            self.job_store.update_job(job)
            return job
        return job

    def retry_job(self, job_id: str) -> JobRecord:
        previous = self.get_job(job_id)
        if previous is None:
            raise ValueError("Job not found.")
        if previous.status != JobStatus.FAILED or not previous.retryable:
            raise ValueError("Only failed retryable jobs can be retried.")
        retry = self._new_job(
            previous.job_type,
            dict(previous.payload),
            result_ref=previous.result_ref,
            retry_of_job_id=previous.job_id,
            attempt_count=previous.attempt_count + 1,
            max_attempts=max(previous.max_attempts, previous.attempt_count + 1),
        )
        self.job_store.save_job(retry)
        return retry

    def enqueue_research_run(self, request: ResearchRunRequest) -> JobRecord:
        run = self.research_service.prepare_run(request)
        job = self._new_job(
            "research_run_create",
            {"run_id": run.run_id, "request": request.model_dump(mode="json")},
            result_ref=JobResultRef(run_id=run.run_id),
        )
        self.job_store.save_job(job)
        return job

    def enqueue_research_stage(self, run_id: str) -> JobRecord:
        self._require_run(run_id)
        job = self._new_job(
            "research_run_stage",
            {"run_id": run_id},
            result_ref=JobResultRef(run_id=run_id),
        )
        self.job_store.save_job(job)
        return job

    def enqueue_research_extract(self, run_id: str) -> JobRecord:
        self._require_run(run_id)
        job = self._new_job(
            "research_run_extract",
            {"run_id": run_id},
            result_ref=JobResultRef(run_id=run_id),
        )
        self.job_store.save_job(job)
        return job

    def enqueue_bible_compose(self, request: BibleSectionCreateRequest) -> JobRecord:
        section = self.bible_service.prepare_section(request)
        job = self._new_job(
            "bible_section_compose",
            {"section_id": section.section_id, "request": request.model_dump(mode="json")},
            result_ref=JobResultRef(section_id=section.section_id),
        )
        self.job_store.save_job(job)
        return job

    def enqueue_bible_regenerate(
        self,
        section_id: str,
        request: BibleSectionRegenerateRequest | None = None,
    ) -> JobRecord:
        if self.bible_service.get_section(section_id) is None:
            raise ValueError("Bible section not found.")
        payload = {"section_id": section_id}
        if request is not None:
            payload["request"] = request.model_dump(mode="json")
        job = self._new_job(
            "bible_section_regenerate",
            payload,
            result_ref=JobResultRef(section_id=section_id),
        )
        self.job_store.save_job(job)
        return job

    def enqueue_bible_export(self, project_id: str) -> JobRecord:
        if self.bible_service.get_profile(project_id) is None:
            raise ValueError("Bible project profile not found.")
        job = self._new_job(
            "bible_project_export",
            {"project_id": project_id},
            result_ref=JobResultRef(project_id=project_id),
        )
        self.job_store.save_job(job)
        return job

    def start_worker(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="sourcebound-job-worker", daemon=True)
        self._thread.start()

    def stop_worker(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        self._thread = None

    def process_pending_jobs(self) -> bool:
        jobs = self.job_store.list_jobs(status=JobStatus.PENDING.value)
        if not jobs:
            return False
        self._execute_job(jobs[0])
        return True

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            processed = self.process_pending_jobs()
            if not processed:
                self._stop_event.wait(self.poll_interval_seconds)

    def _execute_job(self, job: JobRecord) -> None:
        if self._cancel_requested(job):
            self._mark_cancelled(job, "Cancelled before execution started.")
            return
        job.status = JobStatus.RUNNING
        job.status_label = "running"
        job.progress_stage = "running"
        job.progress_current = 10
        job.started_at = utc_now()
        job.last_heartbeat_at = job.started_at
        job.updated_at = utc_now()
        self.job_store.update_job(job)
        try:
            if job.job_type == "research_run_create":
                run_id = str(job.payload["run_id"])
                self.research_service.execute_run(
                    run_id,
                    checkpoint=self._checkpoint(job, "Research run cancelled before the next query batch."),
                )
                job.result_ref = JobResultRef(run_id=run_id)
                run_detail = self.research_service.get_run_detail(run_id)
                if run_detail is not None:
                    if run_detail.run.status.value in {"completed_partial", "degraded_fallback"}:
                        job.completion_state = "partial"
                        job.warnings.extend(run_detail.run.warnings)
                    job.result_payload = {
                        "run_status": run_detail.run.status.value,
                        "warnings": run_detail.run.warnings,
                    }
            elif job.job_type == "research_run_stage":
                run_id = str(job.payload["run_id"])
                self.research_service.stage_run(
                    run_id,
                    checkpoint=self._checkpoint(job, "Research staging cancelled before the next finding was staged."),
                )
                job.result_ref = JobResultRef(run_id=run_id)
            elif job.job_type == "research_run_extract":
                run_id = str(job.payload["run_id"])
                self.research_service.extract_run(
                    run_id,
                    checkpoint=self._checkpoint(job, "Research extraction cancelled before the next extraction phase."),
                )
                job.result_ref = JobResultRef(run_id=run_id)
            elif job.job_type == "bible_section_compose":
                section_id = str(job.payload["section_id"])
                section = self.bible_service.compose_prepared_section(section_id)
                job.result_ref = JobResultRef(section_id=section_id)
                job.result_payload = {
                    "section_title": section.title,
                    "claim_count": len(section.references.claim_ids),
                }
            elif job.job_type == "bible_section_regenerate":
                section_id = str(job.payload["section_id"])
                request_payload = job.payload.get("request")
                request = (
                    BibleSectionRegenerateRequest.model_validate(request_payload)
                    if isinstance(request_payload, dict)
                    else None
                )
                section = self.bible_service.regenerate_section(section_id, request)
                job.result_ref = JobResultRef(section_id=section_id)
                job.result_payload = {
                    "section_title": section.title,
                    "manual_edits_preserved": section.has_manual_edits,
                }
            elif job.job_type == "bible_project_export":
                project_id = str(job.payload["project_id"])
                self._checkpoint(job, "Bible export cancelled before bundle assembly.")()
                export_bundle = self.bible_service.export_project(project_id)
                job.result_ref = JobResultRef(project_id=project_id)
                job.result_payload = export_bundle.model_dump(mode="json")
            else:
                raise ValueError(f"Unsupported job type: {job.job_type}")
            if self._cancel_requested(job):
                job.warnings.append(
                    "Cancellation was requested while work was already in progress; the current atomic step completed."
                )
            self._mark_completed(job)
        except _JobCancellationRequested as exc:
            self._mark_cancelled(job, str(exc))
        except Exception as exc:
            self._mark_failed(job, exc)
        self.job_store.update_job(job)

    def _new_job(
        self,
        job_type: str,
        payload: dict[str, object],
        *,
        result_ref: JobResultRef,
        retry_of_job_id: str | None = None,
        attempt_count: int = 1,
        max_attempts: int = 2,
    ) -> JobRecord:
        now = utc_now()
        return JobRecord(
            job_id=f"job-{uuid4().hex[:12]}",
            job_type=job_type,
            payload=payload,
            retryable=True,
            retry_of_job_id=retry_of_job_id,
            attempt_count=attempt_count,
            max_attempts=max_attempts,
            status_label="queued",
            progress_stage="queued",
            progress_current=0,
            progress_total=100,
            result_ref=result_ref,
            created_at=now,
            updated_at=now,
        )

    def _require_run(self, run_id: str) -> None:
        if self.research_service.get_run_detail(run_id) is None:
            raise ValueError("Research run not found.")

    def _to_summary(self, job: JobRecord) -> JobSummary:
        return JobSummary(
            job_id=job.job_id,
            job_type=job.job_type,
            status=job.status,
            status_label=job.status_label,
            completion_state=job.completion_state,
            progress_stage=job.progress_stage,
            progress_current=job.progress_current,
            progress_total=job.progress_total,
            updated_at=job.updated_at,
            retryable=job.retryable,
            warnings=job.warnings,
        )

    def _cancel_requested(self, job: JobRecord) -> bool:
        refreshed = self.job_store.get_job(job.job_id)
        if refreshed is not None:
            job.cancel_requested_at = refreshed.cancel_requested_at
        return bool(job.cancel_requested_at)

    def _mark_completed(self, job: JobRecord) -> None:
        job.status = JobStatus.PARTIAL if job.completion_state == "partial" else JobStatus.COMPLETED
        job.status_label = "partial" if job.status == JobStatus.PARTIAL else "completed"
        job.progress_stage = job.status_label
        job.progress_current = job.progress_total
        job.completed_at = utc_now()
        job.updated_at = utc_now()
        job.last_heartbeat_at = job.updated_at
        job.error = None
        job.error_code = None
        if job.completion_state is None:
            job.completion_state = "completed"

    def _mark_failed(self, job: JobRecord, exc: Exception) -> None:
        job.status = JobStatus.FAILED
        job.status_label = "failed"
        job.progress_stage = "failed"
        job.error = str(exc)
        job.error_code = exc.__class__.__name__
        job.error_detail = str(exc)
        job.completed_at = utc_now()
        job.updated_at = utc_now()
        job.last_heartbeat_at = job.updated_at

    def _mark_cancelled(self, job: JobRecord, reason: str) -> None:
        job.status = JobStatus.CANCELLED
        job.status_label = "cancelled"
        job.progress_stage = "cancelled"
        job.error = reason
        job.error_detail = reason
        job.completed_at = utc_now()
        job.updated_at = utc_now()
        job.last_heartbeat_at = job.updated_at

    def _checkpoint(self, job: JobRecord, reason: str) -> Callable[[], None]:
        def _inner() -> None:
            if self._cancel_requested(job):
                raise _JobCancellationRequested(reason)
            job.last_heartbeat_at = utc_now()
            job.updated_at = job.last_heartbeat_at
            self.job_store.update_job(job)

        return _inner


class _JobCancellationRequested(RuntimeError):
    pass
