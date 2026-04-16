from __future__ import annotations

from fastapi import APIRouter, HTTPException

from source_aware_worldbuilding.api.routes._job_runtime import require_job_service
from source_aware_worldbuilding.domain.errors import WorkerUnavailableError

router = APIRouter(prefix="/v1/jobs", tags=["jobs"])


def _operator_summary(job: dict) -> str:
    state = job.get("worker_state") or job.get("status_label") or job.get("status") or "queued"
    attempt_count = int(job.get("attempt_count") or 1)
    max_attempts = int(job.get("max_attempts") or max(1, attempt_count))
    error_detail = job.get("error_detail") or job.get("error")
    progress_message = job.get("progress_message") or ""

    if state == "partial":
        return progress_message or "Completed with warnings. Review the warnings before trusting the result."
    if state == "stalled":
        return job.get("stalled_reason") or "Worker heartbeat expired before the job completed."
    if state == "failed":
        if job.get("retryable"):
            return (
                f"Attempt {attempt_count} of {max_attempts} failed. "
                f"{error_detail or 'Review the error and retry when ready.'}"
            )
        return error_detail or "Background job failed."
    if state == "cancel_requested":
        return progress_message or "Cancellation requested; waiting for the next safe checkpoint."
    if state == "cancelled":
        return progress_message or "Cancelled."
    if state == "completed" and job.get("retry_of_job_id"):
        return f"Retry attempt {attempt_count} of {max_attempts} finished successfully."
    if state == "queued" and job.get("retry_of_job_id"):
        return progress_message or f"Queued retry attempt {attempt_count} of {max_attempts}."
    return progress_message or str(state).replace("_", " ")


def _operator_next_action(job: dict) -> str:
    state = job.get("worker_state") or job.get("status_label") or job.get("status") or "queued"
    if state == "partial":
        return "Review warnings and degraded reasons before treating this result as done."
    if state == "stalled":
        return "Inspect worker health, then retry the job if the last checkpoint is safe."
    if state == "failed" and job.get("retryable"):
        return "Inspect the error, then use Retry job to queue a new attempt."
    if state == "failed":
        return "Inspect the error detail before re-running the higher-level workflow."
    if state == "cancel_requested":
        return "Wait for the next safe checkpoint or refresh the job detail."
    if state in {"queued", "running"}:
        return "Wait for completion or cancel the job if the work is no longer needed."
    return ""


def _job_payload(job) -> dict:
    payload = job.model_dump(mode="json")
    payload["operator_summary"] = _operator_summary(payload)
    payload["operator_next_action"] = _operator_next_action(payload)
    return payload


@router.get("")
def list_jobs(
    status: str | None = None,
) -> list[dict]:
    service = require_job_service(action="listing background jobs")
    return [_job_payload(item) for item in service.list_jobs(status=status)]


@router.get("/{job_id}")
def get_job(
    job_id: str,
) -> dict:
    service = require_job_service(action="reading background job details")
    job = service.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return _job_payload(job)


@router.post("/{job_id}/cancel")
def cancel_job(
    job_id: str,
) -> dict:
    service = require_job_service(action="cancelling background jobs")
    try:
        return _job_payload(service.cancel_job(job_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{job_id}/retry")
def retry_job(
    job_id: str,
) -> dict:
    service = require_job_service(action="retrying background jobs")
    try:
        return _job_payload(service.retry_job(job_id))
    except WorkerUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
