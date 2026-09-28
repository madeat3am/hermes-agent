"""Read-only deterministic identity binding for Kanban review workers."""
from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any

from tools.kanban_tools import (
    _board,
    _check,
    _is_delegated_child_context,
    _is_dispatcher_owned_worker,
    _kanban_handler,
    _worker_run_id,
)
from tools.registry import no_cache_check_fn, registry

_MAX_REPORT_BYTES = 16 * 1024 * 1024
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_MODEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}$")
_FIELDS = (
    "request_id",
    "artifact_path",
    "artifact_sha256",
    "answering_provider",
    "answering_model",
)
_HEADER_RE = re.compile(
    rf"^(?P<key>{'|'.join(map(re.escape, _FIELDS))}):[ \t]*(?P<value>[^\r\n]+?)[ \t]*\r?$",
    re.MULTILINE,
)
_SPECIFIED_FIELD_RES = {
    "request_id": re.compile(r"^-[ \t]+request_id:[ \t]*(?P<value>[^\r\n]+?)[ \t]*\r?$", re.MULTILINE),
    "artifact_path": re.compile(r"^-[ \t]+Artifact:[ \t]*(?P<value>[^\r\n]+?)[ \t]*\r?$", re.MULTILINE),
    "artifact_sha256": re.compile(
        r"^-[ \t]+Expected artifact_sha256:[ \t]*(?P<value>[^\r\n]+?)[ \t]*\r?$",
        re.MULTILINE,
    ),
}
_SPECIFIED_ROUTE_RE = re.compile(
    r"^-[ \t]+Report answering provider/model:[ \t]*"
    r"(?P<provider>[A-Za-z0-9][A-Za-z0-9_.:-]{0,127})[ \t]+/[ \t]+"
    r"(?P<model>[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255})\.[ \t]*\r?$",
    re.MULTILINE,
)
_FAMILY_RULES = (
    ("openai-codex", re.compile(r"^gpt-"), "openai"),
    ("xai-oauth", re.compile(r"^grok-"), "xai"),
    ("anthropic", re.compile(r"^claude-"), "anthropic"),
)


@no_cache_check_fn
def _check_review_worker() -> bool:
    return bool(
        os.environ.get("HERMES_KANBAN_TASK")
        and _is_dispatcher_owned_worker()
        and not _is_delegated_child_context()
    )


def _request_fields(body: str) -> dict[str, str]:
    canonical: dict[str, list[str]] = {key: [] for key in _FIELDS}
    for match in _HEADER_RE.finditer(body or ""):
        canonical[match.group("key")].append(match.group("value").strip())

    # A triage specifier may rewrite an already-bound review card into this
    # exact labelled form. Accept only that complete dialect; do not infer from
    # prose or mix it with canonical headers.
    specified: dict[str, list[str]] = {key: [] for key in _FIELDS}
    for key, pattern in _SPECIFIED_FIELD_RES.items():
        specified[key].extend(match.group("value").strip() for match in pattern.finditer(body or ""))
    for match in _SPECIFIED_ROUTE_RE.finditer(body or ""):
        specified["answering_provider"].append(match.group("provider").strip())
        specified["answering_model"].append(match.group("model").strip())

    has_canonical = any(canonical.values())
    has_specified = any(specified.values())
    _check(
        not (has_canonical and has_specified),
        "review request mixes canonical and specified binding fields",
    )
    found = specified if has_specified else canonical
    missing = [key for key, values in found.items() if not values]
    duplicate = [key for key, values in found.items() if len(values) > 1]
    _check(not missing, f"review request is missing required field(s): {', '.join(missing)}")
    _check(not duplicate, f"review request repeats required field(s): {', '.join(duplicate)}")
    return {key: values[0] for key, values in found.items()}


def _model_family(provider: str, model: str) -> str:
    if provider == "openrouter":
        vendor = model.split("/", 1)[0] if "/" in model else model
        return vendor or "openrouter"
    for known_provider, pattern, family in _FAMILY_RULES:
        if provider == known_provider and pattern.match(model):
            return family
    return provider


def _binding(
    task: Any, *, runtime_provider: str, runtime_model: str
) -> tuple[dict[str, Any], str, str, str]:
    fields = _request_fields(task.body or "")
    _check(_ID_RE.fullmatch(fields["request_id"]), "review request has an invalid request_id")
    _check(_SHA256_RE.fullmatch(fields["artifact_sha256"]), "review request has an invalid artifact_sha256")
    _check(_ID_RE.fullmatch(fields["answering_provider"]), "review request has an invalid answering_provider")
    _check(_MODEL_RE.fullmatch(fields["answering_model"]), "review request has an invalid answering_model")
    _check(_ID_RE.fullmatch(runtime_provider), "actual reviewer provider is unavailable or invalid")
    _check(_MODEL_RE.fullmatch(runtime_model), "actual reviewer model is unavailable or invalid")

    report_path = Path(fields["artifact_path"])
    _check(report_path.is_absolute(), "review request artifact_path must be absolute")
    try:
        stat = report_path.stat()
    except OSError as exc:
        _check(False, f"review artifact is unavailable: {exc}")
    _check(report_path.is_file(), "review artifact is not a regular file")
    _check(stat.st_size <= _MAX_REPORT_BYTES, f"review artifact exceeds {_MAX_REPORT_BYTES} bytes")
    try:
        text = report_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        _check(False, f"review artifact could not be read as UTF-8: {exc}")
    computed = hashlib.sha256(text.strip().encode("utf-8")).hexdigest()
    expected = fields["artifact_sha256"].lower()
    binding = {
        "request_id": fields["request_id"],
        "artifact_sha256": computed,
        "reviewer_provider": runtime_provider,
        "reviewer_model": runtime_model,
        "same_family": _model_family(runtime_provider, runtime_model)
        == _model_family(fields["answering_provider"], fields["answering_model"]),
    }
    return binding, str(report_path), expected, computed


@_kanban_handler("kanban_review_binding")
def _handle_review_binding(
    args: dict,
    *,
    runtime_provider: str | None = None,
    runtime_model: str | None = None,
    **_kw: Any,
) -> str:
    _check(not args, "kanban_review_binding takes no parameters")
    _check(_is_dispatcher_owned_worker(), "kanban_review_binding is dispatcher-worker-only")
    _check(not _is_delegated_child_context(), "kanban_review_binding is unavailable to delegated children")
    task_id = os.environ.get("HERMES_KANBAN_TASK", "").strip()
    _check(task_id, "kanban_review_binding requires HERMES_KANBAN_TASK")
    run_id = _worker_run_id(task_id)
    _check(run_id is not None, "kanban_review_binding requires a valid HERMES_KANBAN_RUN_ID")
    with _board(None) as (kb, conn):
        task = kb.get_task(conn, task_id)
        _check(task is not None, f"task {task_id} not found")
        _check(task.current_run_id == run_id, "worker run no longer owns the review task")
        binding, artifact_path, expected, computed = _binding(
            task,
            runtime_provider=str(runtime_provider or "").strip(),
            runtime_model=str(runtime_model or "").strip(),
        )
    matches = computed == expected
    return json.dumps(
        {
            "ok": matches,
            "task_id": task_id,
            "run_id": run_id,
            "artifact_path": artifact_path,
            "expected_artifact_sha256": expected,
            "artifact_matches": matches,
            "binding": binding,
            **({} if matches else {"error": "artifact identity mismatch"}),
        },
        sort_keys=True,
    )


KANBAN_REVIEW_BINDING_SCHEMA = {
    "name": "kanban_review_binding",
    "description": (
        "For the dispatcher-owned review task only, read its exact request identity and artifact, "
        "verify the requested SHA-256, and return deterministic verdict binding JSON using the "
        "provider/model route that actually produced this tool call. Takes no parameters. Call "
        "before reviewing and again immediately before submitting the verdict."
    ),
    "parameters": {"type": "object", "properties": {}, "required": []},
}

registry.register(
    name="kanban_review_binding",
    toolset="kanban",
    schema=KANBAN_REVIEW_BINDING_SCHEMA,
    handler=_handle_review_binding,
    emoji="🔏",
    check_fn=_check_review_worker,
)
