"""Minimal async Python task worker for crawlerv3.

Replace process_html_to_markdown with your transform; the loop stays.

Run:
    pip install httpx
    REGISTRY=http://localhost:8080 PAT=... KIND=html_to_markdown python worker.py
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import sys
from dataclasses import dataclass
from typing import Optional

import httpx


@dataclass
class Task:
    task_id: int
    processor: str
    lake_object_id: int
    blob_url: str
    blob_content_type: str
    blob_size_bytes: int
    attempt_count: int
    lease_token: str
    lease_expires_at: int


@dataclass
class Result:
    extracted_text: Optional[str] = None
    output_bytes: Optional[bytes] = None
    output_content_type: Optional[str] = None
    next_processor: Optional[str] = None


REGISTRY = os.environ.get("REGISTRY", "").rstrip("/")
PAT = os.environ.get("PAT", "")
KINDS = [k.strip() for k in os.environ.get("KIND", "html_to_markdown").split(",")]
BATCH = int(os.environ.get("BATCH", "4"))
IDLE = float(os.environ.get("IDLE_SEC", "5"))

if not REGISTRY or not PAT:
    print("REGISTRY and PAT env vars required", file=sys.stderr)
    sys.exit(2)

AUTH = {"Authorization": f"Bearer {PAT}"}


# ---- Replace this with your real transform. -------------------------------
async def process_html_to_markdown(task: Task, body: bytes) -> Result:
    md = f"# Extracted from lake object {task.lake_object_id}\n\nSource bytes: {len(body)}\n"
    return Result(
        output_bytes=md.encode("utf-8"),
        output_content_type="text/markdown; charset=utf-8",
    )
# ---------------------------------------------------------------------------


async def reserve(client: httpx.AsyncClient) -> list[Task]:
    r = await client.post(
        "/v1/tasks/reserve",
        headers=AUTH,
        json={"kinds": KINDS, "batch": BATCH},
    )
    r.raise_for_status()
    return [Task(**t) for t in r.json().get("tasks", [])]


async def download_blob(client: httpx.AsyncClient, blob_url: str) -> bytes:
    r = await client.get(blob_url, headers=AUTH)
    r.raise_for_status()
    return r.content


async def post_result(client: httpx.AsyncClient, task: Task, result: Result) -> None:
    meta: dict = {"task_id": task.task_id, "lease_token": task.lease_token}
    files: list[tuple] = []

    if result.extracted_text is not None:
        meta["extracted_text"] = result.extracted_text
    elif result.output_bytes is not None:
        meta["output_content_type"] = result.output_content_type
        meta["output_content_sha256"] = hashlib.sha256(result.output_bytes).hexdigest()
        if result.next_processor:
            meta["next_processor"] = result.next_processor
        files.append(("blob", ("output.bin", result.output_bytes, "application/octet-stream")))
    else:
        raise ValueError("Result needs extracted_text or output_bytes")

    files.insert(0, ("meta", (None, json.dumps(meta), "application/json")))
    r = await client.post("/v1/tasks/result", headers=AUTH, files=files)
    r.raise_for_status()


async def post_fail(client: httpx.AsyncClient, task: Task, code: str, msg: str, retryable: bool) -> None:
    try:
        await client.post(
            "/v1/tasks/fail",
            headers=AUTH,
            json={
                "task_id": task.task_id,
                "lease_token": task.lease_token,
                "error_code": code,
                "error_message": msg,
                "retryable": retryable,
            },
        )
    except httpx.HTTPError as e:
        print(f"fail post: {e}", file=sys.stderr)


async def work_one(client: httpx.AsyncClient, task: Task) -> None:
    print(f"task={task.task_id} processor={task.processor} blob={task.blob_url} size={task.blob_size_bytes}")
    try:
        body = await download_blob(client, task.blob_url)
    except Exception as e:
        await post_fail(client, task, "download", str(e), retryable=True)
        return
    try:
        result = await process_html_to_markdown(task, body)
        await post_result(client, task, result)
    except Exception as e:
        await post_fail(client, task, "process", str(e), retryable=True)


async def main() -> None:
    stop = asyncio.Event()

    def _stop(*_):
        print("worker: stopping")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _stop)

    async with httpx.AsyncClient(base_url=REGISTRY, timeout=60.0) as client:
        while not stop.is_set():
            try:
                tasks = await reserve(client)
            except httpx.HTTPError as e:
                print(f"reserve: {e}", file=sys.stderr)
                await asyncio.wait([asyncio.create_task(stop.wait())], timeout=IDLE)
                continue
            if not tasks:
                await asyncio.wait([asyncio.create_task(stop.wait())], timeout=IDLE)
                continue
            for t in tasks:
                if stop.is_set():
                    break
                await work_one(client, t)


if __name__ == "__main__":
    asyncio.run(main())
