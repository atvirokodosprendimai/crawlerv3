// Minimal Node 20+ task worker for crawlerv3.
// Replace processHTMLToMarkdown with your transform; the loop stays.
//
// Run:
//   REGISTRY=http://localhost:8080 PAT=... KIND=html_to_markdown node worker.mjs

import { createHash } from 'node:crypto'

const REGISTRY = (process.env.REGISTRY ?? '').replace(/\/$/, '')
const PAT      = process.env.PAT ?? ''
const KINDS    = (process.env.KIND ?? 'html_to_markdown').split(',').map(s => s.trim())
const BATCH    = Number(process.env.BATCH ?? 4)
const IDLE_MS  = Number(process.env.IDLE_MS ?? 5000)

if (!REGISTRY || !PAT) {
  console.error('REGISTRY and PAT env vars required')
  process.exit(2)
}

const auth = { Authorization: `Bearer ${PAT}` }
const sleep = (ms) => new Promise(r => setTimeout(r, ms))

// ---- Replace this with your real transform. -------------------------------
// Return either { extractedText } or { outputBytes, outputContentType, nextProcessor? }.
async function processHTMLToMarkdown(task, body) {
  const md = `# Extracted from lake object ${task.lake_object_id}\n\nSource bytes: ${body.length}\n`
  return {
    outputBytes: Buffer.from(md, 'utf8'),
    outputContentType: 'text/markdown; charset=utf-8',
  }
}
// ---------------------------------------------------------------------------

async function reserve() {
  const r = await fetch(`${REGISTRY}/v1/tasks/reserve`, {
    method: 'POST',
    headers: { ...auth, 'Content-Type': 'application/json' },
    body: JSON.stringify({ kinds: KINDS, batch: BATCH }),
  })
  if (!r.ok) throw new Error(`reserve ${r.status} ${await r.text()}`)
  return r.json()
}

async function downloadBlob(blobURL) {
  const r = await fetch(`${REGISTRY}${blobURL}`, { headers: auth })
  if (!r.ok) throw new Error(`blob ${r.status} ${await r.text()}`)
  return Buffer.from(await r.arrayBuffer())
}

async function postResult(task, result) {
  const meta = { task_id: task.task_id, lease_token: task.lease_token }
  const form = new FormData()

  if (result.extractedText != null) {
    meta.extracted_text = result.extractedText
  } else if (result.outputBytes) {
    const sha = createHash('sha256').update(result.outputBytes).digest('hex')
    meta.output_content_type   = result.outputContentType
    meta.output_content_sha256 = sha
    if (result.nextProcessor) meta.next_processor = result.nextProcessor
    form.set('blob', new Blob([result.outputBytes]), 'output.bin')
  } else {
    throw new Error('result must have extractedText or outputBytes')
  }
  form.set('meta', JSON.stringify(meta))

  const r = await fetch(`${REGISTRY}/v1/tasks/result`, {
    method: 'POST', headers: auth, body: form,
  })
  if (!r.ok) throw new Error(`result ${r.status} ${await r.text()}`)
}

async function postFail(task, code, message, retryable) {
  await fetch(`${REGISTRY}/v1/tasks/fail`, {
    method: 'POST',
    headers: { ...auth, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      task_id: task.task_id, lease_token: task.lease_token,
      error_code: code, error_message: message, retryable,
    }),
  }).catch(err => console.error('fail post:', err))
}

async function workOne(task) {
  console.log(`task=${task.task_id} processor=${task.processor} blob=${task.blob_url} size=${task.blob_size_bytes}`)
  let body
  try {
    body = await downloadBlob(task.blob_url)
  } catch (err) {
    await postFail(task, 'download', String(err), true)
    return
  }
  try {
    const result = await processHTMLToMarkdown(task, body)
    await postResult(task, result)
  } catch (err) {
    await postFail(task, 'process', String(err), true)
  }
}

let stopping = false
for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => { stopping = true; console.log(`worker: ${sig}, stopping`) })
}

while (!stopping) {
  let resp
  try {
    resp = await reserve()
  } catch (err) {
    console.error('reserve:', err.message)
    await sleep(IDLE_MS)
    continue
  }
  if (!resp.tasks?.length) { await sleep(IDLE_MS); continue }
  for (const t of resp.tasks) {
    if (stopping) break
    await workOne(t)
  }
}
