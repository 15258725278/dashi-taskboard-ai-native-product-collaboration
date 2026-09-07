import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

export const DELIVERY_PROJECTS_ENV = "CODEX_TASKBOARD_DELIVERY_PROJECTS";

function requiredString(value, field) {
  if (typeof value !== "string" || !value.trim()) {
    throw new Error(`${field} must be a non-empty string`);
  }
  return value.trim();
}

function httpUrl(value, field) {
  const source = requiredString(value, field);
  let parsed;
  try {
    parsed = new URL(source);
  } catch {
    throw new Error(`${field} must be an HTTP(S) URL`);
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new Error(`${field} must be an HTTP(S) URL`);
  }
  return parsed.toString().replace(/\/$/, "");
}

export function parseDeliveryProjects(value) {
  if (value instanceof Map) return value;
  const source = typeof value === "string" ? value.trim() : value;
  if (!source) return new Map();
  let parsed = source;
  if (typeof source === "string") {
    try {
      parsed = JSON.parse(source);
    } catch {
      throw new Error(`${DELIVERY_PROJECTS_ENV} must be valid JSON`);
    }
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`${DELIVERY_PROJECTS_ENV} must be an object keyed by project id`);
  }
  return new Map(Object.entries(parsed).map(([projectId, config]) => {
    if (!config || typeof config !== "object" || Array.isArray(config)) {
      throw new Error(`${DELIVERY_PROJECTS_ENV}.${projectId} must be an object`);
    }
    const repository = requiredString(config.repository, `${projectId}.repository`);
    if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(repository)) {
      throw new Error(`${projectId}.repository must use owner/repository form`);
    }
    return [projectId, {
      repository,
      workflow: requiredString(config.workflow, `${projectId}.workflow`),
      workflowRef: requiredString(config.workflowRef ?? "main", `${projectId}.workflowRef`),
      baseRef: requiredString(config.baseRef ?? "main", `${projectId}.baseRef`),
      deployChannel: requiredString(
        config.deployChannel ?? "manual-test-hk",
        `${projectId}.deployChannel`,
      ),
      acceptanceUrl: httpUrl(config.acceptanceUrl, `${projectId}.acceptanceUrl`),
      ghExecutable: requiredString(config.ghExecutable ?? "gh", `${projectId}.ghExecutable`),
    }];
  }));
}

export function deliveryBranchForTask(task) {
  const branch = task?.developmentContext?.branch;
  return typeof branch === "string" && branch.trim() ? branch.trim() : null;
}

async function runGh(executable, args) {
  const result = await execFileAsync(executable, args, {
    encoding: "utf8",
    maxBuffer: 10 * 1024 * 1024,
  });
  return result.stdout;
}

function parseJson(output, operation) {
  try {
    return JSON.parse(output);
  } catch {
    throw new Error(`GitHub CLI returned invalid JSON while ${operation}`);
  }
}

async function delay(milliseconds) {
  await new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function validateManifest(manifest, deliveryId, config, pullRequest) {
  if (!manifest || typeof manifest !== "object" || Array.isArray(manifest)) {
    throw new Error("Deployment workflow returned an invalid delivery manifest");
  }
  if (manifest.deliveryId !== deliveryId) {
    throw new Error("Deployment workflow returned a manifest for another delivery");
  }
  const prNumbers = Array.isArray(manifest.prNumbers)
    ? manifest.prNumbers.filter((value) => Number.isSafeInteger(value) && value > 0)
    : [];
  if (!prNumbers.includes(pullRequest.number)) {
    throw new Error("Deployment manifest does not include the implementation PR");
  }
  return {
    implementationPr: pullRequest.url,
    acceptanceUrl: httpUrl(manifest.acceptanceUrl ?? config.acceptanceUrl, "manifest.acceptanceUrl"),
    workflowRun: httpUrl(manifest.workflowRun, "manifest.workflowRun"),
    immutableTag: requiredString(manifest.immutableTag, "manifest.immutableTag"),
    mergedSha: requiredString(manifest.mergedSha, "manifest.mergedSha"),
    prNumbers,
  };
}

export async function runGitHubDelivery({
  deliveryId,
  branch,
  config,
  onUpdate = () => {},
  command = runGh,
  wait = delay,
  pollIntervalMs = 10_000,
  timeoutMs = 130 * 60_000,
}) {
  const gh = (args) => command(config.ghExecutable, args);
  onUpdate({ status: "dispatching" });
  const pullRequests = parseJson(await gh([
    "pr", "list",
    "--repo", config.repository,
    "--head", branch,
    "--state", "open",
    "--limit", "20",
    "--json", "number,url,headRefName,baseRefName",
  ]), "finding the implementation PR");
  const pullRequest = pullRequests.find((candidate) => (
    candidate.headRefName === branch && candidate.baseRefName === config.baseRef
  ));
  if (!pullRequest) {
    throw new Error(`No open ${config.repository} PR from '${branch}' to '${config.baseRef}'`);
  }

  const dispatchedAt = Date.now();
  await gh([
    "workflow", "run", config.workflow,
    "--repo", config.repository,
    "--ref", config.workflowRef,
    "-f", `pr_numbers=${pullRequest.number}`,
    "-f", `base_ref=${config.baseRef}`,
    "-f", `deploy_channel=${config.deployChannel}`,
    "-f", `delivery_id=${deliveryId}`,
  ]);

  const deadline = dispatchedAt + timeoutMs;
  let workflowRun = null;
  while (Date.now() < deadline && !workflowRun) {
    const runs = parseJson(await gh([
      "run", "list",
      "--repo", config.repository,
      "--workflow", config.workflow,
      "--event", "workflow_dispatch",
      "--limit", "30",
      "--json", "databaseId,displayTitle,status,conclusion,url,createdAt",
    ]), "finding the deployment run");
    workflowRun = runs.find((candidate) => (
      candidate.displayTitle === `Acceptance delivery ${deliveryId}`
      && Date.parse(candidate.createdAt) >= dispatchedAt - 60_000
    )) ?? null;
    if (!workflowRun) await wait(pollIntervalMs);
  }
  if (!workflowRun) throw new Error("Timed out waiting for the deployment workflow to start");

  onUpdate({
    status: "running",
    pullRequestNumber: pullRequest.number,
    implementationPr: pullRequest.url,
    workflowRunId: workflowRun.databaseId,
    workflowRunUrl: workflowRun.url,
  });

  while (Date.now() < deadline) {
    workflowRun = parseJson(await gh([
      "run", "view", String(workflowRun.databaseId),
      "--repo", config.repository,
      "--json", "databaseId,status,conclusion,url",
    ]), "checking the deployment run");
    if (workflowRun.status === "completed") break;
    await wait(pollIntervalMs);
  }
  if (workflowRun?.status !== "completed") {
    throw new Error("Timed out waiting for the deployment workflow to finish");
  }
  if (workflowRun.conclusion !== "success") {
    throw new Error(`Deployment workflow finished with ${workflowRun.conclusion ?? "an unknown result"}`);
  }

  const temporaryDirectory = await mkdtemp(path.join(os.tmpdir(), "taskboard-delivery-"));
  try {
    await gh([
      "run", "download", String(workflowRun.databaseId),
      "--repo", config.repository,
      "--name", `taskboard-delivery-${deliveryId}`,
      "--dir", temporaryDirectory,
    ]);
    const manifest = parseJson(
      await readFile(path.join(temporaryDirectory, "delivery-manifest.json"), "utf8"),
      "reading the delivery manifest",
    );
    return validateManifest(manifest, deliveryId, config, pullRequest);
  } finally {
    await rm(temporaryDirectory, { recursive: true, force: true });
  }
}
