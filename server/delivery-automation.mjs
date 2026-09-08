import { execFile } from "node:child_process";
import { realpath } from "node:fs/promises";
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
    const mode = requiredString(config.mode ?? "local", `${projectId}.mode`);
    if (mode !== "local") throw new Error(`${projectId}.mode must be local`);
    const deployScript = requiredString(config.deployScript, `${projectId}.deployScript`);
    if (path.isAbsolute(deployScript)) {
      throw new Error(`${projectId}.deployScript must be relative to the bound worktree`);
    }
    return [projectId, {
      mode,
      deployScript,
      acceptanceUrl: httpUrl(config.acceptanceUrl, `${projectId}.acceptanceUrl`),
      deploymentRecordUrl: httpUrl(
        config.deploymentRecordUrl,
        `${projectId}.deploymentRecordUrl`,
      ),
      implementationUrl: httpUrl(
        config.implementationUrl ?? config.deploymentRecordUrl,
        `${projectId}.implementationUrl`,
      ),
      shellExecutable: requiredString(
        config.shellExecutable ?? "/bin/zsh",
        `${projectId}.shellExecutable`,
      ),
    }];
  }));
}

export function deliveryBranchForTask(task) {
  const branch = task?.developmentContext?.branch;
  return typeof branch === "string" && branch.trim() ? branch.trim() : null;
}

export function deliveryWorkspaceForTask(task) {
  if (task?.developmentContext?.type !== "worktree") return null;
  const workspacePath = task.developmentContext.path;
  return typeof workspacePath === "string" && workspacePath.trim()
    ? path.resolve(workspacePath.trim())
    : null;
}

function parseJson(output) {
  try {
    return JSON.parse(output);
  } catch {
    throw new Error("Local deployment returned invalid JSON");
  }
}

function taskNumber(identifier) {
  const match = String(identifier ?? "").match(/(\d+)$/);
  return match ? Number(match[1]) : null;
}

async function resolveDeployScript(workspacePath, relativeScript) {
  const workspace = await realpath(workspacePath);
  const script = await realpath(path.resolve(workspace, relativeScript));
  if (script !== workspace && !script.startsWith(`${workspace}${path.sep}`)) {
    throw new Error("Local deployment script must remain inside the bound worktree");
  }
  return { workspace, script };
}

function validateManifest(manifest, deliveryId, config, identifier) {
  if (!manifest || typeof manifest !== "object" || Array.isArray(manifest)) {
    throw new Error("Local deployment returned an invalid delivery manifest");
  }
  if (manifest.deliveryId !== deliveryId) {
    throw new Error("Local deployment returned a manifest for another delivery");
  }
  const number = taskNumber(identifier);
  return {
    implementationPr: httpUrl(
      manifest.implementationUrl ?? config.implementationUrl,
      "manifest.implementationUrl",
    ),
    acceptanceUrl: httpUrl(
      manifest.acceptanceUrl ?? config.acceptanceUrl,
      "manifest.acceptanceUrl",
    ),
    workflowRun: httpUrl(
      manifest.deploymentRecordUrl ?? config.deploymentRecordUrl,
      "manifest.deploymentRecordUrl",
    ),
    immutableTag: requiredString(manifest.immutableTag, "manifest.immutableTag"),
    mergedSha: requiredString(manifest.gitSha, "manifest.gitSha"),
    prNumbers: number === null ? [] : [number],
  };
}

async function runCommand(executable, args, options) {
  return execFileAsync(executable, args, {
    ...options,
    encoding: "utf8",
    maxBuffer: 10 * 1024 * 1024,
  });
}

export async function runLocalDelivery({
  deliveryId,
  workspacePath,
  taskIdentifier,
  config,
  onUpdate = () => {},
  command = runCommand,
}) {
  const { workspace, script } = await resolveDeployScript(workspacePath, config.deployScript);
  onUpdate({
    status: "running",
    workflowRunUrl: config.deploymentRecordUrl,
  });
  const { stdout } = await command(config.shellExecutable, [
    script,
    "--delivery-id", deliveryId,
    "--acceptance-url", config.acceptanceUrl,
    "--record-url", config.deploymentRecordUrl,
    "--implementation-url", config.implementationUrl,
  ], { cwd: workspace });
  return validateManifest(parseJson(stdout), deliveryId, config, taskIdentifier);
}
