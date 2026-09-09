import { createHash, createHmac, randomUUID, timingSafeEqual } from "node:crypto";
import { execFile } from "node:child_process";
import { chmod, mkdir, open, readFile, readdir, rename, rm, stat, unlink, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { isIP } from "node:net";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { WebSocket as WebSocketClient, WebSocketServer } from "ws";

import {
  DEFAULT_PROJECT_ID,
  JIRA_PROJECT_ID,
  TASK_STATUSES,
  isTaskPriority,
  isTaskStatus,
} from "../shared/domain.mjs";
import { resolveCodexExecutable } from "../shared/codex-executable.mjs";
import { withoutTaskboardLauncherEnvironment } from "../shared/codex-environment.mjs";
import { AiChatService } from "./ai-chat.mjs";
import { resolveAiWorkspace, resolveMappedAiWorkspace } from "./ai-chat-catalog.mjs";
import { decodeComposerReferenceKey } from "./composer-reference.mjs";
import { createCloudConfigStore } from "./cloud-config.mjs";
import {
  CloudProxyError,
  createCloudProxy,
  isLocalCompanionRoute,
} from "./cloud-proxy.mjs";
import { ApiError, TaskboardDatabase } from "./database.mjs";
import {
  DELIVERY_PROJECTS_ENV,
  deliveryBranchForTask,
  deliveryWorkspaceForTask,
  parseDeliveryProjects,
  runLocalDelivery,
} from "./delivery-automation.mjs";
import { createJiraConfigStore } from "./jira-config.mjs";
import { createJiraIntegration } from "./jira-integration.mjs";
import { ProjectSummaryService } from "./project-summary.mjs";
import { productSourceBaselinePrompt, resolveProductSourceBaseline } from "./product-source-baseline.mjs";

const PROJECT_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const execFileAsync = promisify(execFile);
const JSON_BODY_LIMIT = 1024 * 1024;
const PROJECT_README_BODY_LIMIT = 3 * 1024 * 1024;
const ATTACHMENT_BODY_LIMIT = 25 * 1024 * 1024;
const AI_CHAT_TURN_BODY_LIMIT = 25 * 1024 * 1024;
const AI_CHAT_ATTACHMENT_LIMIT = 10;
const AI_CHAT_SKILL_MARKER = "\uFFFC";
const HOST_RUNTIME_TTL_MS = 3_000;
const CODEX_PLAN_TAIL_BYTES = 16 * 1024 * 1024;
const INLINE_ATTACHMENT_TYPES = new Set([
  "application/pdf",
  "image/avif",
  "image/gif",
  "image/jpeg",
  "image/png",
  "image/webp",
  "text/plain",
]);
const PROJECT_ID_PATTERN = /^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$/;
const PROJECT_BOARD_DISPLAY_SETTINGS_KEY_PREFIX = "taskboard.project-board-display-settings.v3.";
const TRUSTED_EMBED_ORIGINS = new Set(["app://-"]);
const TRUSTED_ORIGINS_ENV = "CODEX_TASKBOARD_TRUSTED_ORIGINS";
const PUBLIC_SHARED_SECRET_ENV = "CODEX_TASKBOARD_PUBLIC_SHARED_SECRET";
const PUBLIC_USERS_ENV = "CODEX_TASKBOARD_PUBLIC_USERS";
const PUBLIC_ROLES = new Set(["product", "technical", "admin"]);
const PUBLIC_SESSION_COOKIE = "codex_taskboard_session";
const PUBLIC_LOGGED_OUT_COOKIE = "codex_taskboard_logged_out";
const PUBLIC_SESSION_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const CODEX_AGENT_ACTOR = {
  type: "agent",
  id: "codex-agent",
  name: "Codex Agent",
  avatarUrl: null,
};
const CONTENT_TYPES = new Map([
  [".css", "text/css; charset=utf-8"],
  [".html", "text/html; charset=utf-8"],
  [".ico", "image/x-icon"],
  [".jpeg", "image/jpeg"],
  [".jpg", "image/jpeg"],
  [".js", "text/javascript; charset=utf-8"],
  [".json", "application/json; charset=utf-8"],
  [".map", "application/json; charset=utf-8"],
  [".png", "image/png"],
  [".svg", "image/svg+xml"],
  [".webp", "image/webp"],
  [".woff", "font/woff"],
  [".woff2", "font/woff2"],
]);

function sendJson(response, status, value, headers = {}) {
  const body = JSON.stringify(value);
  response.writeHead(status, {
    "cache-control": "no-store",
    "content-length": Buffer.byteLength(body),
    "content-type": "application/json; charset=utf-8",
    ...headers,
  });
  response.end(body);
}

function sendEmpty(response, status, headers = {}) {
  response.writeHead(status, { "cache-control": "no-store", ...headers });
  response.end();
}

function sendHtml(response, status, body, headers = {}) {
  response.writeHead(status, {
    "cache-control": "no-store",
    "content-length": Buffer.byteLength(body),
    "content-security-policy": "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'",
    "content-type": "text/html; charset=utf-8",
    ...headers,
  });
  response.end(body);
}

function toFetchRequest(request) {
  const headers = new Headers();
  for (const [name, value] of Object.entries(request.headers)) {
    if (Array.isArray(value)) {
      for (const entry of value) headers.append(name, entry);
    } else if (value !== undefined) {
      headers.set(name, value);
    }
  }
  const init = { method: request.method, headers };
  if (request.method !== "GET" && request.method !== "HEAD") {
    init.body = Readable.toWeb(request);
    init.duplex = "half";
  }
  return new Request(`http://127.0.0.1${request.url}`, init);
}

async function sendFetchResponse(response, upstream) {
  response.statusCode = upstream.status;
  response.statusMessage = upstream.statusText;
  for (const [name, value] of upstream.headers) {
    if (
      name === "connection"
      || name === "content-encoding"
      || name === "content-length"
      || name === "set-cookie"
      || name === "transfer-encoding"
    ) {
      continue;
    }
    response.setHeader(name, value);
  }
  const cookies = upstream.headers.getSetCookie?.() ?? [];
  if (cookies.length > 0) response.setHeader("set-cookie", cookies);
  if (!upstream.body) {
    response.end();
    return;
  }
  await new Promise((resolve, reject) => {
    const body = Readable.fromWeb(upstream.body);
    body.once("error", reject);
    response.once("finish", resolve);
    body.pipe(response);
  });
}

function normalizeHostname(hostname) {
  return hostname.toLowerCase().replace(/^\[|\]$/g, "");
}

function isTrustedNetworkHost(hostname) {
  const host = normalizeHostname(hostname);
  if (host === "localhost" || host === "::1" || host.endsWith(".local")) return true;
  if (isIP(host) === 4) {
    const octets = host.split(".").map(Number);
    return octets[0] === 127
      || octets[0] === 10
      || (octets[0] === 172 && octets[1] >= 16 && octets[1] <= 31)
      || (octets[0] === 192 && octets[1] === 168)
      || (octets[0] === 169 && octets[1] === 254);
  }
  if (isIP(host) === 6) {
    return host.startsWith("fc")
      || host.startsWith("fd")
      || /^fe[89ab]/.test(host);
  }
  return false;
}

function parseTrustedOrigins(value) {
  if (value === undefined) return new Set();
  const configured = String(value).trim();
  if (!configured) {
    throw new Error(`${TRUSTED_ORIGINS_ENV} must not be empty when configured`);
  }

  const origins = new Set();
  for (const rawOrigin of configured.split(",")) {
    const origin = rawOrigin.trim();
    if (!origin || origin.includes("*")) {
      throw new Error(`${TRUSTED_ORIGINS_ENV} must be a comma-separated list of exact HTTPS origins`);
    }
    let url;
    try {
      url = new URL(origin);
    } catch {
      throw new Error(`${TRUSTED_ORIGINS_ENV} must contain valid HTTPS origins`);
    }
    if (
      url.protocol !== "https:"
      || url.username
      || url.password
      || url.pathname !== "/"
      || url.search
      || url.hash
    ) {
      throw new Error(`${TRUSTED_ORIGINS_ENV} must contain exact HTTPS origins without paths, queries, fragments, or credentials`);
    }
    if (origins.has(url.origin)) {
      throw new Error(`${TRUSTED_ORIGINS_ENV} must not contain duplicate origins`);
    }
    origins.add(url.origin);
  }
  return origins;
}

function parseRequestHost(value) {
  if (typeof value !== "string" || !value || value !== value.trim()) {
    throw new ApiError(403, "INVALID_HOST", "Request Host must be local, private, or explicitly trusted");
  }
  let url;
  try {
    url = new URL(`https://${value}`);
  } catch {
    throw new ApiError(403, "INVALID_HOST", "Request Host must be local, private, or explicitly trusted");
  }
  if (
    url.username
    || url.password
    || url.pathname !== "/"
    || url.search
    || url.hash
    || !url.hostname
  ) {
    throw new ApiError(403, "INVALID_HOST", "Request Host must be local, private, or explicitly trusted");
  }
  return { hostname: url.hostname, httpsOrigin: url.origin };
}

function assertTrustedNetworkRequest(request, allowOpaqueOrigin = false, trustedOrigins = new Set()) {
  const host = parseRequestHost(request.headers.host);
  const trustedNetworkHost = isTrustedNetworkHost(host.hostname);
  const configuredTrustedHost = !trustedNetworkHost && trustedOrigins.has(host.httpsOrigin);
  if (!trustedNetworkHost && !configuredTrustedHost) {
    throw new ApiError(403, "INVALID_HOST", "Request Host must be local, private, or explicitly trusted");
  }

  const origin = request.headers.origin;
  const configuredTrustedOrigin = trustedOrigins.has(origin);
  if (origin && !configuredTrustedOrigin && !TRUSTED_EMBED_ORIGINS.has(origin)) {
    if (!(allowOpaqueOrigin && origin === "null")) {
      let originHost;
      try {
        originHost = new URL(origin).hostname;
      } catch {
        throw new ApiError(403, "INVALID_ORIGIN", "Request Origin must be local or private");
      }
      if (!isTrustedNetworkHost(originHost)) {
        throw new ApiError(403, "INVALID_ORIGIN", "Request Origin must be local or private");
      }
    }
  }
  return configuredTrustedHost || configuredTrustedOrigin;
}

function assertLoopbackRequest(request) {
  const address = request.socket.remoteAddress;
  if (
    address !== "127.0.0.1"
    && address !== "::1"
    && address !== "::ffff:127.0.0.1"
  ) {
    throw new ApiError(403, "LOCAL_ONLY", "This endpoint is only available on this device");
  }
}

function assertPlainObject(value) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new ApiError(400, "INVALID_BODY", "Request body must be a JSON object");
  }
}

function assertAllowedKeys(value, allowed) {
  const unknown = Object.keys(value).filter((key) => !allowed.has(key));
  if (unknown.length > 0) {
    throw new ApiError(400, "UNKNOWN_FIELD", `Unknown field${unknown.length === 1 ? "" : "s"}: ${unknown.join(", ")}`);
  }
}

function assertAllowedQuery(searchParams, allowed, routeLabel) {
  for (const key of searchParams.keys()) {
    if (!allowed.has(key)) {
      throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", `${routeLabel} does not accept query parameter '${key}'`);
    }
    if (searchParams.getAll(key).length !== 1) {
      throw new ApiError(400, "INVALID_QUERY_PARAMETER", `Query parameter '${key}' cannot be repeated`);
    }
  }
}

function assertNoQuery(searchParams, routeLabel) {
  assertAllowedQuery(searchParams, new Set(), routeLabel);
}

function parseAfterCursor(searchParams, routeLabel) {
  assertAllowedQuery(searchParams, new Set(["after"]), routeLabel);
  const value = searchParams.get("after");
  if (value === null) return null;
  const revision = Number(value);
  if (!/^\d+$/.test(value) || !Number.isSafeInteger(revision)) {
    throw new ApiError(400, "INVALID_CURSOR", "Cursor must be a non-negative integer revision");
  }
  return { value, revision };
}

function nextCursor(items, after) {
  if (items.length === 0) return after?.value ?? "0";
  return String(items.reduce(
    (revision, item) => Math.max(revision, item.changeRevision),
    0,
  ));
}

function decodeRouteSegment(value, name) {
  let decoded;
  try {
    decoded = decodeURIComponent(value);
  } catch {
    throw new ApiError(400, "INVALID_PATH", `${name} contains invalid encoding`);
  }
  if (!decoded || decoded.length > 256 || decoded.includes("\0")) {
    throw new ApiError(400, "INVALID_PATH", `${name} is invalid`);
  }
  return decoded;
}

function isLoopbackAddress(value) {
  if (typeof value !== "string") return false;
  const address = value.toLowerCase().split("%", 1)[0];
  return address === "::1"
    || address === "127.0.0.1"
    || address.startsWith("127.")
    || address === "::ffff:127.0.0.1"
    || address.startsWith("::ffff:127.");
}

function assertAiLoopbackRequest(request) {
  if (!isLoopbackAddress(request.socket.remoteAddress)) {
    throw new ApiError(403, "LOCAL_AI_LOOPBACK_REQUIRED", "Local AI routes are only available from this device");
  }
}

function stringField(value, name, { required = false, nullable = false, maxLength }) {
  if (value === undefined) {
    if (required) {
      throw new ApiError(400, "INVALID_FIELD", `'${name}' is required`);
    }
    return undefined;
  }
  if (nullable && value === null) {
    return null;
  }
  if (typeof value !== "string") {
    throw new ApiError(400, "INVALID_FIELD", `'${name}' must be a string${nullable ? " or null" : ""}`);
  }
  const normalized = value.trim();
  if (required && normalized.length === 0) {
    throw new ApiError(400, "INVALID_FIELD", `'${name}' cannot be empty`);
  }
  if (normalized.length > maxLength) {
    throw new ApiError(400, "INVALID_FIELD", `'${name}' cannot exceed ${maxLength} characters`);
  }
  return normalized;
}

function pathField(value, name) {
  const normalized = stringField(value, name, { nullable: true, maxLength: 4096 });
  if (normalized === "") {
    throw new ApiError(400, "INVALID_FIELD", `'${name}' cannot be empty`);
  }
  if (normalized?.includes("\0")) {
    throw new ApiError(400, "INVALID_FIELD", `'${name}' cannot contain null bytes`);
  }
  return normalized;
}

function parseDueDate(value, name = "dueDate") {
  const date = stringField(value, name, { nullable: true, maxLength: 10 });
  if (date !== null && date !== undefined && !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    throw new ApiError(400, "INVALID_FIELD", `'${name}' must use YYYY-MM-DD`);
  }
  return date;
}

function parseDevelopmentContext(value) {
  if (value === null) return null;
  assertPlainObject(value);
  if (value.type === "branch") {
    assertAllowedKeys(value, new Set(["type", "branch"]));
    return {
      type: "branch",
      branch: stringField(value.branch, "developmentContext.branch", { required: true, maxLength: 512 }),
    };
  }
  if (value.type === "worktree") {
    assertAllowedKeys(value, new Set(["type", "path", "branch"]));
    const worktreePath = stringField(value.path, "developmentContext.path", { required: true, maxLength: 4096 });
    if (worktreePath.includes("\0")) {
      throw new ApiError(400, "INVALID_FIELD", "'developmentContext.path' cannot contain null bytes");
    }
    return {
      type: "worktree",
      path: worktreePath,
      branch: stringField(value.branch ?? null, "developmentContext.branch", { nullable: true, maxLength: 512 }),
    };
  }
  throw new ApiError(400, "INVALID_FIELD", "'developmentContext.type' must be branch or worktree");
}

function parseRecurrence(value) {
  if (value === null) return null;
  assertPlainObject(value);
  assertAllowedKeys(value, new Set(["interval", "unit"]));
  if (!Number.isSafeInteger(value.interval) || value.interval < 1 || value.interval > 365) {
    throw new ApiError(400, "INVALID_FIELD", "'recurrence.interval' must be an integer from 1 to 365");
  }
  if (!["day", "week", "month", "year"].includes(value.unit)) {
    throw new ApiError(400, "INVALID_FIELD", "'recurrence.unit' must be day, week, month, or year");
  }
  return { interval: value.interval, unit: value.unit };
}

function parseVersion(value) {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new ApiError(400, "INVALID_FIELD", "'version' must be a positive integer");
  }
  return value;
}

function parseSortOrder(value) {
  if (typeof value !== "number" || !Number.isFinite(value) || Math.abs(value) > 1_000_000_000_000) {
    throw new ApiError(400, "INVALID_FIELD", "'sortOrder' must be a finite number between -1000000000000 and 1000000000000");
  }
  return value;
}

function parseLabels(value) {
  if (!Array.isArray(value) || value.length > 20) {
    throw new ApiError(400, "INVALID_FIELD", "'labels' must be an array with at most 20 entries");
  }
  const labels = value.map((label) => {
    if (typeof label !== "string") {
      throw new ApiError(400, "INVALID_FIELD", "Every label must be a string");
    }
    const normalized = label.trim();
    if (normalized.length === 0 || normalized.length > 64) {
      throw new ApiError(400, "INVALID_FIELD", "Labels must contain 1 to 64 characters");
    }
    return normalized;
  });
  if (new Set(labels).size !== labels.length) {
    throw new ApiError(400, "INVALID_FIELD", "Labels must be unique");
  }
  return labels;
}

function parseStatus(value, fallback) {
  const result = value ?? fallback;
  if (!isTaskStatus(result)) {
    throw new ApiError(400, "INVALID_FIELD", `'status' must be one of: ${TASK_STATUSES.join(", ")}`);
  }
  return result;
}

function parsePriority(value, fallback) {
  const result = value ?? fallback;
  if (!isTaskPriority(result)) {
    throw new ApiError(400, "INVALID_FIELD", "'priority' must be none, urgent, high, medium, or low");
  }
  return result;
}

function slugify(value) {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 64);
}

function validateProjectId(value, { required = true } = {}) {
  const id = stringField(value, "id", { required, maxLength: 64 });
  if (id !== undefined && !PROJECT_ID_PATTERN.test(id)) {
    throw new ApiError(400, "INVALID_FIELD", "'id' must be a lowercase slug containing letters, numbers, or hyphens");
  }
  return id;
}

function parseProjectCreate(body) {
  assertPlainObject(body);
  assertAllowedKeys(body, new Set(["id", "name", "workspacePath"]));
  const name = stringField(body.name, "name", { required: true, maxLength: 120 });
  const id = validateProjectId(body.id ?? slugify(name));
  if (!id) {
    throw new ApiError(400, "INVALID_FIELD", "Project name must contain at least one letter or number when 'id' is omitted");
  }
  const workspacePath = stringField(body.workspacePath ?? null, "workspacePath", { nullable: true, maxLength: 4096 });
  if (workspacePath === "") {
    throw new ApiError(400, "INVALID_FIELD", "'workspacePath' cannot be empty");
  }
  if (workspacePath?.includes("\0")) {
    throw new ApiError(400, "INVALID_FIELD", "'workspacePath' cannot contain null bytes");
  }
  return { id, name, workspacePath };
}

function parseProjectLabel(body) {
  assertPlainObject(body);
  assertAllowedKeys(body, new Set(["label"]));
  return stringField(body.label, "label", { required: true, maxLength: 64 });
}

function parseProjectReadmeSave(body) {
  assertPlainObject(body);
  assertAllowedKeys(body, new Set(["content", "version"]));
  const content = body.content ?? "";
  if (typeof content !== "string") {
    throw new ApiError(400, "INVALID_FIELD", "'content' must be a string");
  }
  if (content.length > 500_000) {
    throw new ApiError(400, "INVALID_FIELD", "'content' cannot exceed 500000 characters");
  }
  const version = body.version;
  if (version !== undefined && (!Number.isSafeInteger(version) || version < 0)) {
    throw new ApiError(400, "INVALID_FIELD", "'version' must be a non-negative integer");
  }
  return { content, version };
}

function parseThreadId(value) {
  if (value === undefined) return undefined;
  return stringField(value, "threadId", { required: true, maxLength: 256 });
}

function parseThreadBinding(value) {
  if (value === undefined || value === null) return value;
  assertPlainObject(value);
  assertAllowedKeys(value, new Set([
    "threadId",
    "codexProjectId",
    "codexProjectKind",
    "codexHostId",
    "workspacePath",
  ]));
  const threadId = stringField(value.threadId, "threadBinding.threadId", {
    required: true,
    maxLength: 256,
  });
  const identityFields = [
    value.codexProjectId,
    value.codexProjectKind,
    value.codexHostId,
    value.workspacePath,
  ];
  if (identityFields.every((field) => field === undefined)) return { threadId };
  if (identityFields.some((field) => field === undefined)) {
    throw new ApiError(400, "INVALID_FIELD", "Thread identity must include project, kind, host, and workspace");
  }
  const codexProjectId = stringField(value.codexProjectId, "threadBinding.codexProjectId", {
    required: true,
    maxLength: 256,
  });
  const codexProjectKind = value.codexProjectKind;
  const codexHostId = stringField(value.codexHostId, "threadBinding.codexHostId", {
    required: true,
    maxLength: 256,
  });
  const workspacePath = stringField(value.workspacePath, "threadBinding.workspacePath", {
    required: true,
    maxLength: 4096,
  });
  if (codexProjectKind !== "local" && codexProjectKind !== "remote") {
    throw new ApiError(400, "INVALID_FIELD", "threadBinding.codexProjectKind must be local or remote");
  }
  if (
    (codexProjectKind === "local" && codexHostId !== "local")
    || (codexProjectKind === "remote" && codexHostId === "local")
    || workspacePath.includes("\0")
  ) {
    throw new ApiError(400, "INVALID_FIELD", "Thread project identity is invalid");
  }
  return { threadId, codexProjectId, codexProjectKind, codexHostId, workspacePath };
}

function requestHeader(request, name) {
  const value = request.headers[name];
  return Array.isArray(value) ? value[0] : value;
}

function actorFromRequest(request) {
  if (request.taskboardActor) return request.taskboardActor;
  if (request.headers["x-taskboard-client"] === "taskctl") {
    return CODEX_AGENT_ACTOR;
  }

  const rawId = requestHeader(request, "x-taskboard-user-id");
  const rawName = requestHeader(request, "x-taskboard-user-name");
  const rawAvatarUrl = requestHeader(request, "x-taskboard-user-avatar");
  if (rawId === undefined && rawName === undefined && rawAvatarUrl === undefined) {
    return { type: "user", id: "local-user", name: "本地用户", avatarUrl: null };
  }
  if (rawId === undefined || rawName === undefined) {
    throw new ApiError(400, "INVALID_ACTOR", "User identity requires both an ID and name");
  }

  const id = stringField(rawId, "X-Taskboard-User-Id", { required: true, maxLength: 96 });
  if (!/^[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?$/.test(id)) {
    throw new ApiError(400, "INVALID_ACTOR", "User ID contains unsupported characters");
  }
  let decodedName;
  try {
    decodedName = decodeURIComponent(rawName);
  } catch {
    throw new ApiError(400, "INVALID_ACTOR", "User name is not valid URL-encoded text");
  }
  const name = stringField(decodedName, "X-Taskboard-User-Name", { required: true, maxLength: 120 });

  let avatarUrl = null;
  if (rawAvatarUrl !== undefined) {
    const value = stringField(rawAvatarUrl, "X-Taskboard-User-Avatar", { required: true, maxLength: 2048 });
    let parsed;
    try {
      parsed = new URL(value);
    } catch {
      throw new ApiError(400, "INVALID_ACTOR", "User avatar URL is invalid");
    }
    if (!["http:", "https:"].includes(parsed.protocol)) {
      throw new ApiError(400, "INVALID_ACTOR", "User avatar URL must use HTTP or HTTPS");
    }
    avatarUrl = parsed.toString();
  }
  return { type: "user", id, name, avatarUrl };
}

function decodeBasicCredentials(header) {
  if (typeof header !== "string" || !header.startsWith("Basic ")) return null;
  let value;
  try {
    value = Buffer.from(header.slice(6).trim(), "base64").toString("utf8");
  } catch {
    return null;
  }
  const separator = value.indexOf(":");
  if (separator < 1) return null;
  return { username: value.slice(0, separator), password: value.slice(separator + 1) };
}

function publicPasswordDigest(value) {
  return createHash("sha256").update(value).digest();
}

function parseCookies(request) {
  const cookies = new Map();
  for (const part of String(requestHeader(request, "cookie") ?? "").split(";")) {
    const separator = part.indexOf("=");
    if (separator < 1) continue;
    const name = part.slice(0, separator).trim();
    const value = part.slice(separator + 1).trim();
    if (name) cookies.set(name, value);
  }
  return cookies;
}

function appendSetCookie(response, value) {
  const current = response.getHeader("set-cookie");
  if (current === undefined) response.setHeader("set-cookie", value);
  else response.setHeader("set-cookie", Array.isArray(current) ? [...current, value] : [current, value]);
}

function publicCookie(request, name, value, { maxAge, httpOnly = true } = {}) {
  const forwardedProto = String(requestHeader(request, "x-forwarded-proto") ?? "")
    .split(",", 1)[0]
    .trim()
    .toLowerCase();
  const secure = forwardedProto === "https" || request.socket?.encrypted === true;
  return [
    `${name}=${value}`,
    "Path=/",
    `Max-Age=${maxAge}`,
    "SameSite=Lax",
    httpOnly ? "HttpOnly" : null,
    secure ? "Secure" : null,
  ].filter(Boolean).join("; ");
}

function publicSessionSecret({ sharedSecret, users }) {
  const hash = createHash("sha256").update("codex-taskboard-public-session-v1\0");
  hash.update(sharedSecret);
  for (const user of [...users].sort((left, right) => left.login.localeCompare(right.login))) {
    hash.update("\0").update(user.login).update("\0").update(user.passwordDigest);
  }
  return hash.digest();
}

function createPublicSessionToken(identity, authConfig, now = Date.now()) {
  const payload = Buffer.from(JSON.stringify({ login: identity.login, exp: now + PUBLIC_SESSION_TTL_MS }))
    .toString("base64url");
  const signature = createHmac("sha256", publicSessionSecret(authConfig))
    .update(payload)
    .digest("base64url");
  return `${payload}.${signature}`;
}

function identityForPublicLogin(login, users) {
  const normalizedLogin = login.trim().toLocaleLowerCase("en-US");
  const configuredUser = users.length > 0
    ? users.find((candidate) => candidate.login === normalizedLogin)
    : null;
  if (users.length > 0 && !configuredUser) return null;
  const name = stringField(configuredUser?.name ?? login, "Public username", {
    required: true,
    maxLength: 120,
  });
  return {
    login: normalizedLogin,
    actor: {
      type: "user",
      id: `basic:${encodeURIComponent(normalizedLogin)}`,
      name,
      avatarUrl: null,
    },
    role: configuredUser?.role ?? "product",
  };
}

function readPublicSession(request, authConfig) {
  const token = parseCookies(request).get(PUBLIC_SESSION_COOKIE);
  if (!token) return null;
  const separator = token.lastIndexOf(".");
  if (separator < 1) return null;
  const payload = token.slice(0, separator);
  const provided = Buffer.from(token.slice(separator + 1), "base64url");
  const expected = createHmac("sha256", publicSessionSecret(authConfig)).update(payload).digest();
  if (provided.length !== expected.length || !timingSafeEqual(provided, expected)) return null;
  let parsed;
  try {
    parsed = JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
  } catch {
    return null;
  }
  if (!parsed || typeof parsed.login !== "string" || !Number.isFinite(parsed.exp) || parsed.exp <= Date.now()) {
    return null;
  }
  return identityForPublicLogin(parsed.login, authConfig.users);
}

function validatePublicCredentials(credentials, { sharedSecret, users }) {
  if (!credentials) return null;
  const normalizedLogin = credentials.username.trim().toLocaleLowerCase("en-US");
  const configuredUser = users.length > 0
    ? users.find((candidate) => candidate.login === normalizedLogin)
    : null;
  const expected = configuredUser?.passwordDigest
    ?? (users.length === 0 && sharedSecret ? publicPasswordDigest(sharedSecret) : null);
  const provided = publicPasswordDigest(credentials.password);
  if (!expected || !timingSafeEqual(provided, expected)) return null;
  return identityForPublicLogin(credentials.username, users);
}

function parsePublicUsers(value) {
  const source = String(value ?? "").trim();
  if (!source) return [];
  let parsed;
  try {
    parsed = JSON.parse(source);
  } catch {
    throw new Error(`${PUBLIC_USERS_ENV} must be valid JSON`);
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`${PUBLIC_USERS_ENV} must be a JSON object keyed by login name`);
  }
  const users = [];
  const normalizedLogins = new Set();
  for (const [rawLogin, config] of Object.entries(parsed)) {
    const login = rawLogin.trim();
    const normalizedLogin = login.toLocaleLowerCase("en-US");
    if (!login || login.length > 120 || normalizedLogins.has(normalizedLogin)) {
      throw new Error(`${PUBLIC_USERS_ENV} contains an invalid or duplicate login`);
    }
    if (!config || typeof config !== "object" || Array.isArray(config)) {
      throw new Error(`${PUBLIC_USERS_ENV}.${login} must be an object`);
    }
    const password = typeof config.password === "string" ? config.password : "";
    const role = typeof config.role === "string" ? config.role : "";
    const name = typeof config.name === "string" ? config.name.trim() : login;
    if (password.length < 16) {
      throw new Error(`${PUBLIC_USERS_ENV}.${login}.password must contain at least 16 characters`);
    }
    if (!PUBLIC_ROLES.has(role)) {
      throw new Error(`${PUBLIC_USERS_ENV}.${login}.role must be product, technical, or admin`);
    }
    if (!name || name.length > 120) {
      throw new Error(`${PUBLIC_USERS_ENV}.${login}.name must contain 1 to 120 characters`);
    }
    normalizedLogins.add(normalizedLogin);
    users.push({
      login: normalizedLogin,
      name,
      role,
      passwordDigest: publicPasswordDigest(password),
    });
  }
  if (users.length === 0) {
    throw new Error(`${PUBLIC_USERS_ENV} must contain at least one user`);
  }
  return users;
}

function authenticatePublicRequest(request, { sharedSecret, users }) {
  const credentials = decodeBasicCredentials(requestHeader(request, "authorization"));
  const identity = validatePublicCredentials(credentials, { sharedSecret, users });
  if (!identity) {
    throw new ApiError(401, "UNAUTHORIZED", "Valid Basic credentials are required");
  }
  if (request.headers["x-taskboard-client"] === "taskctl") identity.actor.type = "agent";
  return identity;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function publicLoginPage({ username = "", invalid = false } = {}) {
  const error = invalid
    ? '<p class="error" role="alert">用户名或密码不正确，请重新输入。</p>'
    : "";
  return `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>SkillHub 登录</title>
  <style>
    * { box-sizing: border-box; }
    body { margin: 0; min-height: 100vh; color: #202124; background: #f7f8fa; font: 14px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    header { height: 68px; display: flex; align-items: center; padding: 0 30px; border-bottom: 1px solid #e5e7eb; background: #fff; font-size: 18px; font-weight: 650; }
    main { min-height: calc(100vh - 68px); display: grid; place-items: center; padding: 32px 20px; }
    form { width: min(100%, 360px); }
    h1 { margin: 0 0 8px; font-size: 24px; font-weight: 650; }
    .hint { margin: 0 0 28px; color: #6b7280; }
    label { display: block; margin: 0 0 16px; color: #4b5563; font-weight: 600; }
    input { width: 100%; height: 44px; margin-top: 7px; padding: 0 12px; border: 1px solid #cfd4dc; border-radius: 6px; background: #fff; color: #202124; font: inherit; outline: none; }
    input:focus { border-color: #2563eb; box-shadow: 0 0 0 3px rgba(37, 99, 235, .12); }
    button { width: 100%; height: 44px; margin-top: 4px; border: 0; border-radius: 6px; background: #202124; color: #fff; font: inherit; font-weight: 650; cursor: pointer; }
    button:hover { background: #111827; }
    .error { margin: -4px 0 16px; color: #c2413a; }
  </style>
</head>
<body>
  <header>SkillHub</header>
  <main>
    <form method="post" autocomplete="on">
      <h1>登录 Taskboard</h1>
      <p class="hint">使用分配给你的协作账户继续。</p>
      ${error}
      <label>用户名<input name="username" value="${escapeHtml(username)}" autocomplete="username" required autofocus></label>
      <label>密码<input name="password" type="password" autocomplete="current-password" required></label>
      <button type="submit">登录</button>
    </form>
  </main>
</body>
</html>`;
}

function publicProductRequestCanWrite(method, pathname) {
  if (method === "GET" || method === "HEAD") return true;
  if (method === "POST" && pathname === "/api/product-sessions") return true;
  if (method === "POST" && /^\/api\/product-sessions\/[^/]+\/(?:turns|approve)$/.test(pathname)) return true;
  if (method === "POST" && /^\/api\/product-sessions\/[^/]+\/acceptance$/.test(pathname)) return true;
  if (method === "PATCH" && /^\/api\/product-sessions\/[^/]+\/product-agent-settings$/.test(pathname)) return true;
  if (method === "PUT" && /^\/api\/product-sessions\/[^/]+\/document$/.test(pathname)) return true;
  return false;
}

function publicTechnicalRequestCanWrite(method, pathname) {
  if (method === "GET" || method === "HEAD") return true;
  if (method === "POST" && /^\/api\/product-sessions\/[^/]+\/(?:technical-turns|technical-approve|delivery|submit-review)$/.test(pathname)) return true;
  if (method === "PATCH" && /^\/api\/product-sessions\/[^/]+\/technical-agent-settings$/.test(pathname)) return true;
  if (method === "PUT" && /^\/api\/product-sessions\/[^/]+\/technical-document$/.test(pathname)) return true;
  return pathname === "/api/tasks"
    || pathname.startsWith("/api/tasks/")
    || pathname.startsWith("/api/attachments/");
}

function publicRoleCanWrite(role, method, pathname) {
  if (role === "admin") return true;
  if (role === "technical") return publicTechnicalRequestCanWrite(method, pathname);
  return publicProductRequestCanWrite(method, pathname);
}

function parseAssigneeTarget(value) {
  if (value === undefined) return undefined;
  if (value !== "current-user" && value !== "codex-agent") {
    throw new ApiError(400, "INVALID_FIELD", "'assigneeTarget' must be current-user or codex-agent");
  }
  return value;
}

function resolveAssignee(target, actor) {
  if (target === undefined) return actor;
  if (target === "codex-agent") return CODEX_AGENT_ACTOR;
  if (actor.type !== "user") {
    throw new ApiError(400, "INVALID_FIELD", "'current-user' requires a user request identity");
  }
  return actor;
}

function parseTaskCreate(body) {
  assertPlainObject(body);
  assertAllowedKeys(body, new Set([
    "projectId", "title", "description", "status", "priority", "labels", "sortOrder", "threadId", "threadBinding",
    "assigneeTarget", "developmentContext", "startDate", "dueDate", "recurrence",
  ]));
  const projectId = validateProjectId(body.projectId ?? DEFAULT_PROJECT_ID);
  const task = {
    projectId,
    title: stringField(body.title, "title", { required: true, maxLength: 240 }),
    description: stringField(body.description ?? "", "description", { maxLength: 100_000 }),
    status: parseStatus(body.status, "backlog"),
    priority: parsePriority(body.priority, "none"),
    labels: body.labels === undefined ? [] : parseLabels(body.labels),
    sortOrder: body.sortOrder === undefined ? undefined : parseSortOrder(body.sortOrder),
    threadId: parseThreadId(body.threadId),
    threadBinding: parseThreadBinding(body.threadBinding),
    assigneeTarget: parseAssigneeTarget(body.assigneeTarget),
    developmentContext: parseDevelopmentContext(body.developmentContext ?? null),
    startDate: parseDueDate(body.startDate ?? null, "startDate"),
    dueDate: parseDueDate(body.dueDate ?? null),
    recurrence: parseRecurrence(body.recurrence ?? null),
  };
  if (task.recurrence && !task.dueDate) {
    throw new ApiError(400, "INVALID_FIELD", "A recurring issue requires 'dueDate'");
  }
  return task;
}

function parseTaskPatch(body) {
  assertPlainObject(body);
  assertAllowedKeys(body, new Set([
    "version", "projectId", "title", "description", "status", "priority", "labels", "threadId", "threadBinding",
    "assigneeTarget", "developmentContext", "startDate", "dueDate", "recurrence",
  ]));
  const version = parseVersion(body.version);
  const threadId = parseThreadId(body.threadId);
  const threadBinding = parseThreadBinding(body.threadBinding);
  const assigneeTarget = parseAssigneeTarget(body.assigneeTarget);
  const changes = {};
  if (body.projectId !== undefined) changes.projectId = validateProjectId(body.projectId);
  if (body.title !== undefined) changes.title = stringField(body.title, "title", { required: true, maxLength: 240 });
  if (body.description !== undefined) changes.description = stringField(body.description, "description", { maxLength: 100_000 });
  if (body.status !== undefined) changes.status = parseStatus(body.status);
  if (body.priority !== undefined) changes.priority = parsePriority(body.priority);
  if (body.labels !== undefined) changes.labels = parseLabels(body.labels);
  if (body.developmentContext !== undefined) changes.developmentContext = parseDevelopmentContext(body.developmentContext);
  if (body.startDate !== undefined) changes.startDate = parseDueDate(body.startDate, "startDate");
  if (body.dueDate !== undefined) changes.dueDate = parseDueDate(body.dueDate);
  if (body.recurrence !== undefined) changes.recurrence = parseRecurrence(body.recurrence);
  if (changes.recurrence && body.dueDate === null) {
    throw new ApiError(400, "INVALID_FIELD", "A recurring issue requires 'dueDate'");
  }
  if (Object.keys(changes).length === 0 && assigneeTarget === undefined) {
    throw new ApiError(400, "INVALID_BODY", "PATCH requires at least one task field");
  }
  return { version, changes, threadId, threadBinding, assigneeTarget };
}

function parseMove(body) {
  assertPlainObject(body);
  assertAllowedKeys(body, new Set(["version", "status", "sortOrder", "threadId", "threadBinding"]));
  return {
    version: parseVersion(body.version),
    status: parseStatus(body.status),
    sortOrder: body.sortOrder === undefined ? undefined : parseSortOrder(body.sortOrder),
    threadId: parseThreadId(body.threadId),
    threadBinding: parseThreadBinding(body.threadBinding),
  };
}

function parseArchive(body) {
  assertPlainObject(body);
  assertAllowedKeys(body, new Set(["version", "threadId", "threadBinding"]));
  return {
    version: parseVersion(body.version),
    threadId: parseThreadId(body.threadId),
    threadBinding: parseThreadBinding(body.threadBinding),
  };
}

function parseRelationOrigin(value) {
  if (value === undefined) return undefined;
  if (value !== "manual" && value !== "mention") {
    throw new ApiError(400, "INVALID_FIELD", "'origin' must be manual or mention");
  }
  return value;
}

function parseRelationMutation(body) {
  assertPlainObject(body);
  assertAllowedKeys(body, new Set(["version", "threadId", "threadBinding", "origin"]));
  return {
    version: parseVersion(body.version),
    threadId: parseThreadId(body.threadId),
    threadBinding: parseThreadBinding(body.threadBinding),
    origin: parseRelationOrigin(body.origin),
  };
}

function parseIssueRelationType(value) {
  if (!["parent", "blocks", "blocked_by", "related"].includes(value)) {
    throw new ApiError(
      400,
      "INVALID_FIELD",
      "'relation type' must be parent, blocks, blocked_by, or related",
    );
  }
  return value;
}

function parseCommentCreate(body) {
  assertPlainObject(body);
  assertAllowedKeys(body, new Set(["body", "threadId", "threadBinding"]));
  return {
    body: stringField(body.body ?? "", "body", { maxLength: 100_000 }),
    threadId: parseThreadId(body.threadId),
    threadBinding: parseThreadBinding(body.threadBinding),
  };
}

function parseCommentPatch(body) {
  assertPlainObject(body);
  assertAllowedKeys(body, new Set(["version", "body", "threadId", "threadBinding"]));
  if (body.body === undefined) {
    throw new ApiError(400, "INVALID_FIELD", "'body' is required");
  }
  return {
    version: parseVersion(body.version),
    body: stringField(body.body, "body", { maxLength: 100_000 }),
    threadId: parseThreadId(body.threadId),
    threadBinding: parseThreadBinding(body.threadBinding),
  };
}

function parseAttachmentHeaders(request) {
  const encodedFilename = request.headers["x-taskboard-filename"];
  if (typeof encodedFilename !== "string") {
    throw new ApiError(400, "INVALID_FILENAME", "X-Taskboard-Filename is required");
  }
  let filename;
  try {
    filename = decodeURIComponent(encodedFilename).trim();
  } catch {
    throw new ApiError(400, "INVALID_FILENAME", "Attachment filename contains invalid encoding");
  }
  if (
    filename.length === 0
    || filename.length > 240
    || filename === "."
    || filename === ".."
    || /[\u0000-\u001f\u007f/\\]/.test(filename)
  ) {
    throw new ApiError(400, "INVALID_FILENAME", "Attachment filename is invalid");
  }

  const rawContentType = request.headers["content-type"];
  const contentType = typeof rawContentType === "string"
    ? rawContentType.split(";", 1)[0].trim().toLowerCase()
    : "application/octet-stream";
  if (contentType.length === 0 || contentType.length > 200 || !/^[!#$%&'*+.^_`|~0-9a-z-]+\/[!#$%&'*+.^_`|~0-9a-z-]+$/.test(contentType)) {
    throw new ApiError(415, "UNSUPPORTED_MEDIA_TYPE", "Attachment Content-Type is invalid");
  }
  const kind = request.headers["x-taskboard-attachment-kind"];
  if (kind !== "inline" && kind !== "attachment") {
    throw new ApiError(
      400,
      "INVALID_ATTACHMENT_KIND",
      "X-Taskboard-Attachment-Kind must be inline or attachment",
    );
  }
  return { filename, contentType, kind };
}

async function readBody(request, limit, tooLargeMessage) {
  const declaredLength = Number(request.headers["content-length"] ?? 0);
  if (Number.isFinite(declaredLength) && declaredLength > limit) {
    throw new ApiError(413, "BODY_TOO_LARGE", tooLargeMessage);
  }

  const chunks = [];
  let length = 0;
  for await (const chunk of request) {
    length += chunk.length;
    if (length > limit) {
      throw new ApiError(413, "BODY_TOO_LARGE", tooLargeMessage);
    }
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

async function readJson(
  request,
  limit = JSON_BODY_LIMIT,
  tooLargeMessage = "Request body cannot exceed 1 MiB",
) {
  const contentType = request.headers["content-type"]?.split(";", 1)[0].trim().toLowerCase();
  if (contentType !== "application/json") {
    throw new ApiError(415, "UNSUPPORTED_MEDIA_TYPE", "Content-Type must be application/json");
  }
  const body = await readBody(request, limit, tooLargeMessage);
  const length = body.length;
  if (length === 0) {
    throw new ApiError(400, "INVALID_JSON", "Request body cannot be empty");
  }
  try {
    return JSON.parse(body.toString("utf8"));
  } catch {
    throw new ApiError(400, "INVALID_JSON", "Request body must contain valid JSON");
  }
}

async function assertEmptyRequestBody(request, routeLabel) {
  const body = await readBody(request, JSON_BODY_LIMIT, "Request body cannot exceed 1 MiB");
  if (body.length > 0) {
    throw new ApiError(400, "INVALID_BODY", `${routeLabel} does not accept a request body`);
  }
}

function parseTaskFilters(searchParams) {
  const allowed = new Set(["projectId", "status", "archived"]);
  for (const key of searchParams.keys()) {
    if (!allowed.has(key)) {
      throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", `Unknown query parameter '${key}'`);
    }
    if (searchParams.getAll(key).length !== 1) {
      throw new ApiError(400, "INVALID_QUERY_PARAMETER", `Query parameter '${key}' cannot be repeated`);
    }
  }

  const projectIdValue = searchParams.get("projectId");
  const statusValue = searchParams.get("status");
  const archived = searchParams.get("archived") ?? "false";
  if (statusValue !== null && !isTaskStatus(statusValue)) {
    throw new ApiError(400, "INVALID_QUERY_PARAMETER", "Invalid task status");
  }
  if (!new Set(["true", "false", "all"]).has(archived)) {
    throw new ApiError(400, "INVALID_QUERY_PARAMETER", "'archived' must be true, false, or all");
  }
  const projectId = projectIdValue === null ? undefined : validateProjectId(projectIdValue);
  return { projectId, status: statusValue ?? undefined, archived };
}

function parseTaskTreeQuery(searchParams) {
  const allowed = new Set(["direction", "depth"]);
  for (const key of searchParams.keys()) {
    if (!allowed.has(key)) {
      throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", `Unknown query parameter '${key}'`);
    }
    if (searchParams.getAll(key).length !== 1) {
      throw new ApiError(400, "INVALID_TREE_QUERY", `Query parameter '${key}' cannot be repeated`);
    }
  }
  const direction = searchParams.get("direction");
  if (direction !== "descendants" && direction !== "ancestors") {
    throw new ApiError(400, "INVALID_TREE_QUERY", "'direction' must be descendants or ancestors");
  }
  const rawDepth = searchParams.get("depth");
  const depth = Number(rawDepth);
  if (!/^\d+$/.test(rawDepth ?? "") || !Number.isSafeInteger(depth) || depth < 1 || depth > 25) {
    throw new ApiError(400, "INVALID_TREE_QUERY", "'depth' must be an integer from 1 to 25");
  }
  return { direction, depth };
}

function parseAiSandbox(value) {
  if (value === undefined) return undefined;
  if (!["read-only", "workspace-write", "danger-full-access"].includes(value)) {
    throw new ApiError(
      400,
      "INVALID_SANDBOX",
      "'sandbox' must be read-only, workspace-write, or danger-full-access",
    );
  }
  return value;
}

function parseAiSetting(value, name, maxLength) {
  const setting = stringField(value, name, { maxLength });
  if (setting === "") {
    throw new ApiError(400, "INVALID_FIELD", `'${name}' cannot be empty`);
  }
  return setting;
}

function parseAiExecutionTarget(value) {
  const fields = [
    "codexProjectId",
    "codexProjectKind",
    "codexHostId",
    "workspacePath",
  ];
  const present = fields.filter((field) => value[field] !== undefined);
  if (present.length === 0) return undefined;
  if (present.length !== fields.length) {
    throw new ApiError(400, "INVALID_CODEX_TARGET", "Codex project identity must contain all four fields");
  }
  const codexProjectKind = parseAiSetting(value.codexProjectKind, "codexProjectKind", 16);
  if (codexProjectKind !== "local" && codexProjectKind !== "remote") {
    throw new ApiError(400, "INVALID_CODEX_TARGET", "'codexProjectKind' must be local or remote");
  }
  const workspacePath = parseAiSetting(value.workspacePath, "workspacePath", 4096);
  if (workspacePath.includes("\0")) {
    throw new ApiError(400, "INVALID_CODEX_TARGET", "'workspacePath' cannot contain null bytes");
  }
  return {
    codexProjectId: parseAiSetting(value.codexProjectId, "codexProjectId", 256),
    codexProjectKind,
    codexHostId: parseAiSetting(value.codexHostId, "codexHostId", 512),
    workspacePath,
  };
}

function aiExecutionTargetFromQuery(searchParams) {
  return parseAiExecutionTarget({
    codexProjectId: searchParams.get("codexProjectId") ?? undefined,
    codexProjectKind: searchParams.get("codexProjectKind") ?? undefined,
    codexHostId: searchParams.get("codexHostId") ?? undefined,
    workspacePath: searchParams.get("workspacePath") ?? undefined,
  });
}

function parseAiThreadCreate(body) {
  assertPlainObject(body);
  assertAllowedKeys(body, new Set([
    "projectId",
    "issueId",
    "title",
    "model",
    "reasoningEffort",
    "sandbox",
    "codexProjectId",
    "codexProjectKind",
    "codexHostId",
    "workspacePath",
  ]));
  return {
    projectId: validateProjectId(body.projectId),
    issueId: parseAiSetting(body.issueId, "issueId", 128),
    title: parseAiSetting(body.title, "title", 160),
    model: parseAiSetting(body.model, "model", 128),
    reasoningEffort: parseAiSetting(body.reasoningEffort, "reasoningEffort", 64),
    sandbox: parseAiSandbox(body.sandbox),
    ...parseAiExecutionTarget(body),
  };
}

function parseAiThreadPatch(body) {
  assertPlainObject(body);
  assertAllowedKeys(body, new Set(["title", "model", "reasoningEffort", "sandbox"]));
  const input = {};
  if (body.title !== undefined) input.title = parseAiSetting(body.title, "title", 160);
  if (body.model !== undefined) input.model = parseAiSetting(body.model, "model", 128);
  if (body.reasoningEffort !== undefined) {
    input.reasoningEffort = parseAiSetting(body.reasoningEffort, "reasoningEffort", 64);
  }
  if (body.sandbox !== undefined) input.sandbox = parseAiSandbox(body.sandbox);
  if (Object.keys(input).length === 0) {
    throw new ApiError(400, "INVALID_BODY", "PATCH requires at least one thread setting");
  }
  return input;
}

function parseAiSkillIds(value) {
  if (value === undefined) return undefined;
  if (!Array.isArray(value) || value.length > 20) {
    throw new ApiError(400, "INVALID_FIELD", "'skillIds' must be an array with at most 20 entries");
  }
  const skillIds = value.map((skillId, index) => (
    stringField(skillId, `skillIds[${index}]`, { required: true, maxLength: 256 })
  ));
  return skillIds;
}

function parseAiAttachments(value) {
  if (value === undefined) return [];
  if (!Array.isArray(value) || value.length > AI_CHAT_ATTACHMENT_LIMIT) {
    throw new ApiError(
      400,
      "INVALID_ATTACHMENT",
      `'attachments' must be an array with at most ${AI_CHAT_ATTACHMENT_LIMIT} files`,
    );
  }
  return value.map((attachment, index) => {
    assertPlainObject(attachment);
    assertAllowedKeys(attachment, new Set(["filename", "contentType", "dataBase64"]));
    const filename = stringField(attachment.filename, `attachments[${index}].filename`, {
      required: true,
      maxLength: 240,
    });
    if (/[\u0000-\u001f\u007f/\\]/.test(filename)) {
      throw new ApiError(
        400,
        "INVALID_ATTACHMENT",
        `'attachments[${index}].filename' is invalid`,
      );
    }
    const contentType = stringField(
      attachment.contentType,
      `attachments[${index}].contentType`,
      { required: true, maxLength: 256 },
    ).toLowerCase();
    const dataBase64 = stringField(
      attachment.dataBase64,
      `attachments[${index}].dataBase64`,
      { required: true, maxLength: AI_CHAT_TURN_BODY_LIMIT },
    );
    if (
      dataBase64.length % 4 !== 0
      || !/^[A-Za-z0-9+/]+={0,2}$/.test(dataBase64)
    ) {
      throw new ApiError(
        400,
        "INVALID_ATTACHMENT",
        `'attachments[${index}].dataBase64' must contain valid base64`,
      );
    }
    const data = Buffer.from(dataBase64, "base64");
    if (data.length === 0 || data.toString("base64") !== dataBase64) {
      throw new ApiError(
        400,
        "INVALID_ATTACHMENT",
        `'attachments[${index}].dataBase64' must contain valid base64`,
      );
    }
    return { filename, contentType, data, size: data.length };
  });
}

function parseAiTurn(body) {
  assertPlainObject(body);
  if (body.contractVersion !== undefined) return parseComposerTurn(body);
  assertAllowedKeys(body, new Set([
    "message",
    "skillIds",
    "dangerFullAccessConfirmed",
    "attachments",
  ]));
  if (
    body.dangerFullAccessConfirmed !== undefined
    && typeof body.dangerFullAccessConfirmed !== "boolean"
  ) {
    throw new ApiError(400, "INVALID_FIELD", "'dangerFullAccessConfirmed' must be a boolean");
  }
  const message = stringField(body.message ?? "", "message", { maxLength: 100_000 });
  const skillIds = parseAiSkillIds(body.skillIds) ?? [];
  if (message.split(AI_CHAT_SKILL_MARKER).length - 1 !== skillIds.length) {
    throw new ApiError(400, "INVALID_FIELD", "'skillIds' must match the Skill markers in 'message'");
  }
  const attachments = parseAiAttachments(body.attachments);
  if (message === "" && attachments.length === 0) {
    throw new ApiError(
      400,
      "INVALID_MESSAGE",
      "A message or at least one attachment is required",
    );
  }
  return {
    message,
    skillIds,
    dangerFullAccessConfirmed: body.dangerFullAccessConfirmed,
    attachments,
  };
}

function parseProductSessionCreate(body) {
  assertPlainObject(body);
  assertAllowedKeys(body, new Set(["projectId", "title", "model", "reasoningEffort"]));
  return {
    projectId: validateProjectId(body.projectId),
    title: stringField(body.title, "title", { required: true, maxLength: 160 }),
    model: parseAiSetting(body.model, "model", 128),
    reasoningEffort: parseAiSetting(body.reasoningEffort, "reasoningEffort", 64),
  };
}

function parseProductAgentSettings(body) {
  assertPlainObject(body);
  assertAllowedKeys(body, new Set(["model", "reasoningEffort"]));
  const input = {};
  if (body.model !== undefined) input.model = parseAiSetting(body.model, "model", 128);
  if (body.reasoningEffort !== undefined) {
    input.reasoningEffort = parseAiSetting(body.reasoningEffort, "reasoningEffort", 64);
  }
  if (Object.keys(input).length === 0) {
    throw new ApiError(400, "INVALID_BODY", "PATCH requires a model or reasoning effort");
  }
  return input;
}

function parseProductSessionList(searchParams) {
  assertAllowedQuery(
    searchParams,
    new Set(["projectId", "page", "pageSize", "query", "status"]),
    "GET /api/product-sessions",
  );
  const parsePageValue = (name, fallback, maximum) => {
    const raw = searchParams.get(name);
    if (raw === null) return fallback;
    const value = Number(raw);
    if (!/^\d+$/.test(raw) || !Number.isSafeInteger(value) || value < 1 || value > maximum) {
      throw new ApiError(400, "INVALID_QUERY_PARAMETER", `'${name}' must be an integer from 1 to ${maximum}`);
    }
    return value;
  };
  const status = searchParams.get("status")?.trim() ?? "";
  if (status && !new Set(["discovery", "draft", "approved", "handed_off"]).has(status)) {
    throw new ApiError(400, "INVALID_QUERY_PARAMETER", "'status' is not a valid product session status");
  }
  return {
    projectId: validateProjectId(searchParams.get("projectId") ?? undefined),
    page: parsePageValue("page", 1, 1_000_000),
    pageSize: parsePageValue("pageSize", 20, 100),
    query: stringField(searchParams.get("query") ?? "", "query", { maxLength: 120 }).trim(),
    status: status || null,
  };
}

function parseProductDocumentUpdate(body) {
  assertPlainObject(body);
  assertAllowedKeys(body, new Set(["content", "version"]));
  if (!Number.isSafeInteger(body.version) || body.version < 0) {
    throw new ApiError(400, "INVALID_FIELD", "'version' must be a non-negative integer");
  }
  return {
    content: stringField(body.content ?? "", "content", { maxLength: 90_000 }),
    version: body.version,
  };
}

function parseProductTurn(body) {
  const turn = parseAiTurn(body);
  if (turn.skillIds.length > 0 || turn.dangerFullAccessConfirmed !== undefined) {
    throw new ApiError(
      400,
      "PRODUCT_TURN_RESTRICTED",
      "Product conversations do not accept Skills or elevated access",
    );
  }
  return turn;
}

function parseDeliverySubmission(body) {
  assertPlainObject(body);
  assertAllowedKeys(body, new Set(["note", "implementationPr", "testDeployment"]));
  const implementationPr = stringField(body.implementationPr, "implementationPr", {
    required: true,
    maxLength: 2_048,
  });
  if (!implementationPr.startsWith("http://") && !implementationPr.startsWith("https://")) {
    throw new ApiError(400, "INVALID_FIELD", "'implementationPr' must be an HTTP(S) URL");
  }
  assertPlainObject(body.testDeployment);
  assertAllowedKeys(body.testDeployment, new Set(["url", "workflowRun", "immutableTag", "prNumbers"]));
  const deploymentUrl = stringField(body.testDeployment.url, "testDeployment.url", {
    required: true,
    maxLength: 2_048,
  });
  const workflowRun = stringField(body.testDeployment.workflowRun, "testDeployment.workflowRun", {
    required: true,
    maxLength: 2_048,
  });
  for (const [name, value] of [["testDeployment.url", deploymentUrl], ["testDeployment.workflowRun", workflowRun]]) {
    if (!value.startsWith("http://") && !value.startsWith("https://")) {
      throw new ApiError(400, "INVALID_FIELD", `'${name}' must be an HTTP(S) URL`);
    }
  }
  const prNumbers = body.testDeployment.prNumbers;
  if (!Array.isArray(prNumbers) || prNumbers.length === 0 || !prNumbers.every(
    (number) => Number.isSafeInteger(number) && number > 0,
  )) {
    throw new ApiError(400, "INVALID_FIELD", "'testDeployment.prNumbers' must contain positive integers");
  }
  return {
    note: stringField(body.note, "note", { required: true, maxLength: 20_000 }),
    implementationPr,
    testDeployment: {
      url: deploymentUrl,
      workflowRun,
      immutableTag: stringField(body.testDeployment.immutableTag, "testDeployment.immutableTag", {
        required: true,
        maxLength: 512,
      }),
      prNumbers: [...new Set(prNumbers)],
    },
  };
}

function parseProductAcceptance(body) {
  assertPlainObject(body);
  assertAllowedKeys(body, new Set(["outcome", "note"]));
  const outcome = stringField(body.outcome, "outcome", { required: true, maxLength: 40 });
  if (outcome !== "accepted" && outcome !== "changes_requested") {
    throw new ApiError(400, "INVALID_FIELD", "'outcome' must be accepted or changes_requested");
  }
  const note = stringField(body.note ?? "", "note", { maxLength: 20_000 }).trim();
  if (outcome === "changes_requested" && !note) {
    throw new ApiError(422, "ACCEPTANCE_NOTE_REQUIRED", "A return note is required when requesting changes");
  }
  return { outcome, note };
}

function productAgentPrompt(session, message, sourceBaseline) {
  const currentDocument = session.productDocument.trim()
    ? session.productDocument
    : "（尚未形成产品方案）";
  return [
    "你是当前项目的产品协作 Agent。你的职责是通过多轮对话澄清业务问题，并帮助产品同学形成可交付给技术团队的产品方案。",
    "开始提出产品方案前，先按以下源码基准只读检查项目源码、蓝图和相关现有文档，确认当前能力、可复用入口与真实约束。",
    productSourceBaselinePrompt(sourceBaseline),
    "你可以读取当前项目中的完整源码，但不得修改任何文件、运行部署、提交代码或声称已经完成技术实现。",
    `当前功能交付目录：${session.artifactPath ?? "由系统创建后提供"}。产品文档由 Taskboard 保存，Agent 不直接写文件。`,
    "优先提出少量关键问题；存在会改变范围或用户行为的未决问题时继续提问，不要假装方案已经就绪。",
    "信息足够时输出一份完整、可直接保存的 Markdown 产品方案。最终方案必须使用以下精确二级标题，不能改名或降级为编号标题：## 背景与问题、## 现有能力依据、## 与现有 SkillHub 的关系、## 目标与非目标、## 目标用户与入口、## 用户流程、## 功能需求、## 界面与状态、## 兼容影响、## 验收标准、## 待确认事项。",
    "每条功能需求使用唯一的 FR-001、FR-002 格式；每条验收场景使用唯一的 AC-001、AC-002 格式。最终没有未决产品问题时，待确认事项写“- 无。”。不要在产品方案中规定源码文件、接口、数据库或框架实现。",
    "",
    "当前已保存的产品方案：",
    currentDocument,
    "",
    "产品同学本轮输入：",
    message,
  ].join("\n");
}

function technicalAgentPrompt(session, message) {
  const currentDocument = session.technicalDocument.trim()
    ? session.technicalDocument
    : "（尚未形成技术方案）";
  return [
    "你是当前需求的技术方案协作 Agent。请基于已确认产品方案和当前项目源码，通过多轮对话帮助技术同学形成可实施、可验证的技术方案。",
    "你可以只读检查当前项目源码，但不要修改文件、提交代码、运行部署或声称开发已经完成。",
    "优先澄清架构边界、数据模型、接口、权限、安全、兼容性、测试、发布和回滚风险。信息充分时输出一份完整 Markdown 技术方案。",
    "最终方案必须使用以下精确二级标题，不能改名或降级为编号标题：## 产品规格基线、## 当前实现证据、## 技术方向、## 影响设计、## 实施计划、## 验证映射、## 蓝图更新范围、## 部署与回滚、## 风险与技术决策。",
    "验证映射必须引用产品文档中的全部 AC-001、AC-002 等编号；蓝图更新范围必须说明功能验收通过后需要更新的 docs/blueprint.md 或 docs/blueprint/ 节点。",
    "",
    "已确认产品方案：",
    session.approvedDocument,
    "",
    "当前已保存技术方案：",
    currentDocument,
    "",
    "技术同学本轮输入：",
    message,
  ].join("\n");
}

function parseComposerCandidateQuery(searchParams) {
  assertAllowedQuery(
    searchParams,
    new Set([
      "projectId",
      "threadId",
      "trigger",
      "query",
      "surface",
      "codexProjectId",
      "codexProjectKind",
      "codexHostId",
      "workspacePath",
    ]),
    "GET /api/local/ai/composer/candidates",
  );
  let projectId;
  const rawProjectId = searchParams.get("projectId");
  if (rawProjectId !== null) {
    try {
      projectId = validateProjectId(rawProjectId);
    } catch {
      throw new ApiError(400, "INVALID_COMPOSER_QUERY", "Composer project id is invalid");
    }
  }
  const trigger = searchParams.get("trigger");
  if (trigger !== "/" && trigger !== "@") {
    throw new ApiError(400, "INVALID_COMPOSER_QUERY", "Composer trigger must be '/' or '@'");
  }
  const query = searchParams.get("query") ?? "";
  if (query.length > 256) {
    throw new ApiError(400, "INVALID_COMPOSER_QUERY", "Composer query cannot exceed 256 characters");
  }
  let threadId;
  try {
    threadId = parseThreadId(searchParams.get("threadId") ?? undefined);
  } catch {
    throw new ApiError(400, "INVALID_COMPOSER_QUERY", "Composer thread id is invalid");
  }
  const surface = searchParams.get("surface") ?? "ai-chat";
  if (!new Set(["ai-chat", "issue-description", "comment"]).has(surface)) {
    throw new ApiError(400, "INVALID_COMPOSER_QUERY", "Composer surface is invalid");
  }
  return {
    projectId,
    threadId,
    trigger,
    query,
    surface,
    ...aiExecutionTargetFromQuery(searchParams),
  };
}

function invalidComposerRebindRequest(message) {
  return new ApiError(400, "INVALID_COMPOSER_REBIND_REQUEST", message);
}

function assertComposerRebindKeys(value, allowed, field) {
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) {
      throw invalidComposerRebindRequest(`'${field}.${key}' is not allowed`);
    }
  }
}

function parseComposerRebindRequest(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw invalidComposerRebindRequest("Composer rebind body must be an object");
  }
  assertComposerRebindKeys(
    value,
    new Set(["contractVersion", "projectId", "threadId", "document"]),
    "body",
  );
  if (value.contractVersion !== "composer.v1") {
    throw invalidComposerRebindRequest("'contractVersion' must be 'composer.v1'");
  }
  let projectId;
  try {
    projectId = validateProjectId(value.projectId);
  } catch {
    throw invalidComposerRebindRequest("'projectId' is invalid");
  }
  let threadId;
  try {
    threadId = parseThreadId(value.threadId);
  } catch {
    throw invalidComposerRebindRequest("'threadId' is invalid");
  }
  const document = value.document;
  if (!document || typeof document !== "object" || Array.isArray(document)) {
    throw invalidComposerRebindRequest("'document' must be an object");
  }
  assertComposerRebindKeys(document, new Set(["version", "nodes"]), "document");
  if (document.version !== 1) {
    throw invalidComposerRebindRequest("'document.version' must be 1");
  }
  if (!Array.isArray(document.nodes) || document.nodes.length > 200) {
    throw invalidComposerRebindRequest("'document.nodes' must contain at most 200 entries");
  }
  let textLength = 0;
  const nodes = document.nodes.map((node, nodeIndex) => {
    if (!node || typeof node !== "object" || Array.isArray(node)) {
      throw invalidComposerRebindRequest(`'document.nodes[${nodeIndex}]' must be an object`);
    }
    if (node.type === "text") {
      assertComposerRebindKeys(node, new Set(["type", "text"]), `document.nodes[${nodeIndex}]`);
      if (typeof node.text !== "string") {
        throw invalidComposerRebindRequest(`'document.nodes[${nodeIndex}].text' must be a string`);
      }
      textLength += node.text.length;
      return { type: "text", text: node.text };
    }
    if (node.type === "unsupportedReference") {
      assertComposerRebindKeys(
        node,
        new Set(["type", "referenceUri", "label"]),
        `document.nodes[${nodeIndex}]`,
      );
      if (typeof node.label !== "string" || node.label.length === 0 || node.label.length > 256) {
        throw invalidComposerRebindRequest(`'document.nodes[${nodeIndex}].label' is invalid`);
      }
      if (typeof node.referenceUri !== "string" || node.referenceUri.length > 1_024) {
        throw invalidComposerRebindRequest(
          `'document.nodes[${nodeIndex}].referenceUri' is invalid`,
        );
      }
      const match = /^taskboard:\/\/composer-reference\/([^/]+)\/([^/]+)\/([^/]+)$/.exec(
        node.referenceUri,
      );
      if (!match) {
        throw invalidComposerRebindRequest(
          `'document.nodes[${nodeIndex}].referenceUri' is not a composer reference marker`,
        );
      }
      try {
        decodeComposerReferenceKey(match[3]);
      } catch {
        throw invalidComposerRebindRequest(
          `'document.nodes[${nodeIndex}].referenceUri' has an invalid reference key`,
        );
      }
      const reasonCode = match[1] !== "v1"
        ? "REFERENCE_FORMAT_UNSUPPORTED"
        : !new Set(["skill", "agent"]).has(match[2])
          ? "REFERENCE_KIND_UNSUPPORTED"
          : null;
      if (!reasonCode) {
        throw invalidComposerRebindRequest(
          `'document.nodes[${nodeIndex}]' must use persistedReference for supported markers`,
        );
      }
      return {
        type: "unsupportedReference",
        referenceUri: node.referenceUri,
        label: node.label,
        reasonCode,
      };
    }
    if (node.type !== "persistedReference") {
      throw invalidComposerRebindRequest(
        `'document.nodes[${nodeIndex}].type' must be text, persistedReference or unsupportedReference`,
      );
    }
    assertComposerRebindKeys(
      node,
      new Set(["type", "referenceKind", "referenceKey", "label"]),
      `document.nodes[${nodeIndex}]`,
    );
    if (node.referenceKind !== "skill" && node.referenceKind !== "agent") {
      throw invalidComposerRebindRequest(
        `'document.nodes[${nodeIndex}].referenceKind' must be skill or agent`,
      );
    }
    if (
      typeof node.referenceKey !== "string"
      || node.referenceKey.length === 0
      || node.referenceKey.length > 512
    ) {
      throw invalidComposerRebindRequest(
        `'document.nodes[${nodeIndex}].referenceKey' is invalid`,
      );
    }
    if (typeof node.label !== "string" || node.label.length === 0 || node.label.length > 256) {
      throw invalidComposerRebindRequest(`'document.nodes[${nodeIndex}].label' is invalid`);
    }
    let stableId;
    try {
      stableId = decodeComposerReferenceKey(node.referenceKey);
    } catch {
      throw invalidComposerRebindRequest(
        `'document.nodes[${nodeIndex}].referenceKey' is not canonical base64url`,
      );
    }
    if (node.referenceKind === "skill" && stableId !== stableId.normalize("NFC")) {
      throw invalidComposerRebindRequest(
        `'document.nodes[${nodeIndex}].referenceKey' does not contain an NFC Skill name`,
      );
    }
    return {
      type: "persistedReference",
      referenceKind: node.referenceKind,
      referenceKey: node.referenceKey,
      label: node.label,
      stableId,
    };
  });
  if (textLength > 100_000) {
    throw invalidComposerRebindRequest("Composer text cannot exceed 100000 characters");
  }
  return {
    contractVersion: "composer.v1",
    projectId,
    threadId,
    document: { version: 1, nodes },
  };
}

async function resolveComposerRebindWorkspace(aiChat, input) {
  let thread;
  if (input.threadId !== undefined) {
    try {
      thread = aiChat.getThread(input.threadId);
    } catch (error) {
      if (error instanceof ApiError && error.code === "AI_CHAT_THREAD_NOT_FOUND") {
        throw new ApiError(400, "INVALID_COMPOSER_QUERY", "Composer thread does not exist");
      }
      throw error;
    }
    if (thread.origin.projectId !== input.projectId) {
      throw new ApiError(
        400,
        "INVALID_COMPOSER_QUERY",
        "Composer thread does not belong to the selected project",
      );
    }
    if (thread.origin.codexProjectKind !== "remote") {
      try {
        if (!(await stat(thread.origin.workspacePath)).isDirectory()) throw new Error("not a directory");
      } catch {
        throw new ApiError(
          409,
          "PROJECT_WORKSPACE_UNAVAILABLE",
          "The conversation workspace is not available on this device",
        );
      }
    }
    return {
      workspacePath: thread.origin.workspacePath,
      composerCatalog: aiChat.composerCatalogForThread(thread),
    };
  }
  let resolved;
  try {
    resolved = await aiChat.resolveContext(input.projectId, thread?.origin.issueId);
  } catch (error) {
    if (
      error instanceof ApiError
      && ["PROJECT_NOT_FOUND", "AI_CHAT_ISSUE_NOT_FOUND"].includes(error.code)
    ) {
      throw new ApiError(400, "INVALID_COMPOSER_QUERY", "Composer project is invalid");
    }
    throw error;
  }
  return { workspacePath: resolved.workspacePath, composerCatalog: aiChat.composerCatalog };
}

function parseComposerDocument(value) {
  assertPlainObject(value);
  assertAllowedKeys(value, new Set(["version", "nodes"]));
  if (value.version !== 1) {
    throw new ApiError(400, "INVALID_COMPOSER_DOCUMENT", "'document.version' must be 1");
  }
  if (!Array.isArray(value.nodes) || value.nodes.length > 200) {
    throw new ApiError(
      400,
      "INVALID_COMPOSER_DOCUMENT",
      "'document.nodes' must be an array with at most 200 entries",
    );
  }
  let textLength = 0;
  const nodes = value.nodes.map((node, index) => {
    assertPlainObject(node);
    if (typeof node.type !== "string" || !node.type) {
      throw new ApiError(
        400,
        "INVALID_COMPOSER_DOCUMENT",
        `'document.nodes[${index}].type' is required`,
      );
    }
    if (node.type === "text") {
      assertAllowedKeys(node, new Set(["type", "text"]));
      if (typeof node.text !== "string") {
        throw new ApiError(
          400,
          "INVALID_COMPOSER_DOCUMENT",
          `'document.nodes[${index}].text' must be a string`,
        );
      }
      textLength += node.text.length;
      return { type: "text", text: node.text };
    }
    if (node.type === "skill" || node.type === "agent") {
      assertAllowedKeys(node, new Set(["type", "candidateRef", "label"]));
      return {
        type: node.type,
        candidateRef: stringField(
          node.candidateRef,
          `document.nodes[${index}].candidateRef`,
          { required: true, maxLength: 512 },
        ),
        label: stringField(node.label, `document.nodes[${index}].label`, {
          required: true,
          maxLength: 256,
        }),
      };
    }
    return { type: node.type };
  });
  if (textLength > 100_000) {
    throw new ApiError(
      400,
      "INVALID_COMPOSER_DOCUMENT",
      "Composer text cannot exceed 100000 characters",
    );
  }
  return { version: 1, nodes };
}

function parseComposerTurn(body) {
  assertAllowedKeys(body, new Set([
    "contractVersion",
    "revision",
    "document",
    "dangerFullAccessConfirmed",
    "attachments",
  ]));
  if (body.contractVersion !== "composer.v1") {
    throw new ApiError(
      400,
      "INVALID_COMPOSER_DOCUMENT",
      "'contractVersion' must be 'composer.v1'",
    );
  }
  if (
    body.dangerFullAccessConfirmed !== undefined
    && typeof body.dangerFullAccessConfirmed !== "boolean"
  ) {
    throw new ApiError(400, "INVALID_FIELD", "'dangerFullAccessConfirmed' must be a boolean");
  }
  return {
    contractVersion: "composer.v1",
    revision: stringField(body.revision, "revision", { required: true, maxLength: 512 }),
    document: parseComposerDocument(body.document),
    dangerFullAccessConfirmed: body.dangerFullAccessConfirmed,
    attachments: parseAiAttachments(body.attachments),
  };
}

class EventHub {
  constructor() {
    this.clients = new Set();
    this.keepAlive = setInterval(() => {
      for (const response of this.clients) response.write(": keep-alive\n\n");
    }, 20_000);
    this.keepAlive.unref();
  }

  connect(request, response) {
    response.writeHead(200, {
      connection: "keep-alive",
      "cache-control": "no-cache, no-transform",
      "content-type": "text/event-stream; charset=utf-8",
      "x-accel-buffering": "no",
    });
    response.write(": connected\n\n");
    this.clients.add(response);
    request.once("close", () => this.clients.delete(response));
  }

  emit(type, value) {
    const event = {
      type,
      projectId: value.projectId ?? value.project?.id ?? value.task?.projectId,
      taskId: value.task?.id ?? value.comment?.taskId ?? value.attachment?.taskId,
      ...value,
      at: new Date().toISOString(),
    };
    const message = `event: ${type}\ndata: ${JSON.stringify(event)}\n\n`;
    for (const response of this.clients) response.write(message);
  }

  close() {
    clearInterval(this.keepAlive);
    for (const response of this.clients) response.end();
    this.clients.clear();
  }
}

async function serveStatic(request, response, pathname, staticDirectory) {
  if (request.method !== "GET" && request.method !== "HEAD") return false;
  let decodedPath;
  try {
    decodedPath = decodeURIComponent(pathname);
  } catch {
    throw new ApiError(400, "INVALID_PATH", "URL path contains invalid encoding");
  }
  if (decodedPath.includes("\0")) {
    throw new ApiError(400, "INVALID_PATH", "URL path is invalid");
  }

  const root = path.resolve(staticDirectory);
  const relativePath = decodedPath === "/" ? "index.html" : decodedPath.replace(/^\/+/, "");
  let filename = path.resolve(root, relativePath);
  if (filename !== root && !filename.startsWith(`${root}${path.sep}`)) {
    throw new ApiError(400, "INVALID_PATH", "URL path is outside the static directory");
  }

  let fileStats;
  try {
    fileStats = await stat(filename);
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  if (!fileStats?.isFile() && !path.extname(relativePath)) {
    filename = path.join(root, "index.html");
    try {
      fileStats = await stat(filename);
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
  }
  if (!fileStats?.isFile()) return false;

  const body = await readFile(filename);
  const headers = {
    "cache-control": path.basename(filename) === "index.html" ? "no-cache" : "public, max-age=31536000, immutable",
    "content-length": body.length,
    "content-type": CONTENT_TYPES.get(path.extname(filename).toLowerCase()) ?? "application/octet-stream",
  };
  response.writeHead(200, headers);
  response.end(request.method === "HEAD" ? undefined : body);
  return true;
}

function methodNotAllowed(response, allowed) {
  sendJson(response, 405, {
    error: { code: "METHOD_NOT_ALLOWED", message: `Allowed methods: ${allowed.join(", ")}` },
  }, { allow: allowed.join(", ") });
}

function codexProjectRoot(state, projectId) {
  if (!projectId || !state || typeof state !== "object") return null;
  const project = state["local-projects"]?.[projectId];
  const root = Array.isArray(project?.rootPaths) ? project.rootPaths[0] : null;
  return typeof root === "string" && root.trim() ? root : null;
}

async function readCodexProjectWorkspaces(codexStatePath) {
  try {
    const state = JSON.parse(await readFile(codexStatePath, "utf8"));
    const projects = state["local-projects"];
    if (!projects || typeof projects !== "object" || Array.isArray(projects)) return {};
    return Object.fromEntries(Object.keys(projects).flatMap((projectId) => {
      const root = codexProjectRoot(state, projectId);
      return root ? [[projectId, root]] : [];
    }));
  } catch {
    return {};
  }
}

function latestThreadCwd(value, threadId) {
  const matches = [];
  const stack = [value];
  while (stack.length > 0) {
    const candidate = stack.pop();
    if (!candidate || typeof candidate !== "object") continue;
    if (candidate.conversationId === threadId && typeof candidate.cwd === "string" && candidate.cwd.trim()) {
      matches.push(candidate);
    }
    stack.push(...(Array.isArray(candidate) ? candidate : Object.values(candidate)));
  }
  matches.sort((left, right) => Number(right.updatedAtMs ?? 0) - Number(left.updatedAtMs ?? 0));
  return matches[0]?.cwd ?? null;
}

async function resolveProjectWorkspace(project, codexProjectId, codexThreadId, codexStatePath, codexProcessesPath) {
  try {
    const state = JSON.parse(await readFile(codexStatePath, "utf8"));
    const assignment = state["thread-project-assignments"]?.[codexThreadId];
    const root = codexProjectRoot(state, project.id)
      ?? codexProjectRoot(state, codexProjectId)
      ?? codexProjectRoot(state, assignment?.projectId)
      ?? (typeof assignment?.cwd === "string" ? assignment.cwd : null);
    if (root) return root;
  } catch {}
  if (project.workspacePath) return project.workspacePath;
  if (!codexThreadId) return null;
  try {
    const processes = JSON.parse(await readFile(codexProcessesPath, "utf8"));
    return latestThreadCwd(processes, codexThreadId);
  } catch {
    return null;
  }
}

async function parseWorktrees(output) {
  const contexts = [];
  for (const block of output.trim().split(/\n\s*\n/)) {
    if (!block) continue;
    let worktreePath = "";
    let branch = null;
    let prunable = false;
    for (const line of block.split("\n")) {
      if (line.startsWith("worktree ")) worktreePath = line.slice(9);
      if (line.startsWith("branch refs/heads/")) branch = line.slice(18);
      if (line.startsWith("prunable")) prunable = true;
    }
    if (!worktreePath || prunable) continue;
    try {
      await stat(worktreePath);
    } catch (error) {
      if (error?.code === "ENOENT") continue;
      throw error;
    }
    contexts.push({ type: "worktree", path: worktreePath, branch });
  }
  return contexts;
}

async function scanDevelopmentContexts(workspacePath, processEnv = process.env) {
  if (!workspacePath) return { workspacePath: null, contexts: [] };
  const environment = withoutTaskboardLauncherEnvironment(processEnv);
  try {
    const rootResult = await execFileAsync("git", ["-C", workspacePath, "rev-parse", "--show-toplevel"], {
      env: environment,
      timeout: 4_000,
      maxBuffer: 1024 * 1024,
    });
    const root = rootResult.stdout.trim();
    const [branchesResult, worktreesResult] = await Promise.all([
      execFileAsync("git", ["-C", root, "for-each-ref", "--format=%(refname:short)", "refs/heads"], {
        env: environment,
        timeout: 4_000,
        maxBuffer: 1024 * 1024,
      }),
      execFileAsync("git", ["-C", root, "worktree", "list", "--porcelain"], {
        env: environment,
        timeout: 4_000,
        maxBuffer: 1024 * 1024,
      }),
    ]);
    const branches = branchesResult.stdout.split("\n").map((branch) => branch.trim()).filter(Boolean);
    return {
      workspacePath: root,
      contexts: [
        ...branches.map((branch) => ({ type: "branch", branch })),
        ...(await parseWorktrees(worktreesResult.stdout)),
      ],
    };
  } catch {
    return { workspacePath, contexts: [] };
  }
}

export function resolveServerOptions(options = {}) {
  const environment = options.processEnv ?? process.env;
  const configuredDataDirectory = options.dataDirectory ?? environment.CODEX_TASKBOARD_DATA_DIR;
  const dataDirectory = configuredDataDirectory
    ? path.resolve(configuredDataDirectory)
    : path.join(PROJECT_ROOT, ".data");
  const codexHome = process.env.CODEX_HOME || path.join(os.homedir(), ".codex");
  const instanceToken = String(
    options.instanceToken ?? environment.CODEX_TASKBOARD_INSTANCE_TOKEN ?? "",
  ).trim();
  if (instanceToken && !/^[a-z0-9-]{16,128}$/i.test(instanceToken)) {
    throw new Error("CODEX_TASKBOARD_INSTANCE_TOKEN must be an identifier");
  }
  const instanceSecret = String(
    options.instanceSecret ?? environment.CODEX_TASKBOARD_INSTANCE_SECRET ?? "",
  ).trim();
  if (instanceToken && !/^[a-f0-9-]{32,128}$/i.test(instanceSecret)) {
    throw new Error("CODEX_TASKBOARD_INSTANCE_SECRET must be set in launcher mode");
  }
  const publicSharedSecret = String(
    options.publicSharedSecret ?? environment[PUBLIC_SHARED_SECRET_ENV] ?? "",
  );
  if (publicSharedSecret && publicSharedSecret.length < 16) {
    throw new Error(`${PUBLIC_SHARED_SECRET_ENV} must contain at least 16 characters`);
  }
  const publicUsers = parsePublicUsers(options.publicUsers ?? environment[PUBLIC_USERS_ENV]);
  const publicAuthEnabled = Boolean(publicSharedSecret || publicUsers.length > 0);
  const deliveryProjects = parseDeliveryProjects(
    options.deliveryProjects ?? environment[DELIVERY_PROJECTS_ENV],
  );
  return {
    dataDirectory,
    databasePath: options.databasePath ?? path.join(dataDirectory, "taskboard.sqlite"),
    attachmentsDirectory: options.attachmentsDirectory ?? path.join(dataDirectory, "attachments"),
    cloudConfigPath: options.cloudConfigPath ?? path.join(dataDirectory, "cloud-companion.json"),
    jiraConfigPath: options.jiraConfigPath ?? path.join(dataDirectory, "jira-connection.json"),
    clientStoragePath: options.clientStoragePath ?? path.join(dataDirectory, "client-storage.json"),
    staticDirectory: options.staticDirectory ?? path.join(PROJECT_ROOT, "dist", "web"),
    skillPath: options.skillPath
      ?? environment.CODEX_TASKBOARD_SKILL_PATH
      ?? path.join(PROJECT_ROOT, "skills", "manage-taskboard", "SKILL.md"),
    codexExecutable: resolveCodexExecutable({ explicit: options.codexExecutable }),
    codexStatePath: options.codexStatePath
      ?? path.join(codexHome, ".codex-global-state.json"),
    codexProcessesPath: options.codexProcessesPath
      ?? path.join(codexHome, "process_manager", "chat_processes.json"),
    instanceToken,
    instanceSecret,
    publicSharedSecret,
    publicUsers,
    publicAuthEnabled,
    deliveryProjects,
    trustedOrigins: parseTrustedOrigins(environment[TRUSTED_ORIGINS_ENV]),
    version: String(
      options.version ?? environment.CODEX_TASKBOARD_VERSION ?? "development",
    ).trim(),
  };
}

export function resolvePort(value = process.env.CODEX_TASKBOARD_PORT ?? "47823") {
  const port = typeof value === "number" ? value : Number(value);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("CODEX_TASKBOARD_PORT must be an integer between 1 and 65535");
  }
  return port;
}

export function resolveHost(value = process.env.CODEX_TASKBOARD_HOST ?? "0.0.0.0") {
  const host = String(value).trim();
  if (host !== "127.0.0.1" && host !== "0.0.0.0") {
    throw new Error("CODEX_TASKBOARD_HOST must be 127.0.0.1 or 0.0.0.0");
  }
  return host;
}

export function createTaskboardServer(options = {}) {
  const resolved = resolveServerOptions(options);
  const codexProcessEnvironment = withoutTaskboardLauncherEnvironment(
    options.processEnv ?? process.env,
  );
  const routePrefix = resolved.instanceToken ? `/${resolved.instanceToken}` : "";
  const database = new TaskboardDatabase(resolved.databasePath);
  database.failActiveDeliveryRuns();
  const events = new EventHub();
  let clientStorageWrite = Promise.resolve();

  async function readClientStorage() {
    try {
      const value = JSON.parse(await readFile(resolved.clientStoragePath, "utf8"));
      return value && typeof value === "object" && !Array.isArray(value) ? value : {};
    } catch (error) {
      if (error.code === "ENOENT") return {};
      throw error;
    }
  }

  function parseClientStorageUpdate(body) {
    assertPlainObject(body);
    assertAllowedKeys(body, new Set(["key", "value"]));
    const key = stringField(body.key, "key", { required: true, maxLength: 512 });
    const value = stringField(body.value, "value", { nullable: true, maxLength: 100_000 });
    return { key, value };
  }

  async function updateClientStorage({ key, value }) {
    clientStorageWrite = clientStorageWrite.catch(() => {}).then(async () => {
      const entries = await readClientStorage();
      if (value === null) delete entries[key];
      else entries[key] = value;
      await mkdir(path.dirname(resolved.clientStoragePath), { recursive: true });
      const temporaryPath = `${resolved.clientStoragePath}.${process.pid}.tmp`;
      await writeFile(temporaryPath, `${JSON.stringify(entries)}\n`, { mode: 0o600 });
      await chmod(temporaryPath, 0o600);
      await rename(temporaryPath, resolved.clientStoragePath);
      await chmod(resolved.clientStoragePath, 0o600);
    });
    await clientStorageWrite;
  }
  const cloudConfig = options.cloudConfigStore ?? createCloudConfigStore({
    configPath: resolved.cloudConfigPath,
  });
  const jiraConfig = options.jiraConfigStore ?? createJiraConfigStore({
    configPath: resolved.jiraConfigPath,
  });
  const jira = createJiraIntegration({
    configStore: jiraConfig,
    database,
    fetch: options.jiraFetch ?? globalThis.fetch,
  });
  let hostRuntime = null;
  function currentHostThreadBinding(threadId) {
    if (
      !hostRuntime
      || hostRuntime.threadId !== threadId
      || !hostRuntime.codexProjectId
      || !hostRuntime.codexProjectKind
      || !hostRuntime.codexHostId
      || !hostRuntime.workspacePath
    ) return undefined;
    return {
      threadId,
      codexProjectId: hostRuntime.codexProjectId,
      codexProjectKind: hostRuntime.codexProjectKind,
      codexHostId: hostRuntime.codexHostId,
      workspacePath: hostRuntime.workspacePath,
    };
  }
  function resolveInputThreadBinding(input) {
    if (input.threadBinding !== undefined) return input;
    const threadBinding = currentHostThreadBinding(input.threadId);
    return threadBinding ? { ...input, threadBinding } : input;
  }
  const cloudProxy = createCloudProxy({
    configStore: cloudConfig,
    fetch: options.remoteFetch ?? globalThis.fetch,
    resolveThreadBinding: currentHostThreadBinding,
    resolveDevelopmentContext: async (projectId, context) => {
      if (!context.branch) return null;
      const config = await cloudConfig.read();
      const workspacePath = config.projectMappings[projectId];
      if (!workspacePath) return null;
      const result = await scanDevelopmentContexts(workspacePath, codexProcessEnvironment);
      return result.contexts.find((candidate) => (
        candidate.type === "worktree" && candidate.branch === context.branch
      )) ?? null;
    },
    assertTaskProjectMoveAllowed: (taskId, targetProjectId) => {
      if (!database.hasAiChatThreadProjectConflict(taskId, targetProjectId)) return;
      throw new CloudProxyError(
        409,
        "AI_CHAT_PROJECT_MOVE_BLOCKED",
        "Delete issue-linked AI conversations before moving the issue to another project",
      );
    },
  });
  async function readCloudJson(pathname) {
    const upstream = await cloudProxy.forward(new Request(`http://127.0.0.1${pathname}`, {
      headers: { accept: "application/json" },
    }));
    let payload;
    try {
      payload = await upstream.json();
    } catch {
      throw new ApiError(
        upstream.ok ? 502 : upstream.status,
        "INVALID_CLOUD_RESPONSE",
        "Cloud taskboard returned an invalid JSON response",
      );
    }
    if (!upstream.ok) {
      throw new ApiError(
        upstream.status,
        payload?.error?.code ?? "CLOUD_REQUEST_FAILED",
        payload?.error?.message ?? "Cloud taskboard request failed",
        payload?.error?.details,
      );
    }
    return payload;
  }

  async function resolveAiChatContext(projectId, issueId, codexTarget) {
    const config = await cloudConfig.read();
    if (!config.remoteUrl) {
      if (codexTarget?.codexProjectKind === "remote") {
        const project = database.getProject(projectId);
        if (!project) {
          throw new ApiError(404, "PROJECT_NOT_FOUND", `Project '${projectId}' does not exist`);
        }
        let issue;
        if (issueId !== undefined) {
          issue = database.getTask(issueId);
          if (!issue || issue.projectId !== projectId || issue.archivedAt != null) {
            throw new ApiError(
              404,
              "AI_CHAT_ISSUE_NOT_FOUND",
              `Task '${issueId}' is not an active task in project '${projectId}'`,
            );
          }
        }
        return { project, issue, addDirectories: [], ...codexTarget };
      }
      let resolvedWorkspace;
      try {
        resolvedWorkspace = await resolveAiWorkspace(
          projectId,
          resolved.codexStatePath,
          database,
        );
      } catch (error) {
        if (
          !(error instanceof ApiError)
          || error.code !== "PROJECT_WORKSPACE_UNAVAILABLE"
          || projectId !== DEFAULT_PROJECT_ID
        ) {
          throw error;
        }
        resolvedWorkspace = {
          workspacePath: PROJECT_ROOT,
          addDirectories: [],
          project: database.getProject(projectId),
        };
      }
      let issue;
      if (issueId !== undefined) {
        issue = database.getTask(issueId);
        if (!issue || issue.projectId !== projectId || issue.archivedAt != null) {
          throw new ApiError(
            404,
            "AI_CHAT_ISSUE_NOT_FOUND",
            `Task '${issueId}' is not an active task in project '${projectId}'`,
          );
        }
      }
      return { ...resolvedWorkspace, issue };
    }

    const projectPayload = await readCloudJson("/api/projects");
    const project = Array.isArray(projectPayload.projects)
      ? projectPayload.projects.find((candidate) => candidate?.id === projectId)
      : null;
    if (!project) {
      throw new ApiError(404, "PROJECT_NOT_FOUND", `Project '${projectId}' does not exist`);
    }

    let issue;
    if (issueId !== undefined) {
      const issuePayload = await readCloudJson(`/api/tasks/${encodeURIComponent(issueId)}`);
      issue = issuePayload.task;
      if (!issue || issue.projectId !== projectId || issue.archivedAt != null) {
        throw new ApiError(
          404,
          "AI_CHAT_ISSUE_NOT_FOUND",
          `Task '${issueId}' is not an active task in project '${projectId}'`,
        );
      }
    }

    if (codexTarget?.codexProjectKind === "remote") {
      return { project, issue, addDirectories: [], ...codexTarget };
    }

    const resolvedWorkspace = await resolveMappedAiWorkspace(
      projectId,
      project,
      config.projectMappings,
    );
    return { ...resolvedWorkspace, issue };
  }

  const aiChat = new AiChatService({
    database,
    codexExecutable: resolved.codexExecutable,
    codexStatePath: resolved.codexStatePath,
    manageTaskboardSkillPath: resolved.skillPath,
    processEnv: codexProcessEnvironment,
    resolveContext: resolveAiChatContext,
    remoteAppServerFactory: options.remoteAppServerFactory,
  });
  const productMessageMarker = "产品同学本轮输入：\n";
  const technicalMessageMarker = "技术同学本轮输入：\n";

  function featureSlug(title) {
    const normalized = title
      .normalize("NFKC")
      .toLocaleLowerCase("en-US")
      .replace(/[^\p{Letter}\p{Number}]+/gu, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 48);
    return normalized || "feature";
  }

  function featureArtifactRelativePath(session) {
    const date = String(session.createdAt ?? new Date().toISOString()).slice(0, 10);
    return path.posix.join(
      "docs",
      "prds",
      `${date}-${featureSlug(session.title)}-${session.id.slice(0, 8)}`,
    );
  }

  function productSessionWithArtifactPath(session) {
    return { ...session, artifactPath: featureArtifactRelativePath(session) };
  }

  async function requireProductProjectWorkspace(session) {
    const project = database.getProject(session.projectId);
    if (!project) {
      throw new ApiError(404, "PROJECT_NOT_FOUND", `Project '${session.projectId}' does not exist`);
    }
    if (!project.workspacePath) {
      throw new ApiError(
        409,
        "PROJECT_WORKSPACE_REQUIRED",
        "Product collaboration requires a readable project workspace",
      );
    }
    const workspacePath = path.resolve(project.workspacePath);
    let workspaceStat;
    try {
      workspaceStat = await stat(workspacePath);
    } catch {
      throw new ApiError(
        409,
        "PROJECT_WORKSPACE_UNAVAILABLE",
        "The project workspace is not available on this Taskboard host",
      );
    }
    if (!workspaceStat.isDirectory()) {
      throw new ApiError(
        409,
        "PROJECT_WORKSPACE_UNAVAILABLE",
        "The configured project workspace is not a directory",
      );
    }
    const thread = aiChat.getThread(session.aiThreadId);
    if (thread.origin.workspacePath !== workspacePath) {
      database.updateAiChatThread(session.aiThreadId, { workspacePath });
    }
    return workspacePath;
  }

  async function readFeatureState(featureDirectory, session) {
    try {
      const state = JSON.parse(await readFile(path.join(featureDirectory, "feature.json"), "utf8"));
      if (state && typeof state === "object" && !Array.isArray(state)) return state;
    } catch (error) {
      if (error?.code !== "ENOENT") {
        throw new ApiError(500, "FEATURE_ARTIFACT_INVALID", "Cannot read the feature artifact state");
      }
    }
    return {
      schemaVersion: 1,
      id: path.basename(featureDirectory),
      title: session.title,
      status: "draft",
      productOwner: null,
      technicalOwner: null,
      productApprovedAt: null,
      technicalApprovedAt: null,
      productSpecHash: null,
      technicalDesignHash: null,
      implementationPr: null,
      testDeployment: {
        url: null,
        workflowRun: null,
        immutableTag: null,
        prNumbers: [],
      },
      blueprintBaselineHash: null,
      blueprintPaths: [],
      blueprintHash: null,
      blueprintUpdatedAt: null,
      acceptedAt: null,
    };
  }

  async function collectProjectBlueprintPaths(workspacePath) {
    const paths = ["docs/blueprint.md"];
    const detailRoot = path.resolve(workspacePath, "docs", "blueprint");
    async function visit(directory) {
      let entries;
      try {
        entries = await readdir(directory, { withFileTypes: true });
      } catch (error) {
        if (error?.code === "ENOENT") return;
        throw error;
      }
      for (const entry of entries) {
        const entryPath = path.join(directory, entry.name);
        if (entry.isDirectory()) {
          await visit(entryPath);
        } else if (entry.isFile()) {
          paths.push(path.relative(workspacePath, entryPath).split(path.sep).join("/"));
        }
      }
    }
    await visit(detailRoot);
    return [...new Set(paths)].sort();
  }

  async function calculateProjectBlueprintHash(workspacePath, relativePaths) {
    const digest = createHash("sha256");
    for (const relativePath of [...relativePaths].sort()) {
      const blueprintPath = path.resolve(workspacePath, relativePath);
      const allowedBlueprintRoot = path.resolve(workspacePath, "docs", "blueprint");
      const canonicalBlueprint = path.resolve(workspacePath, "docs", "blueprint.md");
      if (
        blueprintPath !== canonicalBlueprint
        && blueprintPath !== allowedBlueprintRoot
        && !blueprintPath.startsWith(`${allowedBlueprintRoot}${path.sep}`)
      ) {
        throw new ApiError(500, "BLUEPRINT_PATH_INVALID", "Blueprint path escaped docs/blueprint");
      }
      let content;
      try {
        content = await readFile(blueprintPath);
      } catch (error) {
        if (error?.code === "ENOENT") {
          throw new ApiError(
            409,
            "PROJECT_BLUEPRINT_REQUIRED",
            "Product collaboration requires docs/blueprint.md in the project workspace",
          );
        }
        throw error;
      }
      digest.update(relativePath);
      digest.update("\0");
      digest.update(content);
      digest.update("\0");
    }
    return `sha256:${digest.digest("hex")}`;
  }

  async function currentProjectBlueprint(workspacePath) {
    const paths = await collectProjectBlueprintPaths(workspacePath);
    return {
      paths,
      hash: await calculateProjectBlueprintHash(workspacePath, paths),
    };
  }

  async function requireUpdatedProjectBlueprint(session) {
    const workspacePath = await requireProductProjectWorkspace(session);
    const relativePath = featureArtifactRelativePath(session);
    const featureDirectory = path.resolve(workspacePath, relativePath);
    const state = await readFeatureState(featureDirectory, session);
    const current = await currentProjectBlueprint(workspacePath);
    if (!state.blueprintBaselineHash || state.blueprintBaselineHash === current.hash) {
      throw new ApiError(
        409,
        "PROJECT_BLUEPRINT_UPDATE_REQUIRED",
        "Update the project feature blueprint before accepting this delivery",
      );
    }
    return current;
  }

  async function writeFeatureFile(featureDirectory, filename, content) {
    const target = path.join(featureDirectory, filename);
    const temporary = `${target}.${randomUUID()}.tmp`;
    await writeFile(temporary, content, "utf8");
    await rename(temporary, target);
  }

  async function syncFeatureArtifacts(session) {
    const workspacePath = await requireProductProjectWorkspace(session);
    const relativePath = featureArtifactRelativePath(session);
    const featureDirectory = path.resolve(workspacePath, relativePath);
    const allowedRoot = path.resolve(workspacePath, "docs", "prds");
    if (featureDirectory !== allowedRoot && !featureDirectory.startsWith(`${allowedRoot}${path.sep}`)) {
      throw new ApiError(500, "FEATURE_ARTIFACT_PATH_INVALID", "Feature artifact path escaped docs/prds");
    }
    const state = await readFeatureState(featureDirectory, session);
    state.id = path.basename(featureDirectory);
    state.title = session.title;
    if (!state.blueprintBaselineHash) {
      state.blueprintBaselineHash = (await currentProjectBlueprint(workspacePath)).hash;
    }
    await mkdir(featureDirectory, { recursive: true });

    if (session.approvedDocument) {
      state.status = session.technicalApprovedDocument ? "tech-approved" : "product-approved";
      state.productOwner = session.approvedBy;
      state.productApprovedAt = session.approvedAt;
      state.productSpecHash = `sha256:${createHash("sha256").update(session.approvedDocument).digest("hex")}`;
    }
    if (session.technicalApprovedDocument) {
      state.status = "tech-approved";
      state.technicalOwner = session.technicalApprovedBy;
      state.technicalApprovedAt = session.technicalApprovedAt;
      state.technicalDesignHash = `sha256:${createHash("sha256").update(session.technicalApprovedDocument).digest("hex")}`;
    }
    if (session.deliverySubmittedAt) {
      state.status = session.acceptanceStatus === "changes_requested"
        ? "changes-requested"
        : session.acceptanceStatus === "accepted"
          ? "accepted"
          : "ready-for-uat";
      state.implementationPr = session.implementationPr;
      state.testDeployment = {
        url: session.testDeploymentUrl,
        workflowRun: session.testDeploymentWorkflowRun,
        immutableTag: session.testDeploymentImmutableTag,
        prNumbers: session.testDeploymentPrNumbers,
      };
    }
    if (session.acceptanceStatus === "accepted") {
      const blueprint = await currentProjectBlueprint(workspacePath);
      state.blueprintPaths = blueprint.paths;
      state.blueprintHash = blueprint.hash;
      state.blueprintUpdatedAt = session.acceptanceAt;
      state.acceptedAt = session.acceptanceAt;
    }

    const productDocument = session.productDocument.trim()
      ? session.productDocument
      : [
          `# ${session.title} 产品规格`,
          "",
          "## 背景与问题",
          "待产品与 Agent 基于当前源码澄清。",
          "",
          "## 现有能力依据",
          "待产品与 Agent 基于当前源码和功能蓝图澄清。",
          "",
          "## 与现有 SkillHub 的关系",
          "待产品与 Agent 澄清。",
          "",
          "## 目标与非目标",
          "待产品与 Agent 澄清。",
          "",
          "## 目标用户与入口",
          "待产品与 Agent 澄清。",
          "",
          "## 用户流程",
          "待产品与 Agent 澄清。",
          "",
          "## 功能需求",
          "待产品与 Agent 澄清。",
          "",
          "## 界面与状态",
          "待产品与 Agent 澄清。",
          "",
          "## 兼容影响",
          "待产品与 Agent 澄清。",
          "",
          "## 验收标准",
          "待产品与 Agent 澄清。",
          "",
          "## 待确认事项",
          "待产品与 Agent 澄清。",
          "",
        ].join("\n");
    await writeFeatureFile(featureDirectory, "product.md", productDocument);
    if (session.technicalDocument.trim()) {
      await writeFeatureFile(featureDirectory, "technical.md", session.technicalDocument);
    }
    if (session.deliverySubmittedAt) {
      const acceptanceIds = [...new Set(session.approvedDocument?.match(/AC-\d{3}/g) ?? [])];
      const result = session.acceptanceStatus === "accepted"
        ? "通过"
        : session.acceptanceStatus === "changes_requested"
          ? "失败"
          : "未验收";
      const rows = acceptanceIds.length > 0
        ? acceptanceIds.map((id) => `| ${id} | ${result} | ${session.deliveryNote} | ${session.acceptanceNote || "无"} |`)
        : [`| 未定义 | ${result} | ${session.deliveryNote} | ${session.acceptanceNote || "产品规格缺少 AC 编号"} |`];
      const uatDocument = [
        `# ${session.title} 产品验收记录`,
        "",
        "## 验收版本",
        `- 实现 PR：${session.implementationPr}`,
        `- 测试地址：${session.testDeploymentUrl}`,
        `- 部署工作流：${session.testDeploymentWorkflowRun}`,
        `- 不可变标签：${session.testDeploymentImmutableTag}`,
        `- PR 列表：${session.testDeploymentPrNumbers.join(", ")}`,
        "",
        "## 验收前提",
        `- 技术提交人：${session.deliverySubmittedBy}`,
        `- 技术提交时间：${session.deliverySubmittedAt}`,
        "",
        "## 验收结果",
        "| 验收 ID | 结果 | 证据 | 备注 |",
        "|---|---|---|---|",
        ...rows,
        "",
        "## 问题记录",
        session.acceptanceStatus === "changes_requested" ? session.acceptanceNote : "无。",
        "",
        "## 验收结论",
        `- 结论：${session.acceptanceStatus === "accepted" ? "通过" : session.acceptanceStatus === "changes_requested" ? "退回修改" : "未验收"}`,
        `- 验收人：${session.acceptanceBy || "尚未验收"}`,
        `- 验收时间：${session.acceptanceAt || "尚未验收"}`,
        "",
      ].join("\n");
      await writeFeatureFile(featureDirectory, "uat.md", uatDocument);
    }
    await writeFeatureFile(featureDirectory, "feature.json", `${JSON.stringify(state, null, 2)}\n`);
    return { workspacePath, relativePath };
  }

  const deliveryExecutions = new Map();
  const deliveryRunner = options.deliveryRunner ?? runLocalDelivery;

  function deliveryConfigForSession(session) {
    const config = resolved.deliveryProjects.get(session.projectId);
    if (!config) {
      throw new ApiError(
        409,
        "DELIVERY_AUTOMATION_NOT_CONFIGURED",
        `Project '${session.projectId}' does not have an acceptance deployment configuration`,
      );
    }
    return config;
  }

  async function executeDelivery(run, config, actor) {
    try {
      const result = await deliveryRunner({
        deliveryId: run.id,
        branch: run.branch,
        workspacePath: run.workspacePath,
        taskIdentifier: run.taskIdentifier,
        config,
        onUpdate: (changes) => database.updateDeliveryRun(run.id, changes),
      });
      database.updateDeliveryRun(run.id, {
        status: "running",
        acceptanceUrl: result.acceptanceUrl,
        implementationPr: result.implementationPr,
        pullRequestNumber: result.prNumbers[0] ?? null,
        workflowRunUrl: result.workflowRun,
        immutableTag: result.immutableTag,
        mergedSha: result.mergedSha,
        error: null,
      });
      let session = requireProductSession(run.productSessionId);
      let task = database.getTask(run.taskId);
      if (!task) throw new Error("The technical task no longer exists");
      if (task.status === "done" || task.status === "canceled") {
        throw new Error("The technical task was closed while deployment was running");
      }
      if (task.status !== "in_review") {
        task = database.updateTask(
          task.id,
          task.version,
          { status: "in_review" },
          null,
          null,
          actor,
        );
      }
      session = database.submitForProductAcceptance(session.id, {
        note: `自动验收部署完成。${task.identifier} 已部署到共享验收环境。`,
        implementationPr: result.implementationPr,
        testDeployment: {
          url: result.acceptanceUrl,
          workflowRun: result.workflowRun,
          immutableTag: result.immutableTag,
          prNumbers: result.prNumbers,
        },
      }, actor);
      await syncFeatureArtifacts(session);
      database.updateDeliveryRun(run.id, {
        status: "succeeded",
        completedAt: new Date().toISOString(),
      });
      events.emit("task.updated", { task });
      events.emit("product.delivery.updated", { projectId: session.projectId, deliveryRunId: run.id });
    } catch (error) {
      const message = (error instanceof Error ? error.message : String(error)).slice(0, 2_000);
      database.updateDeliveryRun(run.id, {
        status: "failed",
        error: message,
        completedAt: new Date().toISOString(),
      });
      const task = database.getTask(run.taskId);
      const session = database.getProductSession(run.productSessionId);
      if (task?.status === "in_review" && !session?.deliverySubmittedAt) {
        const reverted = database.updateTask(
          task.id,
          task.version,
          { status: "in_progress" },
          null,
          null,
          actor,
        );
        events.emit("task.updated", { task: reverted });
      }
      events.emit("product.delivery.updated", {
        projectId: session?.projectId,
        deliveryRunId: run.id,
      });
    } finally {
      deliveryExecutions.delete(run.id);
    }
  }

  function startDelivery(sessionId, actor) {
    const session = requireProductSession(sessionId);
    if (session.technicalApprovedDocument === null) {
      throw new ApiError(
        409,
        "TECHNICAL_DOCUMENT_NOT_APPROVED",
        "Approve the technical document before deploying for product acceptance",
      );
    }
    const task = database.getTask(session.technicalTaskId);
    if (!task) {
      throw new ApiError(409, "TECHNICAL_TASK_MISSING", "The technical task no longer exists");
    }
    if (task.status === "done" || task.status === "canceled") {
      throw new ApiError(409, "DELIVERY_ALREADY_CLOSED", "Closed delivery tasks cannot be deployed");
    }
    const branch = deliveryBranchForTask(task);
    if (!branch) {
      throw new ApiError(
        409,
        "DELIVERY_BRANCH_REQUIRED",
        "Bind the technical task to its development branch before deploying",
      );
    }
    const workspacePath = deliveryWorkspaceForTask(task);
    if (!workspacePath) {
      throw new ApiError(
        409,
        "DELIVERY_WORKTREE_REQUIRED",
        "Bind the technical task to its development worktree before deploying",
      );
    }
    const config = deliveryConfigForSession(session);
    const active = database.getLatestDeliveryRunForSession(session.id);
    if (active && ["queued", "dispatching", "running"].includes(active.status)) return active;
    const run = database.createDeliveryRun({
      productSessionId: session.id,
      taskId: task.id,
      taskIdentifier: task.identifier,
      workspacePath,
      branch,
      repository: "local",
      workflow: config.deployScript,
      workflowRef: "worktree",
      baseRef: "local",
      deployChannel: "local-acceptance",
      acceptanceUrl: config.acceptanceUrl,
      createdBy: actor.name,
    });
    const execution = executeDelivery(run, config, actor);
    deliveryExecutions.set(run.id, execution);
    return run;
  }

  function maybeStartDelivery(previousTask, task, actor) {
    if (previousTask.status === "in_review" || task.status !== "in_review") return;
    const session = database.getProductSessionByTechnicalTaskId(task.id);
    if (!session || !resolved.deliveryProjects.has(session.projectId)) return;
    if (!deliveryBranchForTask(task) || session.technicalApprovedDocument === null) return;
    try {
      startDelivery(session.id, actor);
    } catch (error) {
      events.emit("product.delivery.updated", {
        projectId: session.projectId,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  function sanitizeTechnicalArtifact(session, content) {
    if (!content || !session.technicalAiThreadId) return content;
    const thread = aiChat.getThread(session.technicalAiThreadId);
    const prefix = `${thread.origin.workspacePath.replace(/\/$/, "")}/`;
    return content.replaceAll(prefix, "");
  }

  function productSessionForResponse(session) {
    const {
      aiThreadId: _aiThreadId,
      technicalAiThreadId: _technicalAiThreadId,
      ...safeSession
    } = session;
    return {
      ...safeSession,
      artifactPath: featureArtifactRelativePath(session),
      technicalDocument: sanitizeTechnicalArtifact(session, safeSession.technicalDocument),
      technicalApprovedDocument: sanitizeTechnicalArtifact(
        session,
        safeSession.technicalApprovedDocument,
      ),
    };
  }

  function productRunForResponse(run) {
    const { threadId: _threadId, ...safeRun } = run;
    return safeRun;
  }

  function productAgentSettingsForResponse(thread) {
    if (!thread) return null;
    return {
      model: thread.model,
      reasoningEffort: thread.reasoningEffort,
    };
  }

  function productEventForResponse(event) {
    const { threadId: _threadId, runId: _runId, ...safeEvent } = event;
    return safeEvent;
  }

  async function ensureTechnicalThread(session) {
    if (session.technicalAiThreadId) return session;
    if (session.status !== "handed_off" || !session.technicalTaskId) {
      throw new ApiError(
        409,
        "PRODUCT_SESSION_NOT_HANDED_OFF",
        "Hand off the product specification before starting technical design",
      );
    }
    const thread = await aiChat.createThread({
      projectId: session.projectId,
      issueId: session.technicalTaskId,
      title: `技术方案 · ${session.title}`,
      sandbox: "read-only",
    });
    try {
      return database.attachTechnicalAiThread(session.id, thread.id);
    } catch (error) {
      await aiChat.deleteThread(thread.id);
      throw error;
    }
  }

  function requireProductSession(id) {
    const session = database.getProductSession(id);
    if (!session) {
      throw new ApiError(404, "PRODUCT_SESSION_NOT_FOUND", `Product session '${id}' does not exist`);
    }
    return session;
  }

  function productSessionSnapshot(id) {
    const session = requireProductSession(id);
    const snapshot = aiChat.getThreadSnapshot(session.aiThreadId);
    const technicalSnapshot = session.technicalAiThreadId
      ? aiChat.getThreadSnapshot(session.technicalAiThreadId)
      : { thread: null, runs: [], events: [] };
    const technicalWorkspacePrefix = technicalSnapshot.thread?.origin.workspacePath
      ? `${technicalSnapshot.thread.origin.workspacePath.replace(/\/$/, "")}/`
      : null;
    const visibleEvents = snapshot.events.filter((event) => (
      event.role === "user"
      || event.role === "assistant"
      || event.role === "error"
    ));
    return {
      session: productSessionForResponse(session),
      deliveryRun: database.getLatestDeliveryRunForSession(session.id),
      productAgent: productAgentSettingsForResponse(snapshot.thread),
      technicalAgent: productAgentSettingsForResponse(technicalSnapshot.thread),
      runs: snapshot.runs.map(productRunForResponse),
      technicalRuns: technicalSnapshot.runs.map(productRunForResponse),
      technicalEvents: technicalSnapshot.events
        .filter((event) => (
          event.role === "user" || event.role === "assistant" || event.role === "error"
        ))
        .map((event) => {
          if (event.role !== "user") {
            const visibleEvent = technicalWorkspacePrefix && event.content.includes(technicalWorkspacePrefix)
              ? { ...event, content: event.content.replaceAll(technicalWorkspacePrefix, "") }
              : event;
            return productEventForResponse(visibleEvent);
          }
          const markerIndex = event.content.lastIndexOf(technicalMessageMarker);
          const visibleEvent = markerIndex < 0
            ? event
            : { ...event, content: event.content.slice(markerIndex + technicalMessageMarker.length) };
          return productEventForResponse(visibleEvent);
        }),
      events: visibleEvents.map((event) => {
        if (event.role !== "user") return productEventForResponse(event);
        const markerIndex = event.content.lastIndexOf(productMessageMarker);
        const visibleEvent = markerIndex < 0
          ? event
          : { ...event, content: event.content.slice(markerIndex + productMessageMarker.length) };
        return productEventForResponse(visibleEvent);
      }),
    };
  }

  const projectSummary = new ProjectSummaryService({
    database,
    codexExecutable: resolved.codexExecutable,
    processEnv: codexProcessEnvironment,
    workspacePath: PROJECT_ROOT,
  });
  const aiEventResponses = new Set();
  const codexSessionSearches = new Map();
  const codexSessionStateCache = new Map();
  const codexSessionsDirectory = path.join(path.dirname(resolved.codexStatePath), "sessions");

  async function findCodexSession(threadId) {
    const cached = codexSessionSearches.get(threadId);
    if (cached && (cached.path || Date.now() - cached.checkedAt < 5_000)) return cached.path;

    const suffix = `-${threadId}.jsonl`;
    const directories = [codexSessionsDirectory];
    while (directories.length > 0) {
      const directory = directories.pop();
      let entries;
      try {
        entries = await readdir(directory, { withFileTypes: true });
      } catch (error) {
        if (error.code === "ENOENT") continue;
        throw error;
      }
      for (const entry of entries) {
        const entryPath = path.join(directory, entry.name);
        if (entry.isDirectory()) {
          directories.push(entryPath);
        } else if (entry.isFile() && entry.name.endsWith(suffix)) {
          codexSessionSearches.set(threadId, { path: entryPath, checkedAt: Date.now() });
          return entryPath;
        }
      }
    }

    codexSessionSearches.set(threadId, { path: null, checkedAt: Date.now() });
    return null;
  }

  async function readCodexSessionState(threadId) {
    const sessionPath = await findCodexSession(threadId);
    if (!sessionPath) return null;

    const sessionStat = await stat(sessionPath);
    const cached = codexSessionStateCache.get(sessionPath);
    if (cached?.size === sessionStat.size && cached.mtimeMs === sessionStat.mtimeMs) {
      return cached.state;
    }

    const length = Math.min(sessionStat.size, CODEX_PLAN_TAIL_BYTES);
    const buffer = Buffer.alloc(length);
    const handle = await open(sessionPath, "r");
    try {
      await handle.read(buffer, 0, length, sessionStat.size - length);
    } finally {
      await handle.close();
    }

    const lines = buffer.toString("utf8").split("\n");
    if (length < sessionStat.size) lines.shift();
    const records = [];
    for (const line of lines) {
      try {
        records.push(JSON.parse(line));
      } catch {}
    }

    let runningTurnId = null;
    for (const record of records) {
      const payload = record?.payload;
      if (record?.type !== "event_msg" || typeof payload?.turn_id !== "string") continue;
      if (payload.type === "task_started") runningTurnId = payload.turn_id;
      if (
        (payload.type === "task_complete" || payload.type === "turn_aborted")
        && payload.turn_id === runningTurnId
      ) {
        runningTurnId = null;
      }
    }

    let progress = null;
    for (let index = records.length - 1; index >= 0; index -= 1) {
      const record = records[index];
      const payload = record?.payload;
      if (payload?.type !== "custom_tool_call" || typeof payload.input !== "string") continue;

      let statuses = [];
      if (payload.name === "update_plan") {
        try {
          const input = JSON.parse(payload.input);
          statuses = Array.isArray(input.plan)
            ? input.plan.map((item) => item?.status).filter(Boolean)
            : [];
        } catch {}
      } else if (payload.name === "exec") {
        const callIndex = payload.input.lastIndexOf("tools.update_plan(");
        if (callIndex < 0) continue;
        statuses = [...payload.input.slice(callIndex).matchAll(
          /["']?status["']?\s*:\s*["'](completed|in_progress|pending)["']/g,
        )].map((match) => match[1]);
      }

      if (statuses.length > 0) {
        progress = {
          completed: statuses.filter((status) => status === "completed").length,
          total: statuses.length,
        };
        break;
      }
    }

    const state = {
      completed: progress?.completed ?? null,
      total: progress?.total ?? null,
      running: runningTurnId !== null,
    };
    codexSessionStateCache.set(sessionPath, {
      size: sessionStat.size,
      mtimeMs: sessionStat.mtimeMs,
      state,
    });
    return state;
  }

  const server = createServer(async (request, response) => {
    response.setHeader("x-content-type-options", "nosniff");
    response.setHeader("referrer-policy", "no-referrer");
    try {
      const incomingUrl = new URL(request.url, "http://127.0.0.1");
      const requestOrigin = request.headers.origin;
      const taskctlRequest = request.headers["x-taskboard-client"] === "taskctl";
      const hasInstanceRoute = incomingUrl.pathname === routePrefix
        || incomingUrl.pathname.startsWith(`${routePrefix}/`);
      const requiresInstanceRoute = resolved.instanceToken
        && (!resolved.publicAuthEnabled
          || requestOrigin === "app://-"
          || ((requestOrigin === "null" || taskctlRequest) && hasInstanceRoute));
      if (requiresInstanceRoute && incomingUrl.pathname !== "/health") {
        if (incomingUrl.pathname === routePrefix) {
          response.writeHead(301, { location: `${incomingUrl.pathname}/${incomingUrl.search}` });
          response.end();
          return;
        }
        if (
          incomingUrl.pathname !== routePrefix
          && !incomingUrl.pathname.startsWith(`${routePrefix}/`)
        ) {
          throw new ApiError(404, "NOT_FOUND", "Route not found");
        }
        request.url = `${incomingUrl.pathname.slice(routePrefix.length) || "/"}${incomingUrl.search}`;
      }

      const requestPathname = new URL(request.url, "http://127.0.0.1").pathname;
      const launcherRequest = resolved.instanceToken
        && (requestOrigin === "app://-" || requestOrigin === "null" || taskctlRequest)
        && hasInstanceRoute;
      const publicAuthConfig = {
        sharedSecret: resolved.publicSharedSecret,
        users: resolved.publicUsers,
      };
      if (resolved.publicAuthEnabled && launcherRequest) {
        request.taskboardPublicRole = "admin";
      }
      if (resolved.publicAuthEnabled && requestPathname === "/login") {
        if (request.method === "GET" || request.method === "HEAD") {
          const body = publicLoginPage();
          if (request.method === "HEAD") {
            response.writeHead(200, {
              "cache-control": "no-store",
              "content-length": Buffer.byteLength(body),
              "content-type": "text/html; charset=utf-8",
            });
            return response.end();
          }
          return sendHtml(response, 200, body);
        }
        if (request.method === "POST") {
          const contentType = request.headers["content-type"]?.split(";", 1)[0].trim().toLowerCase();
          if (contentType !== "application/x-www-form-urlencoded") {
            return sendHtml(response, 415, publicLoginPage());
          }
          const body = await readBody(request, 16 * 1024, "Login request is too large");
          const form = new URLSearchParams(body.toString("utf8"));
          const username = form.get("username") ?? "";
          const identity = validatePublicCredentials({
            username,
            password: form.get("password") ?? "",
          }, publicAuthConfig);
          if (!identity) return sendHtml(response, 401, publicLoginPage({ username, invalid: true }));
          appendSetCookie(response, publicCookie(
            request,
            PUBLIC_SESSION_COOKIE,
            createPublicSessionToken(identity, publicAuthConfig),
            { maxAge: Math.floor(PUBLIC_SESSION_TTL_MS / 1000) },
          ));
          appendSetCookie(response, publicCookie(request, PUBLIC_LOGGED_OUT_COOKIE, "", {
            maxAge: 0,
            httpOnly: false,
          }));
          return sendEmpty(response, 303, { location: "./" });
        }
        return methodNotAllowed(response, ["GET", "HEAD", "POST"]);
      }
      if (resolved.publicAuthEnabled && requestPathname === "/logout") {
        if (request.method !== "POST") return methodNotAllowed(response, ["POST"]);
        appendSetCookie(response, publicCookie(request, PUBLIC_SESSION_COOKIE, "", { maxAge: 0 }));
        appendSetCookie(response, publicCookie(request, PUBLIC_LOGGED_OUT_COOKIE, "1", {
          maxAge: Math.floor(PUBLIC_SESSION_TTL_MS / 1000),
          httpOnly: false,
        }));
        return sendEmpty(response, 204);
      }
      if (resolved.publicAuthEnabled && !launcherRequest && requestPathname !== "/health") {
        const cookies = parseCookies(request);
        let identity = readPublicSession(request, publicAuthConfig);
        if (!identity && cookies.get(PUBLIC_LOGGED_OUT_COOKIE) === "1") {
          const acceptsHtml = String(requestHeader(request, "accept") ?? "").includes("text/html");
          if ((request.method === "GET" || request.method === "HEAD") && acceptsHtml) {
            return sendEmpty(response, 303, { location: "login" });
          }
          return sendJson(response, 401, {
            error: { code: "SIGNED_OUT", message: "Sign in to continue" },
          });
        }
        if (!identity) {
          const acceptsHtml = String(requestHeader(request, "accept") ?? "").includes("text/html");
          if ((request.method === "GET" || request.method === "HEAD") && acceptsHtml) {
            return sendEmpty(response, 303, { location: "login" });
          }
          identity = authenticatePublicRequest(request, publicAuthConfig);
          if (request.headers["x-taskboard-client"] !== "taskctl") {
            appendSetCookie(response, publicCookie(
              request,
              PUBLIC_SESSION_COOKIE,
              createPublicSessionToken(identity, publicAuthConfig),
              { maxAge: Math.floor(PUBLIC_SESSION_TTL_MS / 1000) },
            ));
          }
        }
        request.taskboardActor = identity.actor;
        request.taskboardPublicRole = identity.role;
      }

      const configuredTrustedRequest = assertTrustedNetworkRequest(
        request,
        Boolean(resolved.instanceToken),
        resolved.trustedOrigins,
      );
      const origin = requestOrigin;
      const trustedEmbedOrigin = TRUSTED_EMBED_ORIGINS.has(origin)
        || (Boolean(resolved.instanceToken) && origin === "null");
      if (trustedEmbedOrigin) {
        response.setHeader("access-control-allow-origin", origin);
        response.setHeader("access-control-allow-methods", "GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS");
        response.setHeader(
          "access-control-allow-headers",
          request.headers["access-control-request-headers"] ?? "content-type",
        );
        response.setHeader("access-control-expose-headers", "x-codex-taskboard-proof");
        response.setHeader("access-control-allow-private-network", "true");
        response.setHeader("vary", "origin");
        if (request.method === "OPTIONS") {
          response.writeHead(204);
          response.end();
          return;
        }
      }
      if (resolved.instanceToken && origin === "app://-") {
        const challenge = request.headers["x-codex-taskboard-challenge"];
        if (typeof challenge !== "string" || !/^[a-f0-9]{32,128}$/i.test(challenge)) {
          throw new ApiError(401, "INVALID_INSTANCE_CHALLENGE", "Launcher challenge is required");
        }
        response.setHeader(
          "x-codex-taskboard-proof",
          createHmac("sha256", resolved.instanceSecret).update(challenge).digest("hex"),
        );
      }
      const url = new URL(request.url, "http://127.0.0.1");
      const pathname = url.pathname;
      if (
        resolved.publicAuthEnabled
        && !publicRoleCanWrite(
          request.taskboardPublicRole,
          request.method ?? "GET",
          pathname,
        )
      ) {
        throw new ApiError(
          403,
          "PUBLIC_ROLE_FORBIDDEN",
          "The authenticated public role cannot perform this change",
        );
      }
      const isLocalAiRoute = pathname === "/api/local/ai" || pathname.startsWith("/api/local/ai/");
      const isDevelopmentContextsRoute = /^\/api\/projects\/[^/]+\/development-contexts$/.test(pathname);
      if (
        (configuredTrustedRequest || resolved.publicAuthEnabled)
        && (
          pathname.startsWith("/api/local/")
          || pathname === "/api/device-workspaces"
          || isDevelopmentContextsRoute
        )
      ) {
        throw new ApiError(
          409,
          "LOCAL_COMPANION_REQUIRED",
          "This capability requires a device-local Taskboard origin",
        );
      }
      if (isLocalAiRoute) {
        assertAiLoopbackRequest(request);
      } else if (pathname.startsWith("/api/local/")) {
        assertLoopbackRequest(request);
      }
      const isMachineCapabilityRoute = pathname === "/api/meta"
        || pathname === "/api/device-workspaces"
        || isDevelopmentContextsRoute;
      const capabilityCloudConfig = isMachineCapabilityRoute
        ? await cloudConfig.read()
        : null;
      if (capabilityCloudConfig?.remoteUrl) assertLoopbackRequest(request);

      if (pathname === "/health") {
        if (request.method !== "GET") return methodNotAllowed(response, ["GET"]);
        if (resolved.instanceToken) {
          const challenge = request.headers["x-codex-taskboard-challenge"];
          if (typeof challenge !== "string" || !/^[a-f0-9]{32,128}$/i.test(challenge)) {
            throw new ApiError(401, "INVALID_INSTANCE_CHALLENGE", "Launcher challenge is required");
          }
          return sendJson(response, 200, {
            status: "ok",
            product: "codex-taskboard",
            version: resolved.version,
            proof: createHmac("sha256", resolved.instanceSecret)
              .update(challenge)
              .digest("hex"),
          });
        }
        return sendJson(response, 200, { status: "ok" });
      }

      if (pathname === "/api/client-storage") {
        if (request.method === "GET") {
          await clientStorageWrite;
          const entries = await readClientStorage();
          const config = await cloudConfig.read();
          if (config.remoteUrl) {
            assertLoopbackRequest(request);
            const shared = await readCloudJson("/api/client-storage");
            for (const key of Object.keys(entries)) {
              if (key.startsWith(PROJECT_BOARD_DISPLAY_SETTINGS_KEY_PREFIX)) delete entries[key];
            }
            for (const [key, value] of Object.entries(shared.entries)) {
              if (key.startsWith(PROJECT_BOARD_DISPLAY_SETTINGS_KEY_PREFIX)) entries[key] = value;
            }
          }
          return sendJson(response, 200, { entries });
        }
        if (request.method === "PATCH") {
          const update = parseClientStorageUpdate(await readJson(request));
          const config = await cloudConfig.read();
          if (
            config.remoteUrl
            && update.key.startsWith(PROJECT_BOARD_DISPLAY_SETTINGS_KEY_PREFIX)
          ) {
            assertLoopbackRequest(request);
            return sendFetchResponse(
              response,
              await cloudProxy.forward(new Request("http://127.0.0.1/api/client-storage", {
                method: "PATCH",
                headers: { "content-type": "application/json" },
                body: JSON.stringify(update),
              })),
            );
          }
          await updateClientStorage(update);
          if (update.key.startsWith(PROJECT_BOARD_DISPLAY_SETTINGS_KEY_PREFIX)) {
            events.emit("client-storage.updated", { key: update.key });
          }
          return sendEmpty(response, 204);
        }
        return methodNotAllowed(response, ["GET", "PATCH"]);
      }

      if (pathname === "/api/local/codex-thread-progress") {
        if (request.method !== "GET") return methodNotAllowed(response, ["GET"]);
        if ([...url.searchParams.keys()].some((key) => key !== "threadId")) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "Only 'threadId' is supported");
        }
        const threadIds = [...new Set(url.searchParams.getAll("threadId").map((value) => (
          value.trim().replace(/^(?:local|cloud):/i, "")
        )))];
        if (threadIds.length > 64 || threadIds.some((threadId) => (
          !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(threadId)
        ))) {
          throw new ApiError(400, "INVALID_FIELD", "'threadId' must contain valid Codex thread IDs");
        }
        const entries = await Promise.all(threadIds.map(async (threadId) => (
          [threadId, await readCodexSessionState(threadId)]
        )));
        return sendJson(response, 200, { progress: Object.fromEntries(entries) });
      }

      if (pathname === "/api/local/host-runtime") {
        if (request.method === "GET") {
          const runtime = hostRuntime && Date.now() - hostRuntime.updatedAt <= HOST_RUNTIME_TTL_MS
            ? hostRuntime
            : null;
          return sendJson(response, 200, { runtime });
        }
        if (request.method === "PUT") {
          const body = await readJson(request);
          assertPlainObject(body);
          assertAllowedKeys(body, new Set([
            "threadId",
            "threadRunning",
            "threadTodoProgress",
            "codexProjectId",
            "codexProjectKind",
            "codexHostId",
            "workspacePath",
          ]));
          const threadId = stringField(body.threadId, "threadId", { required: true, maxLength: 256 });
          if (typeof body.threadRunning !== "boolean") {
            throw new ApiError(400, "INVALID_FIELD", "'threadRunning' must be a boolean");
          }
          let threadTodoProgress = null;
          if (body.threadTodoProgress != null) {
            assertPlainObject(body.threadTodoProgress);
            assertAllowedKeys(body.threadTodoProgress, new Set(["completed", "total"]));
            const { completed, total } = body.threadTodoProgress;
            if (!Number.isInteger(completed) || !Number.isInteger(total) || completed < 0 || total < 1) {
              throw new ApiError(400, "INVALID_FIELD", "'threadTodoProgress' is invalid");
            }
            threadTodoProgress = { completed: Math.min(completed, total), total };
          }
          hostRuntime = {
            threadId,
            threadRunning: body.threadRunning,
            threadTodoProgress,
            codexProjectId: stringField(body.codexProjectId ?? null, "codexProjectId", {
              nullable: true,
              maxLength: 256,
            }),
            codexProjectKind: body.codexProjectKind === "local" || body.codexProjectKind === "remote"
              ? body.codexProjectKind
              : null,
            codexHostId: stringField(body.codexHostId ?? null, "codexHostId", {
              nullable: true,
              maxLength: 256,
            }),
            workspacePath: stringField(body.workspacePath ?? null, "workspacePath", {
              nullable: true,
              maxLength: 4096,
            }),
            updatedAt: Date.now(),
          };
          return sendJson(response, 200, { runtime: hostRuntime });
        }
        return methodNotAllowed(response, ["GET", "PUT"]);
      }

      if (pathname === "/api/local/cloud-session") {
        if ([...url.searchParams.keys()].length > 0) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "Cloud session routes do not accept query parameters");
        }
        if (request.method === "GET") {
          const config = await cloudConfig.read();
          return sendJson(response, 200, config.remoteUrl
            ? {
              mode: "cloud",
              remoteUrl: config.remoteUrl,
              actorName: config.actorName,
              authenticated: true,
            }
            : { mode: "local", authenticated: false });
        }
        if (request.method === "PUT") {
          const body = await readJson(request);
          assertPlainObject(body);
          assertAllowedKeys(body, new Set(["remoteUrl", "actorName", "sharedKey"]));
          try {
            const config = await cloudConfig.configure({
              remoteUrl: body.remoteUrl,
              actorName: body.actorName,
              sharedKey: body.sharedKey,
            });
            return sendJson(response, 200, {
              mode: "cloud",
              remoteUrl: config.remoteUrl,
              actorName: config.actorName,
              authenticated: true,
            });
          } catch (error) {
            throw new ApiError(400, error.code ?? "INVALID_CLOUD_CONFIG", error.message);
          }
        }
        if (request.method === "DELETE") {
          await cloudConfig.clearCloud();
          return sendJson(response, 200, { mode: "local", authenticated: false });
        }
        return methodNotAllowed(response, ["GET", "PUT", "DELETE"]);
      }

      if (pathname === "/api/local/jira-connection") {
        if ([...url.searchParams.keys()].length > 0) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "Jira 连接接口不接受查询参数");
        }
        if (request.method === "GET") {
          return sendJson(response, 200, { connection: await jira.status() });
        }
        if (request.method === "PUT") {
          const activeCloudConfig = await cloudConfig.read();
          if (activeCloudConfig.remoteUrl) {
            throw new ApiError(
              409,
              "JIRA_LOCAL_MODE_REQUIRED",
              "Jira 连接当前仅支持本地数据模式，请先退出云端协作模式",
            );
          }
          const body = await readJson(request);
          assertPlainObject(body);
          assertAllowedKeys(body, new Set(["baseUrl", "username", "password", "projects"]));
          const baseUrl = stringField(body.baseUrl, "baseUrl", { required: true, maxLength: 2048 });
          const username = stringField(body.username ?? "", "username", { maxLength: 254 });
          const password = body.password ?? "";
          if (typeof password !== "string") {
            throw new ApiError(400, "INVALID_FIELD", "'password' must be a string");
          }
          if (password.length > 4096) {
            throw new ApiError(400, "INVALID_FIELD", "'password' cannot exceed 4096 characters");
          }
          try {
            const connection = await jira.configure({
              baseUrl,
              username,
              password,
              projects: body.projects,
            });
            events.emit("project.labels.updated", { project: database.getProject(JIRA_PROJECT_ID) });
            return sendJson(response, 200, { connection });
          } catch (error) {
            if (error instanceof ApiError) throw error;
            throw new ApiError(400, error.code ?? "INVALID_JIRA_CONFIG", error.message);
          }
        }
        return methodNotAllowed(response, ["GET", "PUT"]);
      }

      if (pathname === "/api/local/jira-connection/sync") {
        if (request.method !== "POST") return methodNotAllowed(response, ["POST"]);
        if ([...url.searchParams.keys()].length > 0) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "Jira 同步接口不接受查询参数");
        }
        await assertEmptyRequestBody(request, "POST /api/local/jira-connection/sync");
        const connection = await jira.sync({ force: true });
        events.emit("project.labels.updated", { project: database.getProject(JIRA_PROJECT_ID) });
        return sendJson(response, 200, { connection });
      }

      const projectMappingRoute = pathname.match(/^\/api\/local\/project-mappings\/([^/]+)$/);
      if (projectMappingRoute) {
        if (request.method !== "PUT") return methodNotAllowed(response, ["PUT"]);
        if ([...url.searchParams.keys()].length > 0) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "Project mapping routes do not accept query parameters");
        }
        let projectId;
        try {
          projectId = decodeURIComponent(projectMappingRoute[1]);
        } catch {
          throw new ApiError(400, "INVALID_PATH", "Project id contains invalid encoding");
        }
        validateProjectId(projectId);
        const body = await readJson(request);
        assertPlainObject(body);
        assertAllowedKeys(body, new Set(["workspacePath"]));
        const workspacePath = pathField(body.workspacePath, "workspacePath");
        if (!workspacePath || !path.isAbsolute(workspacePath)) {
          throw new ApiError(400, "INVALID_FIELD", "'workspacePath' must be absolute");
        }
        await cloudConfig.setProjectWorkspace(projectId, workspacePath);
        return sendJson(response, 200, { projectId, workspacePath });
      }

      if (pathname === "/api/meta") {
        if (request.method !== "GET") return methodNotAllowed(response, ["GET"]);
        if ([...url.searchParams.keys()].length > 0) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "GET /api/meta does not accept query parameters");
        }
        return sendJson(response, 200, {
          ...(configuredTrustedRequest || resolved.publicAuthEnabled
            ? {}
            : { manageTaskboardSkillPath: resolved.skillPath }),
          ...(resolved.publicAuthEnabled ? { currentActor: actorFromRequest(request) } : {}),
          capabilities: {
            localAiChat: !configuredTrustedRequest
              && !resolved.publicAuthEnabled
              && isLoopbackAddress(request.socket.remoteAddress),
            ...(!capabilityCloudConfig?.remoteUrl ? { productCollaboration: true } : {}),
            ...(resolved.publicAuthEnabled ? {
              publicAccess: true,
              publicRole: request.taskboardPublicRole,
              productCollaborationWrite: request.taskboardPublicRole !== "technical",
              technicalCollaborationWrite: request.taskboardPublicRole !== "product",
              taskWrite: request.taskboardPublicRole !== "product",
            } : {}),
          },
          ...(capabilityCloudConfig?.remoteUrl
            ? {
              mode: "cloud",
              realtime: {
                transport: "websocket",
                endpoint: "/api/events",
              },
              localCapabilities: { available: !configuredTrustedRequest },
            }
            : {}),
        });
      }

      if (pathname.startsWith("/api/product-sessions")) {
        const productCloudConfig = await cloudConfig.read();
        if (productCloudConfig.remoteUrl) {
          throw new ApiError(
            409,
            "PRODUCT_COLLABORATION_LOCAL_MODE_REQUIRED",
            "Product collaboration is currently available only in local data mode",
          );
        }
      }

      if (pathname === "/api/product-collaboration/catalog") {
        if (request.method !== "GET") return methodNotAllowed(response, ["GET"]);
        assertAllowedQuery(
          url.searchParams,
          new Set(["projectId"]),
          "GET /api/product-collaboration/catalog",
        );
        const projectId = validateProjectId(url.searchParams.get("projectId") ?? undefined);
        const catalog = await aiChat.getCatalog(projectId);
        return sendJson(response, 200, { models: catalog.models });
      }

      if (pathname === "/api/product-sessions") {
        if (request.method === "GET") {
          const query = parseProductSessionList(url.searchParams);
          const result = database.listProductSessions(query.projectId, query);
          return sendJson(response, 200, {
            ...result,
            data: result.data.map(productSessionForResponse),
          });
        }
        if (request.method === "POST") {
          assertNoQuery(url.searchParams, "POST /api/product-sessions");
          const input = parseProductSessionCreate(await readJson(request));
          const actor = actorFromRequest(request);
          const sessionId = randomUUID();
          const thread = await aiChat.createThread({
            projectId: input.projectId,
            title: input.title,
            model: input.model,
            reasoningEffort: input.reasoningEffort,
            sandbox: "read-only",
          });
          try {
            const session = database.createProductSession({
              id: sessionId,
              ...input,
              aiThreadId: thread.id,
              actor,
            });
            await syncFeatureArtifacts(session);
            return sendJson(response, 201, { session: productSessionForResponse(session) });
          } catch (error) {
            await aiChat.deleteThread(thread.id);
            throw error;
          }
        }
        return methodNotAllowed(response, ["GET", "POST"]);
      }

      const productSessionEventsRoute = pathname.match(/^\/api\/product-sessions\/([^/]+)\/events$/);
      if (productSessionEventsRoute) {
        if (request.method !== "GET") return methodNotAllowed(response, ["GET"]);
        assertNoQuery(url.searchParams, "GET /api/product-sessions/:id/events");
        const sessionId = decodeRouteSegment(productSessionEventsRoute[1], "Product session id");
        const session = requireProductSession(sessionId);
        await aiChat.getThreadSnapshot(session.aiThreadId);
        response.writeHead(200, {
          connection: "keep-alive",
          "cache-control": "no-cache, no-transform",
          "content-type": "text/event-stream; charset=utf-8",
          "x-accel-buffering": "no",
        });
        aiEventResponses.add(response);
        const notify = () => {
          response.write('event: product.session\ndata: {"type":"product.session"}\n\n');
        };
        const unsubscribers = [aiChat.subscribe(session.aiThreadId, notify)];
        if (session.technicalAiThreadId) {
          unsubscribers.push(aiChat.subscribe(session.technicalAiThreadId, notify));
        }
        response.write(": connected\n\n");
        response.write('event: product.session\ndata: {"type":"product.session"}\n\n');
        const keepAlive = setInterval(() => response.write(": keep-alive\n\n"), 20_000);
        keepAlive.unref();
        request.once("close", () => {
          clearInterval(keepAlive);
          for (const unsubscribe of unsubscribers) unsubscribe();
          aiEventResponses.delete(response);
        });
        return;
      }

      const productAgentSettingsRoute = pathname.match(
        /^\/api\/product-sessions\/([^/]+)\/(product-agent-settings|technical-agent-settings)$/,
      );
      if (productAgentSettingsRoute) {
        if (request.method !== "PATCH") return methodNotAllowed(response, ["PATCH"]);
        assertNoQuery(url.searchParams, "PATCH /api/product-sessions/:id/:agent-settings");
        const sessionId = decodeRouteSegment(productAgentSettingsRoute[1], "Product session id");
        const input = parseProductAgentSettings(await readJson(request));
        let session = requireProductSession(sessionId);
        const technical = productAgentSettingsRoute[2] === "technical-agent-settings";
        if (technical) session = await ensureTechnicalThread(session);
        const threadId = technical ? session.technicalAiThreadId : session.aiThreadId;
        const thread = await aiChat.updateThread(threadId, input);
        return sendJson(response, 200, {
          settings: productAgentSettingsForResponse(thread),
        });
      }

      const productSessionTurnsRoute = pathname.match(/^\/api\/product-sessions\/([^/]+)\/turns$/);
      if (productSessionTurnsRoute) {
        if (request.method !== "POST") return methodNotAllowed(response, ["POST"]);
        assertNoQuery(url.searchParams, "POST /api/product-sessions/:id/turns");
        const sessionId = decodeRouteSegment(productSessionTurnsRoute[1], "Product session id");
        const session = requireProductSession(sessionId);
        if (session.status === "approved" || session.status === "handed_off") {
          throw new ApiError(409, "PRODUCT_SESSION_APPROVED", "Approved product sessions cannot receive new turns");
        }
        const turn = parseProductTurn(await readJson(
          request,
          AI_CHAT_TURN_BODY_LIMIT,
          "Product conversation turn cannot exceed 25 MiB",
        ));
        const workspacePath = await requireProductProjectWorkspace(session);
        const sourceBaseline = await resolveProductSourceBaseline(
          workspacePath,
          path.join(resolved.dataDirectory, "product-source-baselines"),
          codexProcessEnvironment,
        );
        const run = await aiChat.startTurn(session.aiThreadId, {
          ...turn,
          message: productAgentPrompt(productSessionWithArtifactPath(session), turn.message, sourceBaseline),
          skillIds: [],
        }, { productSourceDirectory: sourceBaseline.sourceDirectory });
        return sendJson(response, 202, { run: productRunForResponse(run) });
      }

      const productDocumentRoute = pathname.match(/^\/api\/product-sessions\/([^/]+)\/document$/);
      if (productDocumentRoute) {
        if (request.method !== "PUT") return methodNotAllowed(response, ["PUT"]);
        assertNoQuery(url.searchParams, "PUT /api/product-sessions/:id/document");
        const sessionId = decodeRouteSegment(productDocumentRoute[1], "Product session id");
        const input = parseProductDocumentUpdate(await readJson(request));
        const session = database.updateProductSessionDocument(sessionId, input.version, input.content);
        await syncFeatureArtifacts(session);
        return sendJson(response, 200, { session: productSessionForResponse(session) });
      }

      const technicalTurnsRoute = pathname.match(/^\/api\/product-sessions\/([^/]+)\/technical-turns$/);
      if (technicalTurnsRoute) {
        if (request.method !== "POST") return methodNotAllowed(response, ["POST"]);
        assertNoQuery(url.searchParams, "POST /api/product-sessions/:id/technical-turns");
        const sessionId = decodeRouteSegment(technicalTurnsRoute[1], "Product session id");
        let session = requireProductSession(sessionId);
        if (session.technicalApprovedDocument !== null) {
          throw new ApiError(409, "TECHNICAL_DOCUMENT_APPROVED", "Approved technical sessions cannot receive new turns");
        }
        const turn = parseProductTurn(await readJson(
          request,
          AI_CHAT_TURN_BODY_LIMIT,
          "Technical conversation turn cannot exceed 25 MiB",
        ));
        session = await ensureTechnicalThread(session);
        session = database.claimTechnicalOwner(session.id, actorFromRequest(request));
        const run = await aiChat.startTurn(session.technicalAiThreadId, {
          ...turn,
          message: technicalAgentPrompt(session, turn.message),
          skillIds: [],
        });
        return sendJson(response, 202, { run: productRunForResponse(run) });
      }

      const technicalDocumentRoute = pathname.match(/^\/api\/product-sessions\/([^/]+)\/technical-document$/);
      if (technicalDocumentRoute) {
        if (request.method !== "PUT") return methodNotAllowed(response, ["PUT"]);
        assertNoQuery(url.searchParams, "PUT /api/product-sessions/:id/technical-document");
        const sessionId = decodeRouteSegment(technicalDocumentRoute[1], "Product session id");
        const input = parseProductDocumentUpdate(await readJson(request));
        const currentSession = requireProductSession(sessionId);
        const session = database.updateTechnicalDocument(
          sessionId,
          input.version,
          sanitizeTechnicalArtifact(currentSession, input.content),
          actorFromRequest(request),
        );
        await syncFeatureArtifacts(session);
        return sendJson(response, 200, { session: productSessionForResponse(session) });
      }

      const technicalApproveRoute = pathname.match(/^\/api\/product-sessions\/([^/]+)\/technical-approve$/);
      if (technicalApproveRoute) {
        if (request.method !== "POST") return methodNotAllowed(response, ["POST"]);
        assertNoQuery(url.searchParams, "POST /api/product-sessions/:id/technical-approve");
        await assertEmptyRequestBody(request, "POST /api/product-sessions/:id/technical-approve");
        const sessionId = decodeRouteSegment(technicalApproveRoute[1], "Product session id");
        const actor = actorFromRequest(request);
        let session = database.approveTechnicalDocument(sessionId, actor);
        await syncFeatureArtifacts(session);
        const currentTask = database.getTask(session.technicalTaskId);
        if (!currentTask) {
          throw new ApiError(409, "TECHNICAL_TASK_MISSING", "The technical task no longer exists");
        }
        const marker = "\n\n---\n\n# 已确认技术方案\n\n";
        const originalDescription = currentTask.description.split(marker)[0];
        const approvedTechnicalDocument = sanitizeTechnicalArtifact(
          session,
          session.technicalApprovedDocument,
        );
        const description = [
          originalDescription,
          marker.trimStart(),
          approvedTechnicalDocument,
          "",
          `技术方案版本：${session.technicalApprovedVersion}`,
          `技术确认人：${session.technicalApprovedBy}`,
          `技术确认时间：${session.technicalApprovedAt}`,
          "",
          "开发接手要求：严格依据以上冻结版本实施；产品或技术方案发生变化时，先回到协作会话更新制品。",
        ].join("\n");
        const task = database.updateTask(
          currentTask.id,
          currentTask.version,
          {
            description,
            labels: [...new Set([...currentTask.labels, "technical-approved"])],
          },
          null,
          null,
          actor,
        );
        session = database.getProductSession(session.id);
        events.emit("task.updated", { task });
        return sendJson(response, 200, { session: productSessionForResponse(session), task });
      }

      const submitReviewRoute = pathname.match(/^\/api\/product-sessions\/([^/]+)\/submit-review$/);
      const deliveryRoute = pathname.match(/^\/api\/product-sessions\/([^/]+)\/delivery$/);
      if (deliveryRoute) {
        if (request.method !== "POST") return methodNotAllowed(response, ["POST"]);
        assertNoQuery(url.searchParams, "POST /api/product-sessions/:id/delivery");
        await assertEmptyRequestBody(request, "POST /api/product-sessions/:id/delivery");
        const sessionId = decodeRouteSegment(deliveryRoute[1], "Product session id");
        const deliveryRun = startDelivery(sessionId, actorFromRequest(request));
        events.emit("product.delivery.updated", {
          projectId: requireProductSession(sessionId).projectId,
          deliveryRunId: deliveryRun.id,
        });
        return sendJson(response, 202, { deliveryRun });
      }

      if (submitReviewRoute) {
        if (request.method !== "POST") return methodNotAllowed(response, ["POST"]);
        assertNoQuery(url.searchParams, "POST /api/product-sessions/:id/submit-review");
        const sessionId = decodeRouteSegment(submitReviewRoute[1], "Product session id");
        const input = parseDeliverySubmission(await readJson(request));
        const actor = actorFromRequest(request);
        let session = requireProductSession(sessionId);
        const currentTask = database.getTask(session.technicalTaskId);
        if (!currentTask) {
          throw new ApiError(409, "TECHNICAL_TASK_MISSING", "The technical task no longer exists");
        }
        if (currentTask.status === "done" || currentTask.status === "canceled") {
          throw new ApiError(409, "DELIVERY_ALREADY_CLOSED", "Closed delivery tasks cannot be resubmitted");
        }
        const task = database.updateTask(
          currentTask.id,
          currentTask.version,
          { status: "in_review" },
          null,
          null,
          actor,
        );
        session = database.submitForProductAcceptance(session.id, input, actor);
        await syncFeatureArtifacts(session);
        events.emit("task.updated", { task });
        return sendJson(response, 200, { session: productSessionForResponse(session), task });
      }

      const productAcceptanceRoute = pathname.match(/^\/api\/product-sessions\/([^/]+)\/acceptance$/);
      if (productAcceptanceRoute) {
        if (request.method !== "POST") return methodNotAllowed(response, ["POST"]);
        assertNoQuery(url.searchParams, "POST /api/product-sessions/:id/acceptance");
        const sessionId = decodeRouteSegment(productAcceptanceRoute[1], "Product session id");
        const input = parseProductAcceptance(await readJson(request));
        const actor = actorFromRequest(request);
        let session = requireProductSession(sessionId);
        const currentTask = database.getTask(session.technicalTaskId);
        if (!currentTask) {
          throw new ApiError(409, "TECHNICAL_TASK_MISSING", "The technical task no longer exists");
        }
        if (currentTask.status !== "in_review") {
          throw new ApiError(409, "DELIVERY_NOT_IN_REVIEW", "The delivery task must be in review");
        }
        if (input.outcome === "accepted") {
          await requireUpdatedProjectBlueprint(session);
        }
        const task = database.updateTask(
          currentTask.id,
          currentTask.version,
          { status: input.outcome === "accepted" ? "done" : "in_progress" },
          null,
          null,
          actor,
        );
        session = database.recordProductAcceptance(session.id, input.outcome, input.note, actor);
        await syncFeatureArtifacts(session);
        events.emit("task.updated", { task });
        return sendJson(response, 200, { session: productSessionForResponse(session), task });
      }

      const productApproveRoute = pathname.match(/^\/api\/product-sessions\/([^/]+)\/approve$/);
      if (productApproveRoute) {
        if (request.method !== "POST") return methodNotAllowed(response, ["POST"]);
        assertNoQuery(url.searchParams, "POST /api/product-sessions/:id/approve");
        await assertEmptyRequestBody(request, "POST /api/product-sessions/:id/approve");
        const sessionId = decodeRouteSegment(productApproveRoute[1], "Product session id");
        const actor = actorFromRequest(request);
        let session = database.approveProductSession(sessionId, actor);
        await syncFeatureArtifacts(session);
        let task = session.technicalTaskId ? database.getTask(session.technicalTaskId) : null;
        if (!task) {
          task = database.createTask({
            projectId: session.projectId,
            title: session.title,
            description: [
              `产品协作会话：${session.id}`,
              `产品方案版本：${session.approvedVersion}`,
              `产品确认人：${session.approvedBy}`,
              `产品确认时间：${session.approvedAt}`,
              `功能文档目录：${featureArtifactRelativePath(session)}`,
              "",
              "# 已确认产品方案",
              "",
              session.approvedDocument,
              "",
              "---",
              "技术接手要求：读取以上已确认方案，使用项目标准交付 Skill 生成 technical.md；技术方案确认前不要开始实现。",
            ].join("\n"),
            status: "todo",
            priority: "high",
            labels: ["特性", "product-approved"],
            actor,
            assignee: CODEX_AGENT_ACTOR,
            developmentContext: null,
            startDate: null,
            dueDate: null,
            recurrence: null,
          });
          session = database.handoffProductSession(session.id, task.id);
          events.emit("task.created", { task });
        }
        return sendJson(response, 200, { session: productSessionForResponse(session), task });
      }

      const productSessionRoute = pathname.match(/^\/api\/product-sessions\/([^/]+)$/);
      if (productSessionRoute) {
        if (request.method !== "GET") return methodNotAllowed(response, ["GET"]);
        assertNoQuery(url.searchParams, "GET /api/product-sessions/:id");
        const sessionId = decodeRouteSegment(productSessionRoute[1], "Product session id");
        return sendJson(response, 200, productSessionSnapshot(sessionId));
      }

      if (pathname === "/api/local/ai/catalog") {
        if (request.method !== "GET") return methodNotAllowed(response, ["GET"]);
        assertAllowedQuery(url.searchParams, new Set([
          "projectId",
          "codexProjectId",
          "codexProjectKind",
          "codexHostId",
          "workspacePath",
        ]), "GET /api/local/ai/catalog");
        const projectId = validateProjectId(url.searchParams.get("projectId") ?? undefined);
        return sendJson(
          response,
          200,
          await aiChat.getCatalog(projectId, undefined, aiExecutionTargetFromQuery(url.searchParams)),
        );
      }

      if (pathname === "/api/local/ai/composer/candidates") {
        if (request.method !== "GET") return methodNotAllowed(response, ["GET"]);
        const query = parseComposerCandidateQuery(url.searchParams);
        return sendJson(
          response,
          200,
          await aiChat.composerCatalog.candidatesForSurface(
            await aiChat.getComposerCandidates(query),
            query,
          ),
        );
      }

      if (pathname === "/api/local/ai/composer/rebind") {
        if (request.method !== "POST") return methodNotAllowed(response, ["POST"]);
        assertNoQuery(url.searchParams, "POST /api/local/ai/composer/rebind");
        const input = parseComposerRebindRequest(await readJson(request));
        const { workspacePath, composerCatalog } = await resolveComposerRebindWorkspace(aiChat, input);
        return sendJson(
          response,
          200,
          await composerCatalog.rebindPersistedReferences({
            workspacePath,
            nodes: input.document.nodes,
          }),
        );
      }

      const projectSummaryRoute = pathname.match(/^\/api\/local\/projects\/([^/]+)\/summary$/);
      if (projectSummaryRoute) {
        if (request.method !== "GET") return methodNotAllowed(response, ["GET"]);
        assertNoQuery(url.searchParams, "GET /api/local/projects/:id/summary");
        const projectId = validateProjectId(
          decodeRouteSegment(projectSummaryRoute[1], "Project id"),
        );
        return sendJson(response, 200, projectSummary.get(projectId));
      }

      if (pathname === "/api/local/ai/threads") {
        assertNoQuery(url.searchParams, "/api/local/ai/threads");
        if (request.method === "GET") {
          return sendJson(response, 200, { threads: await aiChat.listThreads() });
        }
        if (request.method === "POST") {
          const thread = await aiChat.createThread(parseAiThreadCreate(await readJson(request)));
          return sendJson(response, 201, { thread });
        }
        return methodNotAllowed(response, ["GET", "POST"]);
      }

      const aiThreadEventsRoute = pathname.match(/^\/api\/local\/ai\/threads\/([^/]+)\/events$/);
      if (aiThreadEventsRoute) {
        if (request.method !== "GET") return methodNotAllowed(response, ["GET"]);
        assertNoQuery(url.searchParams, "GET /api/local/ai/threads/:id/events");
        const threadId = decodeRouteSegment(aiThreadEventsRoute[1], "Thread id");
        await aiChat.getThreadSnapshot(threadId);
        response.writeHead(200, {
          connection: "keep-alive",
          "cache-control": "no-cache, no-transform",
          "content-type": "text/event-stream; charset=utf-8",
          "x-accel-buffering": "no",
        });
        aiEventResponses.add(response);
        const unsubscribe = aiChat.subscribe(threadId, (event) => {
          const type = event?.type === "ai.run" ? "ai.run" : "ai.event";
          response.write(`event: ${type}\ndata: ${JSON.stringify(event)}\n\n`);
        });
        response.write(": connected\n\n");
        response.write('event: ai.event\ndata: {"type":"ai.event"}\n\n');
        const keepAlive = setInterval(() => response.write(": keep-alive\n\n"), 20_000);
        keepAlive.unref();
        request.once("close", () => {
          clearInterval(keepAlive);
          unsubscribe();
          aiEventResponses.delete(response);
        });
        return;
      }

      const aiThreadTurnRoute = pathname.match(/^\/api\/local\/ai\/threads\/([^/]+)\/turns$/);
      if (aiThreadTurnRoute) {
        if (request.method !== "POST") return methodNotAllowed(response, ["POST"]);
        assertNoQuery(url.searchParams, "POST /api/local/ai/threads/:id/turns");
        const threadId = decodeRouteSegment(aiThreadTurnRoute[1], "Thread id");
        const run = await aiChat.startTurn(
          threadId,
          parseAiTurn(await readJson(
            request,
            AI_CHAT_TURN_BODY_LIMIT,
            "AI chat turn body cannot exceed 25 MiB",
          )),
        );
        return sendJson(response, 202, { run });
      }

      const aiThreadCompactRoute = pathname.match(/^\/api\/local\/ai\/threads\/([^/]+)\/compact$/);
      if (aiThreadCompactRoute) {
        if (request.method !== "POST") return methodNotAllowed(response, ["POST"]);
        assertNoQuery(url.searchParams, "POST /api/local/ai/threads/:id/compact");
        const threadId = decodeRouteSegment(aiThreadCompactRoute[1], "Thread id");
        await assertEmptyRequestBody(request, "POST /api/local/ai/threads/:id/compact");
        const thread = await aiChat.compactThread(threadId);
        return sendJson(response, 200, { thread });
      }

      const aiThreadRoute = pathname.match(/^\/api\/local\/ai\/threads\/([^/]+)$/);
      if (aiThreadRoute) {
        assertNoQuery(url.searchParams, "/api/local/ai/threads/:id");
        const threadId = decodeRouteSegment(aiThreadRoute[1], "Thread id");
        if (request.method === "GET") {
          return sendJson(response, 200, await aiChat.getThreadSnapshot(threadId));
        }
        if (request.method === "PATCH") {
          const thread = await aiChat.updateThread(threadId, parseAiThreadPatch(await readJson(request)));
          return sendJson(response, 200, { thread });
        }
        if (request.method === "DELETE") {
          await assertEmptyRequestBody(request, "DELETE /api/local/ai/threads/:id");
          await aiChat.deleteThread(threadId);
          return sendEmpty(response, 204);
        }
        return methodNotAllowed(response, ["GET", "PATCH", "DELETE"]);
      }

      const aiInterruptRoute = pathname.match(/^\/api\/local\/ai\/runs\/([^/]+)\/interrupt$/);
      if (aiInterruptRoute) {
        if (request.method !== "POST") return methodNotAllowed(response, ["POST"]);
        assertNoQuery(url.searchParams, "POST /api/local/ai/runs/:id/interrupt");
        const runId = decodeRouteSegment(aiInterruptRoute[1], "Run id");
        await assertEmptyRequestBody(request, "POST /api/local/ai/runs/:id/interrupt");
        const run = await aiChat.interrupt(runId);
        return sendJson(response, 200, { run });
      }

      if (pathname === "/api/device-workspaces") {
        if (request.method !== "GET") return methodNotAllowed(response, ["GET"]);
        if ([...url.searchParams.keys()].length > 0) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "GET /api/device-workspaces does not accept query parameters");
        }
        return sendJson(response, 200, {
          workspaces: await readCodexProjectWorkspaces(resolved.codexStatePath),
        });
      }


      let currentCloudConfig = null;
      if (pathname.startsWith("/api/")) {
        currentCloudConfig = await cloudConfig.read();
        if (currentCloudConfig.remoteUrl) {
          assertLoopbackRequest(request);
          if (!isLocalCompanionRoute(pathname)) {
            return sendFetchResponse(
              response,
              await cloudProxy.forward(toFetchRequest(request)),
            );
          }
        }
      }

      if (pathname === "/api/projects") {
        if (request.method === "GET") {
          if ([...url.searchParams.keys()].length > 0) {
            throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "GET /api/projects does not accept query parameters");
          }
          const projects = database.listProjects()
            .filter((project) => !resolved.publicAuthEnabled || project.id !== DEFAULT_PROJECT_ID)
            .map((project) => ({
              ...project,
              workspacePath: resolved.publicAuthEnabled || project.id === DEFAULT_PROJECT_ID
                ? null
                : currentCloudConfig?.projectMappings[project.id] ?? project.workspacePath,
            }));
          return sendJson(response, 200, { projects });
        }
        if (request.method === "POST") {
          const project = database.createProject(parseProjectCreate(await readJson(request)));
          events.emit("project.created", { project });
          return sendJson(response, 201, { project });
        }
        return methodNotAllowed(response, ["GET", "POST"]);
      }

      const projectRoute = pathname.match(/^\/api\/projects\/([^/]+)$/);
      if (projectRoute) {
        if ([...url.searchParams.keys()].length > 0) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "Project routes do not accept query parameters");
        }
        let projectId;
        try {
          projectId = decodeURIComponent(projectRoute[1]);
        } catch {
          throw new ApiError(400, "INVALID_PATH", "Project id contains invalid encoding");
        }
        validateProjectId(projectId);
        if (request.method === "DELETE") {
          database.deleteProject(projectId);
          return sendEmpty(response, 204);
        }
        return methodNotAllowed(response, ["DELETE"]);
      }

      const projectLabelsRoute = pathname.match(/^\/api\/projects\/([^/]+)\/labels$/);
      if (projectLabelsRoute) {
        if ([...url.searchParams.keys()].length > 0) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "Project label routes do not accept query parameters");
        }
        let projectId;
        try {
          projectId = decodeURIComponent(projectLabelsRoute[1]);
        } catch {
          throw new ApiError(400, "INVALID_PATH", "Project id contains invalid encoding");
        }
        validateProjectId(projectId);
        if (request.method !== "POST" && request.method !== "DELETE") {
          return methodNotAllowed(response, ["POST", "DELETE"]);
        }
        if (request.method === "DELETE" && projectId === JIRA_PROJECT_ID) {
          throw new ApiError(
            409,
            "JIRA_LABEL_CATALOG_DELETE_UNAVAILABLE",
            "Jira 标签目录由同步管理，不能在 Taskboard 中删除",
          );
        }
        const label = parseProjectLabel(await readJson(request));
        const project = request.method === "POST"
          ? database.addProjectLabel(projectId, label)
          : database.deleteProjectLabel(projectId, label);
        events.emit("project.labels.updated", { project });
        return sendJson(response, 200, { project });
      }

      const projectReadmeAttachmentsRoute = pathname.match(
        /^\/api\/projects\/([^/]+)\/readme\/attachments$/,
      );
      if (projectReadmeAttachmentsRoute) {
        if ([...url.searchParams.keys()].length > 0) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "Project README attachment routes do not accept query parameters");
        }
        let projectId;
        try {
          projectId = decodeURIComponent(projectReadmeAttachmentsRoute[1]);
        } catch {
          throw new ApiError(400, "INVALID_PATH", "Project id contains invalid encoding");
        }
        validateProjectId(projectId);
        if (request.method !== "POST") return methodNotAllowed(response, ["POST"]);
        const metadata = parseAttachmentHeaders(request);
        if (metadata.kind !== "inline") {
          throw new ApiError(400, "INVALID_ATTACHMENT_KIND", "Project README attachments must be inline");
        }
        const body = await readBody(request, ATTACHMENT_BODY_LIMIT, "Attachment cannot exceed 25 MiB");
        const id = randomUUID();
        await mkdir(resolved.attachmentsDirectory, { recursive: true });
        const storagePath = path.join(resolved.attachmentsDirectory, id);
        await writeFile(storagePath, body, { flag: "wx" });
        let attachment;
        try {
          attachment = database.createProjectReadmeAttachment(projectId, {
            id,
            ...metadata,
            size: body.length,
          });
        } catch (error) {
          await unlink(storagePath);
          throw error;
        }
        return sendJson(response, 201, { attachment });
      }

      const projectReadmeRoute = pathname.match(/^\/api\/projects\/([^/]+)\/readme$/);
      if (projectReadmeRoute) {
        if ([...url.searchParams.keys()].length > 0) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "Project README routes do not accept query parameters");
        }
        let projectId;
        try {
          projectId = decodeURIComponent(projectReadmeRoute[1]);
        } catch {
          throw new ApiError(400, "INVALID_PATH", "Project id contains invalid encoding");
        }
        validateProjectId(projectId);
        if (request.method === "GET") {
          return sendJson(response, 200, { readme: database.getProjectReadme(projectId) });
        }
        if (request.method === "PUT") {
          const input = parseProjectReadmeSave(await readJson(
            request,
            PROJECT_README_BODY_LIMIT,
            "Project README request cannot exceed 3 MiB",
          ));
          const readme = database.saveProjectReadme(projectId, input.content, input.version);
          events.emit("project.readme.updated", {
            projectId,
            readmeVersion: readme.version,
          });
          return sendJson(response, 200, { readme });
        }
        return methodNotAllowed(response, ["GET", "PUT"]);
      }

      const developmentContextsRoute = pathname.match(/^\/api\/projects\/([^/]+)\/development-contexts$/);
      if (developmentContextsRoute) {
        if (request.method !== "GET") return methodNotAllowed(response, ["GET"]);
        const unknownQuery = [...url.searchParams.keys()].filter((key) => (
          !["codexProjectId", "codexThreadId", "workspacePath"].includes(key)
        ));
        if (unknownQuery.length > 0) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", `Unknown query parameter: ${unknownQuery[0]}`);
        }
        let projectId;
        try {
          projectId = decodeURIComponent(developmentContextsRoute[1]);
        } catch {
          throw new ApiError(400, "INVALID_PATH", "Project id contains invalid encoding");
        }
        validateProjectId(projectId);
        const project = currentCloudConfig.remoteUrl
          ? {
            id: projectId,
            workspacePath: projectId === DEFAULT_PROJECT_ID
              ? null
              : currentCloudConfig.projectMappings[projectId] ?? null,
          }
          : database.getProject(projectId);
        if (!project) throw new ApiError(404, "PROJECT_NOT_FOUND", `Project '${projectId}' does not exist`);
        const codexProjectId = stringField(url.searchParams.get("codexProjectId") ?? null, "codexProjectId", {
          nullable: true,
          maxLength: 128,
        });
        const codexThreadId = stringField(url.searchParams.get("codexThreadId") ?? null, "codexThreadId", {
          nullable: true,
          maxLength: 256,
        });
        const deviceWorkspacePath = stringField(
          url.searchParams.get("workspacePath") ?? null,
          "workspacePath",
          { nullable: true, maxLength: 4096 },
        );
        if (deviceWorkspacePath?.includes("\0")) {
          throw new ApiError(400, "INVALID_FIELD", "'workspacePath' cannot contain null bytes");
        }
        const workspacePath = deviceWorkspacePath ?? await resolveProjectWorkspace(
          project,
          codexProjectId,
          codexThreadId,
          resolved.codexStatePath,
          resolved.codexProcessesPath,
        );
        return sendJson(
          response,
          200,
          await scanDevelopmentContexts(workspacePath, codexProcessEnvironment),
        );
      }

      if (pathname === "/api/tasks") {
        if (request.method === "GET") {
          const filters = parseTaskFilters(url.searchParams);
          if (!filters.projectId || filters.projectId === JIRA_PROJECT_ID) await jira.sync();
          return sendJson(response, 200, { tasks: database.listTasks(filters) });
        }
        if (request.method === "POST") {
          const actor = actorFromRequest(request);
          const { assigneeTarget, ...parsedInput } = parseTaskCreate(await readJson(request));
          const input = resolveInputThreadBinding(parsedInput);
          if (input.projectId === JIRA_PROJECT_ID) {
            throw new ApiError(
              409,
              "JIRA_CREATE_UNAVAILABLE",
              "请在 Jira 中新建议题，Taskboard 当前只同步已分配给你的任务",
            );
          }
          const task = database.createTask({
            ...input,
            actor,
            assignee: resolveAssignee(assigneeTarget, actor),
          });
          events.emit("task.created", { task });
          return sendJson(response, 201, { task });
        }
        return methodNotAllowed(response, ["GET", "POST"]);
      }

      if (pathname === "/api/events") {
        if (request.method !== "GET") return methodNotAllowed(response, ["GET"]);
        if ([...url.searchParams.keys()].length > 0) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "GET /api/events does not accept query parameters");
        }
        events.connect(request, response);
        return;
      }

      const taskRelationRoute = pathname.match(
        /^\/api\/tasks\/([^/]+)\/relations\/([^/]+)\/([^/]+)$/,
      );
      if (taskRelationRoute) {
        let taskId;
        let type;
        let relatedTaskId;
        try {
          taskId = decodeURIComponent(taskRelationRoute[1]);
          type = decodeURIComponent(taskRelationRoute[2]);
          relatedTaskId = decodeURIComponent(taskRelationRoute[3]);
        } catch {
          throw new ApiError(400, "INVALID_PATH", "Issue relation path contains invalid encoding");
        }
        if (
          taskId.length === 0
          || taskId.length > 128
          || relatedTaskId.length === 0
          || relatedTaskId.length > 128
        ) {
          throw new ApiError(400, "INVALID_PATH", "Issue relation task id is invalid");
        }
        if ([...url.searchParams.keys()].length > 0) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "Issue relation routes do not accept query parameters");
        }
        const relationType = parseIssueRelationType(type);
        if (request.method === "POST") {
          const { version, threadId, threadBinding, origin } = resolveInputThreadBinding(
            parseRelationMutation(await readJson(request)),
          );
          const result = database.addTaskRelation(
            taskId,
            version,
            relationType,
            relatedTaskId,
            threadId,
            threadBinding,
            actorFromRequest(request),
            origin,
          );
          events.emit("task.relation.updated", result);
          return sendJson(response, 200, result);
        }
        if (request.method === "DELETE") {
          const { version, threadId, threadBinding, origin } = resolveInputThreadBinding(
            parseRelationMutation(await readJson(request)),
          );
          const result = database.removeTaskRelation(
            taskId,
            version,
            relationType,
            relatedTaskId,
            threadId,
            threadBinding,
            actorFromRequest(request),
            origin,
          );
          events.emit("task.relation.updated", result);
          return sendJson(response, 200, result);
        }
        return methodNotAllowed(response, ["POST", "DELETE"]);
      }

      const taskActivitiesRoute = pathname.match(/^\/api\/tasks\/([^/]+)\/activities$/);
      if (taskActivitiesRoute) {
        let taskId;
        try {
          taskId = decodeURIComponent(taskActivitiesRoute[1]);
        } catch {
          throw new ApiError(400, "INVALID_PATH", "Task id contains invalid encoding");
        }
        if (taskId.length === 0 || taskId.length > 128) {
          throw new ApiError(400, "INVALID_PATH", "Task id is invalid");
        }
        if ([...url.searchParams.keys()].length > 0) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "Activity routes do not accept query parameters");
        }
        if (request.method === "GET") {
          return sendJson(response, 200, { activities: database.listTaskActivities(taskId) });
        }
        return methodNotAllowed(response, ["GET"]);
      }

      const taskCommentsRoute = pathname.match(/^\/api\/tasks\/([^/]+)\/comments$/);
      if (taskCommentsRoute) {
        let taskId;
        try {
          taskId = decodeURIComponent(taskCommentsRoute[1]);
        } catch {
          throw new ApiError(400, "INVALID_PATH", "Task id contains invalid encoding");
        }
        if (taskId.length === 0 || taskId.length > 128) {
          throw new ApiError(400, "INVALID_PATH", "Task id is invalid");
        }
        if (request.method === "GET") {
          const after = parseAfterCursor(url.searchParams, "Comment routes");
          const comments = after
            ? database.listCommentsAfter(taskId, after)
            : database.listComments(taskId);
          return sendJson(response, 200, {
            comments,
            nextCursor: nextCursor(comments, after),
          });
        }
        if ([...url.searchParams.keys()].length > 0) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "Comment routes do not accept query parameters");
        }
        if (request.method === "POST") {
          const comment = database.createComment(taskId, {
            ...resolveInputThreadBinding(parseCommentCreate(await readJson(request))),
            actor: actorFromRequest(request),
          });
          const task = database.getTask(taskId);
          events.emit("comment.created", { comment, task });
          return sendJson(response, 201, { comment });
        }
        return methodNotAllowed(response, ["GET", "POST"]);
      }

      const commentRoute = pathname.match(/^\/api\/comments\/([^/]+)$/);
      if (commentRoute) {
        let id;
        try {
          id = decodeURIComponent(commentRoute[1]);
        } catch {
          throw new ApiError(400, "INVALID_PATH", "Comment id contains invalid encoding");
        }
        if (id.length === 0 || id.length > 128) {
          throw new ApiError(400, "INVALID_PATH", "Comment id is invalid");
        }
        if ([...url.searchParams.keys()].length > 0) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "Comment routes do not accept query parameters");
        }
        if (request.method === "PATCH") {
          const patch = resolveInputThreadBinding(parseCommentPatch(await readJson(request)));
          const comment = database.updateComment(
            id,
            patch.version,
            patch.body,
            patch.threadId,
            patch.threadBinding,
          );
          const task = database.getTask(comment.taskId);
          events.emit("comment.updated", { comment, task });
          return sendJson(response, 200, { comment });
        }
        if (request.method === "DELETE") {
          const { version } = parseArchive(await readJson(request));
          const comment = database.deleteComment(id, version);
          for (const attachment of comment.attachments) {
            try {
              await unlink(path.join(resolved.attachmentsDirectory, attachment.id));
            } catch (error) {
              if (error.code !== "ENOENT") throw error;
            }
          }
          const task = database.getTask(comment.taskId);
          events.emit("comment.deleted", { comment, task });
          return sendEmpty(response, 204);
        }
        return methodNotAllowed(response, ["PATCH", "DELETE"]);
      }

      const commentAttachmentsRoute = pathname.match(/^\/api\/comments\/([^/]+)\/attachments$/);
      if (commentAttachmentsRoute) {
        let commentId;
        try {
          commentId = decodeURIComponent(commentAttachmentsRoute[1]);
        } catch {
          throw new ApiError(400, "INVALID_PATH", "Comment id contains invalid encoding");
        }
        if (commentId.length === 0 || commentId.length > 128) {
          throw new ApiError(400, "INVALID_PATH", "Comment id is invalid");
        }
        if (request.method === "GET") {
          const after = parseAfterCursor(url.searchParams, "Attachment routes");
          const attachments = database.listCommentAttachments(commentId, after);
          return sendJson(response, 200, {
            attachments,
            nextCursor: nextCursor(attachments, after),
          });
        }
        if ([...url.searchParams.keys()].length > 0) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "Attachment routes do not accept query parameters");
        }
        if (request.method === "POST") {
          const comment = database.getComment(commentId);
          if (!comment) throw new ApiError(404, "COMMENT_NOT_FOUND", `Comment '${commentId}' does not exist`);
          const metadata = parseAttachmentHeaders(request);
          const body = await readBody(request, ATTACHMENT_BODY_LIMIT, "Attachment cannot exceed 25 MiB");
          const id = randomUUID();
          await mkdir(resolved.attachmentsDirectory, { recursive: true });
          const storagePath = path.join(resolved.attachmentsDirectory, id);
          await writeFile(storagePath, body, { flag: "wx" });
          let attachment;
          try {
            attachment = database.createCommentAttachment(commentId, { id, ...metadata, size: body.length });
          } catch (error) {
            await unlink(storagePath);
            throw error;
          }
          const task = database.getTask(comment.taskId);
          events.emit("attachment.created", { attachment, comment: database.getComment(commentId), task });
          return sendJson(response, 201, { attachment });
        }
        return methodNotAllowed(response, ["GET", "POST"]);
      }

      const taskAttachmentsRoute = pathname.match(/^\/api\/tasks\/([^/]+)\/attachments$/);
      if (taskAttachmentsRoute) {
        let taskId;
        try {
          taskId = decodeURIComponent(taskAttachmentsRoute[1]);
        } catch {
          throw new ApiError(400, "INVALID_PATH", "Task id contains invalid encoding");
        }
        if (taskId.length === 0 || taskId.length > 128) {
          throw new ApiError(400, "INVALID_PATH", "Task id is invalid");
        }
        if (request.method === "GET") {
          const after = parseAfterCursor(url.searchParams, "Attachment routes");
          const attachments = database.listAttachments(taskId, after);
          return sendJson(response, 200, {
            attachments,
            nextCursor: nextCursor(attachments, after),
          });
        }
        if ([...url.searchParams.keys()].length > 0) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "Attachment routes do not accept query parameters");
        }
        if (request.method === "POST") {
          const task = database.getTask(taskId);
          if (!task) throw new ApiError(404, "TASK_NOT_FOUND", `Task '${taskId}' does not exist`);
          const metadata = parseAttachmentHeaders(request);
          const body = await readBody(request, ATTACHMENT_BODY_LIMIT, "Attachment cannot exceed 25 MiB");
          const id = randomUUID();
          await mkdir(resolved.attachmentsDirectory, { recursive: true });
          const storagePath = path.join(resolved.attachmentsDirectory, id);
          await writeFile(storagePath, body, { flag: "wx" });
          let attachment;
          try {
            attachment = database.createAttachment(taskId, { id, ...metadata, size: body.length });
          } catch (error) {
            await unlink(storagePath);
            throw error;
          }
          events.emit("attachment.created", { attachment, task });
          return sendJson(response, 201, { attachment });
        }
        return methodNotAllowed(response, ["GET", "POST"]);
      }

      const attachmentContentRoute = pathname.match(/^\/api\/attachments\/([^/]+)\/(content|download)$/);
      if (attachmentContentRoute) {
        let id;
        try {
          id = decodeURIComponent(attachmentContentRoute[1]);
        } catch {
          throw new ApiError(400, "INVALID_PATH", "Attachment id contains invalid encoding");
        }
        if (id.length === 0 || id.length > 128) {
          throw new ApiError(400, "INVALID_PATH", "Attachment id is invalid");
        }
        if ([...url.searchParams.keys()].length > 0) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "Attachment routes do not accept query parameters");
        }
        if (request.method !== "GET" && request.method !== "HEAD") {
          return methodNotAllowed(response, ["GET", "HEAD"]);
        }
        const attachment = database.getAttachment(id) ?? database.getProjectReadmeAttachment(id);
        if (!attachment) throw new ApiError(404, "ATTACHMENT_NOT_FOUND", `Attachment '${id}' does not exist`);
        const body = await readFile(path.join(resolved.attachmentsDirectory, attachment.id));
        const encodedFilename = encodeURIComponent(attachment.filename).replace(/['()*]/g, (character) => (
          `%${character.charCodeAt(0).toString(16).toUpperCase()}`
        ));
        const canOpenInline = attachmentContentRoute[2] === "content"
          && (
            INLINE_ATTACHMENT_TYPES.has(attachment.contentType)
            || attachment.contentType.startsWith("video/")
          );
        response.writeHead(200, {
          "cache-control": "private, no-store",
          "content-disposition": `${canOpenInline ? "inline" : "attachment"}; filename*=UTF-8''${encodedFilename}`,
          "content-length": body.length,
          "content-security-policy": "sandbox; default-src 'none'",
          "content-type": canOpenInline ? attachment.contentType : "application/octet-stream",
        });
        response.end(request.method === "HEAD" ? undefined : body);
        return;
      }

      const attachmentRoute = pathname.match(/^\/api\/attachments\/([^/]+)$/);
      if (attachmentRoute) {
        let id;
        try {
          id = decodeURIComponent(attachmentRoute[1]);
        } catch {
          throw new ApiError(400, "INVALID_PATH", "Attachment id contains invalid encoding");
        }
        if (id.length === 0 || id.length > 128) {
          throw new ApiError(400, "INVALID_PATH", "Attachment id is invalid");
        }
        if ([...url.searchParams.keys()].length > 0) {
          throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "Attachment routes do not accept query parameters");
        }
        if (request.method !== "DELETE") return methodNotAllowed(response, ["DELETE"]);
        const attachment = database.getAttachment(id);
        if (!attachment) throw new ApiError(404, "ATTACHMENT_NOT_FOUND", `Attachment '${id}' does not exist`);
        try {
          await unlink(path.join(resolved.attachmentsDirectory, attachment.id));
        } catch (error) {
          if (error.code !== "ENOENT") throw error;
        }
        database.deleteAttachment(id);
        const task = database.getTask(attachment.taskId);
        events.emit("attachment.deleted", { attachment, task });
        return sendEmpty(response, 204);
      }

      const taskTreeRoute = pathname.match(/^\/api\/tasks\/([^/]+)\/tree$/);
      if (taskTreeRoute) {
        let id;
        try {
          id = decodeURIComponent(taskTreeRoute[1]);
        } catch {
          throw new ApiError(400, "INVALID_PATH", "Task id contains invalid encoding");
        }
        if (id.length === 0 || id.length > 128) {
          throw new ApiError(400, "INVALID_PATH", "Task id is invalid");
        }
        if (request.method !== "GET") return methodNotAllowed(response, ["GET"]);
        const { direction, depth } = parseTaskTreeQuery(url.searchParams);
        return sendJson(response, 200, { tree: database.getTaskTree(id, direction, depth) });
      }

      const taskRoute = pathname.match(/^\/api\/tasks\/([^/]+)(?:\/(archive|restore|move))?$/);
      if (taskRoute) {
        let id;
        try {
          id = decodeURIComponent(taskRoute[1]);
        } catch {
          throw new ApiError(400, "INVALID_PATH", "Task id contains invalid encoding");
        }
        if (id.length === 0 || id.length > 128) {
          throw new ApiError(400, "INVALID_PATH", "Task id is invalid");
        }
        const action = taskRoute[2];
        if (!action && request.method === "GET") {
          if ([...url.searchParams.keys()].length > 0) {
            throw new ApiError(400, "UNKNOWN_QUERY_PARAMETER", "GET /api/tasks/:id does not accept query parameters");
          }
          const task = database.getTask(id);
          if (!task) throw new ApiError(404, "TASK_NOT_FOUND", `Task '${id}' does not exist`);
          return sendJson(response, 200, { task });
        }
        if (!action && request.method === "PATCH") {
          const actor = actorFromRequest(request);
          const {
            version,
            changes,
            threadId,
            threadBinding,
            assigneeTarget,
          } = resolveInputThreadBinding(parseTaskPatch(await readJson(request)));
          const current = database.getTask(id);
          if (!current) throw new ApiError(404, "TASK_NOT_FOUND", `Task '${id}' does not exist`);
          let jiraChanged = false;
          if (current.source !== "jira" && changes.projectId === JIRA_PROJECT_ID) {
            throw new ApiError(
              409,
              "JIRA_PROJECT_MOVE_UNAVAILABLE",
              "本地任务不能移入 Jira 同步项目",
            );
          }
          if (current.source === "jira") {
            if (current.version !== version) {
              throw new ApiError(409, "VERSION_CONFLICT", "Task changed since it was last read", {
                expectedVersion: version,
                actualVersion: current.version,
              });
            }
            if (current.archivedAt !== null) {
              throw new ApiError(409, "TASK_ARCHIVED", "Archived tasks cannot be updated");
            }
            if (Object.hasOwn(changes, "projectId")) {
              throw new ApiError(409, "JIRA_PROJECT_MOVE_UNAVAILABLE", "Jira 任务不能移到本地项目");
            }
            if (assigneeTarget !== undefined) {
              throw new ApiError(409, "JIRA_ASSIGNEE_UNAVAILABLE", "请在 Jira 中修改经办人");
            }
            const dueDate = Object.hasOwn(changes, "dueDate") ? changes.dueDate : current.dueDate;
            const recurrence = Object.hasOwn(changes, "recurrence")
              ? changes.recurrence
              : current.recurrence;
            if (recurrence && !dueDate) {
              throw new ApiError(400, "INVALID_FIELD", "A recurring issue requires a due date");
            }
            jiraChanged = await jira.updateTask(current, changes);
          }
          if (assigneeTarget !== undefined) {
            changes.assignee = resolveAssignee(assigneeTarget, actor);
          }
          let task;
          try {
            task = database.updateTask(id, version, changes, threadId, threadBinding, actor);
          } catch (error) {
            if (jiraChanged) {
              try {
                await jira.reconcile();
              } catch {
                throw new ApiError(
                  502,
                  "JIRA_RECONCILE_FAILED",
                  "Jira 已更新，但 Taskboard 重新同步失败，请手动同步",
                );
              }
            }
            throw error;
          }
          events.emit("task.updated", { task });
          maybeStartDelivery(current, task, actor);
          return sendJson(response, 200, { task });
        }
        if (!action && request.method === "DELETE") {
          const current = database.getTask(id);
          if (current?.source === "jira") {
            throw new ApiError(409, "JIRA_DELETE_UNAVAILABLE", "Jira 任务不能从 Taskboard 永久删除");
          }
          const { version } = parseArchive(await readJson(request));
          const deleted = database.deleteArchivedTask(id, version);
          for (const attachmentId of deleted.attachmentIds) {
            try {
              await unlink(path.join(resolved.attachmentsDirectory, attachmentId));
            } catch (error) {
              if (error.code !== "ENOENT") throw error;
            }
          }
          events.emit("task.deleted", { task: deleted.task });
          return sendEmpty(response, 204);
        }
        if (action === "move" && request.method === "POST") {
          const move = resolveInputThreadBinding(parseMove(await readJson(request)));
          const current = database.getTask(id);
          if (!current) throw new ApiError(404, "TASK_NOT_FOUND", `Task '${id}' does not exist`);
          if (current.source === "jira") {
            if (current.version !== move.version) {
              throw new ApiError(409, "VERSION_CONFLICT", "Task changed since it was last read", {
                expectedVersion: move.version,
                actualVersion: current.version,
              });
            }
            if (current.archivedAt !== null) {
              throw new ApiError(409, "TASK_ARCHIVED", "Archived tasks cannot be moved");
            }
            await jira.moveTask(current, move.status);
          }
          const actor = actorFromRequest(request);
          const task = database.moveTask(
            id,
            move.version,
            move.status,
            move.sortOrder,
            move.threadId,
            move.threadBinding,
            actor,
          );
          events.emit("task.moved", { task });
          maybeStartDelivery(current, task, actor);
          return sendJson(response, 200, { task });
        }
        if (action === "archive" && request.method === "POST") {
          const current = database.getTask(id);
          if (current?.source === "jira") {
            throw new ApiError(409, "JIRA_ARCHIVE_UNAVAILABLE", "Jira 任务由同步范围自动管理，不能手动归档");
          }
          const { version, threadId, threadBinding } = resolveInputThreadBinding(
            parseArchive(await readJson(request)),
          );
          const task = database.archiveTask(
            id,
            version,
            threadId,
            threadBinding,
            actorFromRequest(request),
          );
          events.emit("task.archived", { task });
          return sendJson(response, 200, { task });
        }
        if (action === "restore" && request.method === "POST") {
          const current = database.getTask(id);
          if (current?.source === "jira") {
            throw new ApiError(409, "JIRA_RESTORE_UNAVAILABLE", "Jira 任务由同步范围自动管理，不能手动恢复");
          }
          const { version, threadId, threadBinding } = resolveInputThreadBinding(
            parseArchive(await readJson(request)),
          );
          const task = database.restoreTask(
            id,
            version,
            threadId,
            threadBinding,
            actorFromRequest(request),
          );
          events.emit("task.restored", { task });
          return sendJson(response, 200, { task });
        }
        return methodNotAllowed(response, action ? ["POST"] : ["GET", "PATCH", "DELETE"]);
      }

      if (pathname.startsWith("/api/")) {
        throw new ApiError(404, "NOT_FOUND", "API route not found");
      }
      if (await serveStatic(request, response, pathname, resolved.staticDirectory)) return;
      throw new ApiError(404, "NOT_FOUND", "Resource not found");
    } catch (error) {
      if (response.headersSent) {
        response.destroy(error);
        return;
      }
      if (error instanceof ApiError) {
        const payload = { error: { code: error.code, message: error.message } };
        if (error.details !== undefined) payload.error.details = error.details;
        sendJson(response, error.status, payload, error.status === 401
          ? { "www-authenticate": 'Basic realm="Codex Taskboard Product Collaboration", charset="UTF-8"' }
          : {});
        return;
      }
      if (error instanceof CloudProxyError) {
        const payload = { error: { code: error.code, message: error.message } };
        if (error.details !== undefined) payload.error.details = error.details;
        sendJson(response, error.status, payload);
        return;
      }
      console.error(error);
      sendJson(response, 500, { error: { code: "INTERNAL_ERROR", message: "Internal server error" } });
    }
  });

  const cloudRealtimeServer = new WebSocketServer({ noServer: true });
  const cloudRealtimeSockets = new Set();

  function rejectWebSocketUpgrade(socket, status, message) {
    const body = `${message}\n`;
    socket.end([
      `HTTP/1.1 ${status} ${message}`,
      "Connection: close",
      "Content-Type: text/plain; charset=utf-8",
      `Content-Length: ${Buffer.byteLength(body)}`,
      "",
      body,
    ].join("\r\n"));
  }

  function closeOrTerminateWebSocket(webSocket, code, reason) {
    if (webSocket.readyState !== WebSocketClient.OPEN) {
      webSocket.terminate();
      return;
    }
    if (code >= 1000 && ![1004, 1005, 1006, 1015].includes(code)) {
      webSocket.close(code, reason);
    } else {
      webSocket.terminate();
    }
  }

  server.on("upgrade", async (request, socket, head) => {
    let remoteSocket;
    try {
      const incomingUrl = new URL(request.url, "http://127.0.0.1");
      if (resolved.instanceToken) {
        if (!incomingUrl.pathname.startsWith(`${routePrefix}/`)) {
          rejectWebSocketUpgrade(socket, 404, "Not Found");
          return;
        }
        request.url = `${incomingUrl.pathname.slice(routePrefix.length) || "/"}${incomingUrl.search}`;
      }
      assertTrustedNetworkRequest(
        request,
        Boolean(resolved.instanceToken),
        resolved.trustedOrigins,
      );
      const url = new URL(request.url, "http://127.0.0.1");
      if (url.pathname !== "/api/events" || [...url.searchParams.keys()].length > 0) {
        rejectWebSocketUpgrade(socket, 404, "Not Found");
        return;
      }
      assertLoopbackRequest(request);
      const target = await cloudProxy.webSocketTarget("/api/events");
      remoteSocket = new WebSocketClient(target.url, { headers: target.headers });
      const pendingMessages = [];
      const queueMessage = (data, isBinary) => pendingMessages.push({ data, isBinary });
      remoteSocket.on("message", queueMessage);
      await new Promise((resolve, reject) => {
        const cleanup = () => {
          remoteSocket.off("open", onOpen);
          remoteSocket.off("error", onError);
          remoteSocket.off("close", onClose);
        };
        const onOpen = () => {
          cleanup();
          resolve();
        };
        const onError = (error) => {
          cleanup();
          reject(error);
        };
        const onClose = () => {
          cleanup();
          reject(new Error("Cloud realtime connection closed before opening"));
        };
        remoteSocket.once("open", onOpen);
        remoteSocket.once("error", onError);
        remoteSocket.once("close", onClose);
      });
      cloudRealtimeServer.handleUpgrade(request, socket, head, (localSocket) => {
        const pair = { localSocket, remoteSocket };
        cloudRealtimeSockets.add(pair);
        const removePair = () => cloudRealtimeSockets.delete(pair);
        const forwardMessage = (data, isBinary) => {
          if (localSocket.readyState === WebSocketClient.OPEN) {
            localSocket.send(data, { binary: isBinary });
          }
        };

        remoteSocket.off("message", queueMessage);
        remoteSocket.on("message", forwardMessage);
        for (const { data, isBinary } of pendingMessages) forwardMessage(data, isBinary);

        localSocket.on("message", () => {
          localSocket.close(1008, "Client messages are not supported");
        });
        localSocket.on("close", (code, reason) => {
          removePair();
          closeOrTerminateWebSocket(remoteSocket, code, reason);
        });
        localSocket.on("error", () => remoteSocket.terminate());

        remoteSocket.on("close", (code, reason) => {
          removePair();
          closeOrTerminateWebSocket(localSocket, code, reason);
        });
        remoteSocket.on("error", () => {
          if (localSocket.readyState === WebSocketClient.OPEN) {
            localSocket.close(1011, "Cloud realtime connection failed");
          }
        });
      });
    } catch (error) {
      remoteSocket?.terminate();
      rejectWebSocketUpgrade(socket, error?.status ?? 502, "WebSocket connection failed");
    }
  });

  let listening = false;
  return {
    database,
    aiChat,
    server,
    options: resolved,
    async listen({ host = "127.0.0.1", port = resolvePort(), fd = null } = {}) {
      if (host !== "127.0.0.1" && host !== "0.0.0.0") {
        throw new Error("Taskboard server must bind to 127.0.0.1 or 0.0.0.0");
      }
      if (fd !== null && (!Number.isInteger(fd) || fd < 3 || fd > 255)) {
        throw new Error("Taskboard server listen fd must be an inherited file descriptor");
      }
      await new Promise((resolve, reject) => {
        const onError = (error) => {
          server.off("listening", onListening);
          reject(error);
        };
        const onListening = () => {
          server.off("error", onError);
          resolve();
        };
        server.once("error", onError);
        server.once("listening", onListening);
        if (fd === null) server.listen(port, host);
        else server.listen({ fd });
      });
      listening = true;
      return server.address();
    },
    async close() {
      for (const { localSocket, remoteSocket } of cloudRealtimeSockets) {
        localSocket.terminate();
        remoteSocket.terminate();
      }
      cloudRealtimeSockets.clear();
      cloudRealtimeServer.close();
      const serverClosed = listening
        ? new Promise((resolve, reject) => {
            server.close((error) => error ? reject(error) : resolve());
          })
        : Promise.resolve();
      events.close();
      for (const response of aiEventResponses) response.end();
      aiEventResponses.clear();
      await aiChat.close();
      await projectSummary.close();
      await serverClosed;
      listening = false;
      database.close();
    },
  };
}
