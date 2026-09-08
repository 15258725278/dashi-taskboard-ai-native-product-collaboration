import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  deliveryWorkspaceForTask,
  parseDeliveryProjects,
  runLocalDelivery,
} from "../server/delivery-automation.mjs";

const config = parseDeliveryProjects({
  skillhub: {
    mode: "local",
    deployScript: "scripts/taskboard-local-acceptance-deploy.sh",
    acceptanceUrl: "https://admin.example.com/skillhub/",
    deploymentRecordUrl: "https://admin.example.com/taskboard/?project=skillhub",
  },
}).get("skillhub");

test("local delivery runs the bound worktree script and reads its manifest", async () => {
  const workspace = await mkdtemp(path.join(os.tmpdir(), "taskboard-local-delivery-"));
  try {
    await mkdir(path.join(workspace, "scripts"));
    await writeFile(path.join(workspace, "scripts", "taskboard-local-acceptance-deploy.sh"), "#!/bin/zsh\n");
    const calls = [];
    const result = await runLocalDelivery({
      deliveryId: "delivery-123",
      workspacePath: workspace,
      taskIdentifier: "SKI-42",
      config,
      command: async (executable, args, options) => {
        calls.push({ executable, args, options });
        return { stdout: JSON.stringify({
          deliveryId: "delivery-123",
          acceptanceUrl: "https://admin.example.com/skillhub/",
          deploymentRecordUrl: "https://admin.example.com/taskboard/?project=skillhub",
          implementationUrl: "https://admin.example.com/taskboard/?project=skillhub",
          immutableTag: "local-abcdef0",
          gitSha: "abcdef0123456789",
        }) };
      },
    });
    assert.equal(result.acceptanceUrl, "https://admin.example.com/skillhub");
    assert.equal(result.immutableTag, "local-abcdef0");
    assert.deepEqual(result.prNumbers, [42]);
    assert.equal(path.basename(calls[0].options.cwd), path.basename(workspace));
    assert.equal(calls[0].args.includes("--delivery-id"), true);
  } finally {
    await rm(workspace, { recursive: true, force: true });
  }
});

test("local delivery requires a worktree development context", () => {
  assert.equal(deliveryWorkspaceForTask({ developmentContext: { type: "branch", branch: "codex/x" } }), null);
  assert.equal(deliveryWorkspaceForTask({
    developmentContext: { type: "worktree", path: "/tmp/worktree", branch: "codex/x" },
  }), "/tmp/worktree");
});
