import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";

import { parseDeliveryProjects, runGitHubDelivery } from "../server/delivery-automation.mjs";

const config = parseDeliveryProjects({
  skillhub: {
    repository: "example/skillhub",
    workflow: "pr-batch-test-deploy.yml",
    acceptanceUrl: "https://test.example.com",
  },
}).get("skillhub");

test("GitHub delivery dispatches the configured workflow and reads its manifest", async () => {
  const calls = [];
  const deliveryId = "delivery-123";
  const command = async (_executable, args) => {
    calls.push(args);
    if (args[0] === "pr") {
      return JSON.stringify([{
        number: 42,
        url: "https://github.com/example/skillhub/pull/42",
        headRefName: "codex/feature",
        baseRefName: "main",
      }]);
    }
    if (args[0] === "workflow") return "";
    if (args[0] === "run" && args[1] === "list") {
      return JSON.stringify([{
        databaseId: 100,
        displayTitle: `Acceptance delivery ${deliveryId}`,
        status: "in_progress",
        conclusion: null,
        url: "https://github.com/example/skillhub/actions/runs/100",
        createdAt: new Date().toISOString(),
      }]);
    }
    if (args[0] === "run" && args[1] === "view") {
      return JSON.stringify({
        databaseId: 100,
        status: "completed",
        conclusion: "success",
        url: "https://github.com/example/skillhub/actions/runs/100",
      });
    }
    if (args[0] === "run" && args[1] === "download") {
      const directory = args[args.indexOf("--dir") + 1];
      await writeFile(path.join(directory, "delivery-manifest.json"), JSON.stringify({
        schemaVersion: 1,
        deliveryId,
        acceptanceUrl: "https://test.example.com",
        workflowRun: "https://github.com/example/skillhub/actions/runs/100",
        immutableTag: "manual-test-hk-100-abcdef0",
        mergedSha: "abcdef0123456789",
        prNumbers: [42],
      }));
      return "";
    }
    throw new Error(`Unexpected command: ${args.join(" ")}`);
  };

  const result = await runGitHubDelivery({
    deliveryId,
    branch: "codex/feature",
    config,
    command,
    wait: async () => {},
  });

  assert.equal(result.acceptanceUrl, "https://test.example.com");
  assert.equal(result.immutableTag, "manual-test-hk-100-abcdef0");
  assert.deepEqual(result.prNumbers, [42]);
  assert.equal(calls.some((args) => args.includes(`delivery_id=${deliveryId}`)), true);
});

test("GitHub delivery fails clearly when the bound branch has no open PR", async () => {
  await assert.rejects(
    runGitHubDelivery({
      deliveryId: "delivery-no-pr",
      branch: "codex/missing",
      config,
      command: async () => "[]",
      wait: async () => {},
    }),
    /No open example\/skillhub PR from 'codex\/missing' to 'main'/,
  );
});
