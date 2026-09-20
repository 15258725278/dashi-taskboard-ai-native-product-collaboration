import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, test, vi } from "vitest";
import { ProductCollaborationView } from "./ProductCollaborationView";

const catalog = vi.hoisted(() => ({
  models: [{
    slug: "gpt-current",
    displayName: "GPT Current",
    description: "",
    defaultReasoningEffort: "medium",
    supportedReasoningEfforts: ["low", "medium"],
  }],
}));
const updateProductSessionAgentSettings = vi.hoisted(() => vi.fn(
  async (_sessionId: string, settings: { model: string; reasoningEffort: string }) => settings,
));

vi.mock("../api", () => ({
  approveProductSession: vi.fn(),
  approveTechnicalDocument: vi.fn(),
  createProductSession: vi.fn(),
  getProductCollaborationCatalog: vi.fn(async () => ({ models: catalog.models })),
  getProductSession: vi.fn(async () => ({
    session: {
      id: "session-1",
      title: "Live model catalog",
      status: "discovery",
      creator: { name: "Product" },
      updatedAt: "2026-09-16T00:00:00.000Z",
      productDocument: "",
      technicalDocument: null,
      technicalApprovedDocument: null,
      technicalTaskId: null,
      acceptanceStatus: "pending",
    },
    productAgent: { model: "removed-model", reasoningEffort: "ultra" },
    technicalAgent: null,
    deliveryRun: null,
    runs: [],
    technicalRuns: [],
    events: [],
    technicalEvents: [],
  })),
  listProductSessions: vi.fn(async () => ({
    data: [{
      id: "session-1",
      title: "Live model catalog",
      status: "discovery",
      creator: { name: "Product" },
      updatedAt: "2026-09-16T00:00:00.000Z",
    }],
    pagination: { page: 1, pageSize: 15, totalItems: 1, totalPages: 1 },
  })),
  recordProductAcceptance: vi.fn(),
  saveProductDocument: vi.fn(),
  saveTechnicalDocument: vi.fn(),
  startProductAcceptanceDelivery: vi.fn(),
  startProductSessionTurn: vi.fn(),
  startTechnicalSessionTurn: vi.fn(),
  subscribeProductSession: vi.fn(() => () => {}),
  updateProductSessionAgentSettings,
  updateTechnicalSessionAgentSettings: vi.fn(),
}));

describe("ProductCollaborationView model catalog", () => {
  afterEach(() => {
    cleanup();
    updateProductSessionAgentSettings.mockClear();
    catalog.models = [{
      slug: "gpt-current",
      displayName: "GPT Current",
      description: "",
      defaultReasoningEffort: "medium",
      supportedReasoningEfforts: ["low", "medium"],
    }];
  });

  test("refreshes a drifted model catalog when the browser window regains focus", async () => {
    render(<ProductCollaborationView
      projectId="project"
      canWrite
      canTechnicalWrite={false}
      tasks={[]}
      onTaskCreated={() => {}}
      onOpenTask={() => {}}
      onError={() => {}}
    />);

    expect(await screen.findByLabelText("Agent model settings")).toBeTruthy();
    await waitFor(() => expect(updateProductSessionAgentSettings).toHaveBeenCalledWith("session-1", {
      model: "gpt-current",
      reasoningEffort: "medium",
    }));

    catalog.models = [{
      slug: "gpt-latest",
      displayName: "GPT Latest",
      description: "",
      defaultReasoningEffort: "low",
      supportedReasoningEfforts: ["low"],
    }];
    act(() => {
      window.dispatchEvent(new Event("focus"));
    });

    await waitFor(() => expect(updateProductSessionAgentSettings).toHaveBeenCalledWith("session-1", {
      model: "gpt-latest",
      reasoningEffort: "low",
    }));
    expect((screen.getByLabelText("Select model") as HTMLSelectElement).value).toBe("gpt-latest");
    expect((screen.getByLabelText("Select reasoning effort") as HTMLSelectElement).value).toBe("low");
  });
});
