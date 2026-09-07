import {
  useCallback,
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import type { ChangeEvent, ClipboardEvent } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import {
  approveProductSession,
  approveTechnicalDocument,
  createProductSession,
  getProductCollaborationCatalog,
  getProductSession,
  listProductSessions,
  recordProductAcceptance,
  saveProductDocument,
  saveTechnicalDocument,
  startProductSessionTurn,
  startTechnicalSessionTurn,
  submitProductAcceptanceReview,
  subscribeProductSession,
  updateProductSessionAgentSettings,
  updateTechnicalSessionAgentSettings,
} from "../api";
import { reasoningEffortForModel } from "../aiChatState";
import { useTaskboardI18n } from "../i18n";
import type {
  AiChatModel,
  AiChatAttachmentInput,
  ProductAgentSettings,
  ProductAiEvent,
  ProductSession,
  ProductSessionSnapshot,
  Task,
} from "../types";
import { AttachmentIcon, PlusIcon, SendIcon } from "./SemanticIcons";
import "./ProductCollaborationView.css";

interface ProductCollaborationViewProps {
  projectId: string;
  canWrite: boolean;
  canTechnicalWrite: boolean;
  tasks: Task[];
  onTaskCreated: (task: Task) => void;
  onOpenTask: (taskId: string) => void;
  onError: (error: unknown) => void;
}

const STATUS_LABELS = {
  discovery: ["需求澄清", "Discovery"],
  draft: ["方案草稿", "Draft"],
  approved: ["已确认", "Approved"],
  handed_off: ["技术接手", "Handed off"],
} as const;

const DELIVERY_STATUS_LABELS = {
  ready: ["开发就绪", "Ready for development"],
  development: ["开发中", "In development"],
  review: ["待产品验收", "Awaiting acceptance"],
  changes: ["修改中", "Changes requested"],
  accepted: ["已完成", "Completed"],
} as const;

const EFFORT_LABELS: Record<string, readonly [string, string]> = {
  minimal: ["最低", "Minimal"],
  low: ["轻度", "Low"],
  medium: ["中", "Medium"],
  high: ["高", "High"],
  xhigh: ["极高", "Extra high"],
  max: ["最高", "Maximum"],
  ultra: ["超高", "Ultra"],
};

const PRODUCT_IMAGE_TYPES = new Set([
  "image/gif",
  "image/jpeg",
  "image/png",
  "image/webp",
]);

interface PendingProductImage extends AiChatAttachmentInput {
  id: string;
  previewUrl: string;
}

function eventAttachments(event: ProductAiEvent) {
  const values = event.data?.attachments;
  if (!Array.isArray(values)) return [];
  return values.flatMap((value) => {
    if (!value || typeof value !== "object") return [];
    const attachment = value as Record<string, unknown>;
    return typeof attachment.filename === "string" ? [attachment.filename] : [];
  });
}

export function ProductCollaborationView({
  projectId,
  canWrite,
  canTechnicalWrite,
  tasks,
  onTaskCreated,
  onOpenTask,
  onError,
}: ProductCollaborationViewProps) {
  const { text } = useTaskboardI18n();
  const [sessions, setSessions] = useState<ProductSession[]>([]);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [sessionQuery, setSessionQuery] = useState("");
  const deferredSessionQuery = useDeferredValue(sessionQuery);
  const [sessionStatus, setSessionStatus] = useState<ProductSession["status"] | "all">("all");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [snapshot, setSnapshot] = useState<ProductSessionSnapshot | null>(null);
  const [models, setModels] = useState<AiChatModel[]>([]);
  const [newTitle, setNewTitle] = useState("");
  const [newModel, setNewModel] = useState("");
  const [newReasoningEffort, setNewReasoningEffort] = useState("");
  const [creating, setCreating] = useState(false);
  const [message, setMessage] = useState("");
  const [images, setImages] = useState<PendingProductImage[]>([]);
  const [document, setDocument] = useState("");
  const [technicalDocument, setTechnicalDocument] = useState("");
  const [documentTab, setDocumentTab] = useState<"product" | "technical">("product");
  const [deliveryNote, setDeliveryNote] = useState("");
  const [implementationPr, setImplementationPr] = useState("");
  const [testDeploymentUrl, setTestDeploymentUrl] = useState("");
  const [testDeploymentWorkflowRun, setTestDeploymentWorkflowRun] = useState("");
  const [testDeploymentImmutableTag, setTestDeploymentImmutableTag] = useState("");
  const [testDeploymentPrNumbers, setTestDeploymentPrNumbers] = useState("");
  const [acceptanceNote, setAcceptanceNote] = useState("");
  const [busy, setBusy] = useState(false);
  const [settingsSaving, setSettingsSaving] = useState(false);
  const imageInputRef = useRef<HTMLInputElement>(null);

  const refreshList = useCallback(async () => {
    const result = await listProductSessions(projectId, {
      page,
      pageSize: 15,
      query: deferredSessionQuery,
      status: sessionStatus === "all" ? null : sessionStatus,
    });
    setSessions(result.data);
    setTotalPages(Math.max(1, result.pagination.totalPages));
    setSelectedId((current) => (
      current && result.data.some((item) => item.id === current)
        ? current
        : result.data[0]?.id ?? null
    ));
  }, [deferredSessionQuery, page, projectId, sessionStatus]);

  const refreshSession = useCallback(async (sessionId: string) => {
    const next = await getProductSession(sessionId);
    setSnapshot(next);
    setDocument(next.session.productDocument);
    setTechnicalDocument(next.session.technicalDocument);
    setDeliveryNote(next.session.deliveryNote);
    setImplementationPr(next.session.implementationPr ?? "");
    setTestDeploymentUrl(next.session.testDeploymentUrl ?? "");
    setTestDeploymentWorkflowRun(next.session.testDeploymentWorkflowRun ?? "");
    setTestDeploymentImmutableTag(next.session.testDeploymentImmutableTag ?? "");
    setTestDeploymentPrNumbers(next.session.testDeploymentPrNumbers.join(", "));
    setAcceptanceNote(next.session.acceptanceNote ?? "");
    setSessions((current) => current.map((item) => (
      item.id === next.session.id ? next.session : item
    )));
  }, []);

  useEffect(() => {
    setPage(1);
    setSessionQuery("");
    setSessionStatus("all");
    setSelectedId(null);
    setSnapshot(null);
  }, [projectId]);

  useEffect(() => {
    const controller = new AbortController();
    setModels([]);
    void getProductCollaborationCatalog(projectId, controller.signal)
      .then((catalog) => {
        setModels(catalog.models);
        const defaultModel = catalog.models[0];
        setNewModel(defaultModel?.slug ?? "");
        setNewReasoningEffort(defaultModel?.defaultReasoningEffort ?? "");
      })
      .catch((error) => {
        if (!controller.signal.aborted) onError(error);
      });
    return () => controller.abort();
  }, [onError, projectId]);

  useEffect(() => {
    void refreshList().catch(onError);
  }, [onError, refreshList]);

  useEffect(() => {
    if (!selectedId) {
      setSnapshot(null);
      return;
    }
    void refreshSession(selectedId).catch(onError);
    return subscribeProductSession(
      selectedId,
      () => void refreshSession(selectedId).catch(onError),
    );
  }, [onError, refreshSession, selectedId]);

  const visibleEvents = useMemo(() => (
    (canTechnicalWrite && snapshot?.session.status === "handed_off"
      ? snapshot.technicalEvents
      : snapshot?.events
    )?.filter((event) => (
      event.role === "user" || event.role === "assistant" || event.role === "error"
    )) ?? []
  ), [canTechnicalWrite, snapshot]);
  const latestAssistant = useMemo(() => (
    [...visibleEvents].reverse().find((event) => event.role === "assistant")?.content ?? ""
  ), [visibleEvents]);
  const technicalMode = canTechnicalWrite && snapshot?.session.status === "handed_off";
  const isRunning = technicalMode
    ? snapshot?.technicalRuns.some((run) => run.status === "running") ?? false
    : snapshot?.runs.some((run) => run.status === "running") ?? false;
  const isApproved = snapshot?.session.status === "approved"
    || snapshot?.session.status === "handed_off";
  const documentDirty = snapshot ? document !== snapshot.session.productDocument : false;
  const technicalDocumentDirty = snapshot
    ? technicalDocument !== snapshot.session.technicalDocument
    : false;
  const technicalApproved = snapshot?.session.technicalApprovedDocument !== null
    && snapshot?.session.technicalApprovedDocument !== undefined;
  const technicalTask = snapshot?.session.technicalTaskId
    ? tasks.find((task) => task.id === snapshot.session.technicalTaskId) ?? null
    : null;
  const defaultAgentSettings = useMemo<ProductAgentSettings | null>(() => {
    const model = models[0];
    return model ? {
      model: model.slug,
      reasoningEffort: model.defaultReasoningEffort,
    } : null;
  }, [models]);
  const activeAgentSettings = technicalMode
    ? snapshot?.technicalAgent ?? defaultAgentSettings
    : snapshot?.productAgent ?? defaultAgentSettings;
  const activeModel = models.find((model) => model.slug === activeAgentSettings?.model) ?? models[0];
  const canConfigureAgent = technicalMode
    ? canTechnicalWrite && !technicalApproved
    : canWrite && !isApproved;

  const sessionStage = useCallback((session: ProductSession) => {
    const task = session.technicalTaskId
      ? tasks.find((candidate) => candidate.id === session.technicalTaskId)
      : null;
    if (session.acceptanceStatus === "accepted" || task?.status === "done") return "accepted";
    if (task?.status === "in_review") return "review";
    if (session.acceptanceStatus === "changes_requested") return "changes";
    if (task?.status === "in_progress" || task?.status === "blocked") return "development";
    if (session.technicalApprovedDocument) return "ready";
    return null;
  }, [tasks]);

  useEffect(() => {
    setDocumentTab(technicalMode ? "technical" : "product");
    setImages([]);
  }, [selectedId, technicalMode]);

  useEffect(() => {
    if (!isRunning || !selectedId) return;
    const timer = window.setInterval(() => {
      void refreshSession(selectedId).catch(onError);
    }, 800);
    return () => window.clearInterval(timer);
  }, [isRunning, onError, refreshSession, selectedId]);

  async function createSession() {
    if (!canWrite) return;
    const title = newTitle.trim();
    if (!title) return;
    setBusy(true);
    try {
      const session = await createProductSession({
        projectId,
        title,
        ...(newModel ? { model: newModel } : {}),
        ...(newReasoningEffort ? { reasoningEffort: newReasoningEffort } : {}),
      });
      setPage(1);
      setSessionQuery("");
      setSessionStatus("all");
      setSessions((current) => [session, ...current.filter((item) => item.id !== session.id)]);
      setSelectedId(session.id);
      setNewTitle("");
      setCreating(false);
    } catch (error) {
      onError(error);
    } finally {
      setBusy(false);
    }
  }

  async function saveAgentSettings(next: ProductAgentSettings) {
    if (!snapshot || !canConfigureAgent || isRunning) return;
    setSettingsSaving(true);
    try {
      const settings = technicalMode
        ? await updateTechnicalSessionAgentSettings(snapshot.session.id, next)
        : await updateProductSessionAgentSettings(snapshot.session.id, next);
      setSnapshot((current) => current ? {
        ...current,
        ...(technicalMode ? { technicalAgent: settings } : { productAgent: settings }),
      } : current);
    } catch (error) {
      onError(error);
    } finally {
      setSettingsSaving(false);
    }
  }

  function selectNewModel(modelSlug: string) {
    const model = models.find((candidate) => candidate.slug === modelSlug);
    if (!model) return;
    setNewModel(model.slug);
    setNewReasoningEffort(reasoningEffortForModel(model, newReasoningEffort));
  }

  function selectActiveModel(modelSlug: string) {
    const model = models.find((candidate) => candidate.slug === modelSlug);
    if (!model) return;
    void saveAgentSettings({
      model: model.slug,
      reasoningEffort: reasoningEffortForModel(model, activeAgentSettings?.reasoningEffort),
    });
  }

  async function addImages(files: File[]) {
    const accepted = files
      .filter((file) => PRODUCT_IMAGE_TYPES.has(file.type))
      .slice(0, Math.max(0, 10 - images.length));
    if (accepted.length === 0) return;
    try {
      const next = await Promise.all(accepted.map((file, index) => (
        new Promise<PendingProductImage>((resolve, reject) => {
          const reader = new FileReader();
          reader.onload = () => {
            if (typeof reader.result !== "string") {
              reject(new Error(text(`无法读取图片 ${file.name}`, `Could not read ${file.name}`)));
              return;
            }
            const separator = reader.result.indexOf(",");
            resolve({
              id: `${file.name}-${file.size}-${file.lastModified}-${Date.now()}-${index}`,
              filename: file.name,
              contentType: file.type,
              dataBase64: reader.result.slice(separator + 1),
              previewUrl: reader.result,
            });
          };
          reader.onerror = () => reject(new Error(text(
            `无法读取图片 ${file.name}`,
            `Could not read ${file.name}`,
          )));
          reader.readAsDataURL(file);
        })
      )));
      setImages((current) => [...current, ...next].slice(0, 10));
    } catch (error) {
      onError(error);
    }
  }

  function selectImages(event: ChangeEvent<HTMLInputElement>) {
    const files = Array.from(event.currentTarget.files ?? []);
    event.currentTarget.value = "";
    void addImages(files);
  }

  function pasteImages(event: ClipboardEvent<HTMLTextAreaElement>) {
    const files = Array.from(event.clipboardData.files)
      .filter((file) => PRODUCT_IMAGE_TYPES.has(file.type));
    if (files.length === 0) return;
    event.preventDefault();
    void addImages(files);
  }

  async function sendMessage() {
    const canSend = technicalMode ? canTechnicalWrite && !technicalApproved : canWrite && !isApproved;
    if (!canSend || !snapshot || (!message.trim() && images.length === 0) || isRunning) return;
    const content = message.trim();
    const attachments = images.map(({ filename, contentType, dataBase64 }) => ({
      filename,
      contentType,
      dataBase64,
    }));
    setBusy(true);
    try {
      if (technicalMode) await startTechnicalSessionTurn(snapshot.session.id, content, attachments);
      else await startProductSessionTurn(snapshot.session.id, content, attachments);
      setMessage("");
      setImages([]);
      await refreshSession(snapshot.session.id);
    } catch (error) {
      onError(error);
    } finally {
      setBusy(false);
    }
  }

  async function persistTechnicalDocument() {
    if (!canTechnicalWrite || !snapshot || technicalApproved || !technicalDocumentDirty) return;
    setBusy(true);
    try {
      const session = await saveTechnicalDocument(
        snapshot.session.id,
        technicalDocument,
        snapshot.session.technicalDocumentVersion,
      );
      setSnapshot((current) => current ? { ...current, session } : current);
      setSessions((current) => current.map((item) => item.id === session.id ? session : item));
    } catch (error) {
      onError(error);
    } finally {
      setBusy(false);
    }
  }

  async function approveTechnical() {
    if (!canTechnicalWrite || !snapshot || technicalApproved || technicalDocumentDirty || !technicalDocument.trim()) return;
    setBusy(true);
    try {
      const result = await approveTechnicalDocument(snapshot.session.id);
      setSnapshot((current) => current ? { ...current, session: result.session } : current);
      setSessions((current) => current.map((item) => (
        item.id === result.session.id ? result.session : item
      )));
      onTaskCreated(result.task);
    } catch (error) {
      onError(error);
    } finally {
      setBusy(false);
    }
  }

  async function submitReview() {
    const prNumbers = [...new Set(testDeploymentPrNumbers
      .split(",")
      .map((value) => Number(value.trim()))
      .filter((value) => Number.isSafeInteger(value) && value > 0))];
    if (
      !canTechnicalWrite
      || !snapshot
      || !technicalApproved
      || !deliveryNote.trim()
      || !implementationPr.trim()
      || !testDeploymentUrl.trim()
      || !testDeploymentWorkflowRun.trim()
      || !testDeploymentImmutableTag.trim()
      || prNumbers.length === 0
    ) return;
    setBusy(true);
    try {
      const result = await submitProductAcceptanceReview(snapshot.session.id, {
        note: deliveryNote.trim(),
        implementationPr: implementationPr.trim(),
        testDeployment: {
          url: testDeploymentUrl.trim(),
          workflowRun: testDeploymentWorkflowRun.trim(),
          immutableTag: testDeploymentImmutableTag.trim(),
          prNumbers,
        },
      });
      setSnapshot((current) => current ? { ...current, session: result.session } : current);
      setSessions((current) => current.map((item) => (
        item.id === result.session.id ? result.session : item
      )));
      onTaskCreated(result.task);
    } catch (error) {
      onError(error);
    } finally {
      setBusy(false);
    }
  }

  async function decideAcceptance(outcome: "accepted" | "changes_requested") {
    if (!canWrite || !snapshot || technicalTask?.status !== "in_review") return;
    if (outcome === "changes_requested" && !acceptanceNote.trim()) return;
    setBusy(true);
    try {
      const result = await recordProductAcceptance(
        snapshot.session.id,
        outcome,
        acceptanceNote.trim(),
      );
      setSnapshot((current) => current ? { ...current, session: result.session } : current);
      setSessions((current) => current.map((item) => (
        item.id === result.session.id ? result.session : item
      )));
      onTaskCreated(result.task);
    } catch (error) {
      onError(error);
    } finally {
      setBusy(false);
    }
  }

  async function persistDocument() {
    if (!canWrite || !snapshot || isApproved || !documentDirty) return;
    setBusy(true);
    try {
      const session = await saveProductDocument(
        snapshot.session.id,
        document,
        snapshot.session.documentVersion,
      );
      setSnapshot((current) => current ? { ...current, session } : current);
      setSessions((current) => current.map((item) => item.id === session.id ? session : item));
    } catch (error) {
      onError(error);
    } finally {
      setBusy(false);
    }
  }

  async function approve() {
    if (!canWrite || !snapshot || isApproved || !document.trim() || documentDirty) return;
    setBusy(true);
    try {
      const result = await approveProductSession(snapshot.session.id);
      setSnapshot((current) => current ? { ...current, session: result.session } : current);
      setSessions((current) => current.map((item) => (
        item.id === result.session.id ? result.session : item
      )));
      onTaskCreated(result.task);
    } catch (error) {
      onError(error);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="product-collaboration">
      <aside className="product-session-sidebar">
        <div className="product-session-sidebar-header">
          <h2>{text("产品需求", "Product requests")}</h2>
          {canWrite && (
            <button
              className="icon-button"
              type="button"
              onClick={() => setCreating((current) => !current)}
              aria-label={text("新建产品需求", "New product request")}
              title={text("新建产品需求", "New product request")}
            >
              <PlusIcon color="currentColor" size={14} />
            </button>
          )}
        </div>
        {canWrite && creating && (
          <form className="product-session-create" onSubmit={(event) => {
            event.preventDefault();
            void createSession();
          }}>
            <div className="product-session-create-primary">
              <input
                autoFocus
                value={newTitle}
                onChange={(event) => setNewTitle(event.target.value)}
                placeholder={text("需求名称", "Request name")}
                maxLength={160}
              />
              <button className="button primary" type="submit" disabled={busy || !newTitle.trim()}>
                {text("创建", "Create")}
              </button>
            </div>
            <div className="product-agent-create-settings">
              <label>
                <span>{text("模型", "Model")}</span>
                <select
                  value={newModel}
                  onChange={(event) => selectNewModel(event.target.value)}
                  disabled={models.length === 0}
                >
                  {models.map((model) => (
                    <option value={model.slug} key={model.slug}>{model.displayName}</option>
                  ))}
                </select>
              </label>
              <label>
                <span>{text("思考", "Reasoning")}</span>
                <select
                  value={newReasoningEffort}
                  onChange={(event) => setNewReasoningEffort(event.target.value)}
                  disabled={!newModel}
                >
                  {(models.find((model) => model.slug === newModel)?.supportedReasoningEfforts ?? [])
                    .map((effort) => (
                      <option value={effort} key={effort}>
                        {EFFORT_LABELS[effort] ? text(...EFFORT_LABELS[effort]) : effort}
                      </option>
                    ))}
                </select>
              </label>
            </div>
          </form>
        )}
        <div className="product-session-filters">
          <input
            type="search"
            value={sessionQuery}
            onChange={(event) => {
              setSessionQuery(event.target.value);
              setPage(1);
            }}
            placeholder={text("搜索需求…", "Search requests…")}
            aria-label={text("搜索产品需求", "Search product requests")}
          />
          <select
            value={sessionStatus}
            onChange={(event) => {
              setSessionStatus(event.target.value as ProductSession["status"] | "all");
              setPage(1);
            }}
            aria-label={text("按状态筛选", "Filter by status")}
          >
            <option value="all">{text("全部状态", "All statuses")}</option>
            {Object.entries(STATUS_LABELS).map(([value, labels]) => (
              <option value={value} key={value}>{text(labels[0], labels[1])}</option>
            ))}
          </select>
        </div>
        <div className="product-session-list">
          {sessions.map((session) => (
            <button
              className={`product-session-item${selectedId === session.id ? " active" : ""}`}
              type="button"
              key={session.id}
              onClick={() => setSelectedId(session.id)}
            >
              <span className="product-session-title">{session.title}</span>
              <span className={`product-session-status status-${session.status}`}>
                {sessionStage(session)
                  ? text(
                      DELIVERY_STATUS_LABELS[sessionStage(session)!][0],
                      DELIVERY_STATUS_LABELS[sessionStage(session)!][1],
                    )
                  : text(STATUS_LABELS[session.status][0], STATUS_LABELS[session.status][1])}
              </span>
              <span className="product-session-owner">{session.creator.name}</span>
              <time>{new Date(session.updatedAt).toLocaleDateString()}</time>
            </button>
          ))}
        </div>
        {totalPages > 1 && (
          <nav className="product-session-pagination" aria-label={text("需求分页", "Request pagination")}>
            <button
              type="button"
              onClick={() => setPage((current) => Math.max(1, current - 1))}
              disabled={page <= 1}
              aria-label={text("上一页", "Previous page")}
              title={text("上一页", "Previous page")}
            >‹</button>
            <span>{page} / {totalPages}</span>
            <button
              type="button"
              onClick={() => setPage((current) => Math.min(totalPages, current + 1))}
              disabled={page >= totalPages}
              aria-label={text("下一页", "Next page")}
              title={text("下一页", "Next page")}
            >›</button>
          </nav>
        )}
      </aside>

      {!snapshot ? (
        <div className="product-session-empty">
          <h2>{text("创建产品需求", "Create a product request")}</h2>
        </div>
      ) : (
        <>
          <section className="product-chat">
            <header className="product-pane-header">
              <div>
                <h2>{snapshot.session.title}</h2>
                <div className="product-session-header-meta">
                  <span className={`product-session-status status-${snapshot.session.status}`}>
                    {sessionStage(snapshot.session)
                      ? text(
                          DELIVERY_STATUS_LABELS[sessionStage(snapshot.session)!][0],
                          DELIVERY_STATUS_LABELS[sessionStage(snapshot.session)!][1],
                        )
                      : text(
                          STATUS_LABELS[snapshot.session.status][0],
                          STATUS_LABELS[snapshot.session.status][1],
                        )}
                  </span>
                  <span>{text("产品负责人", "Product owner")}: {snapshot.session.creator.name}</span>
                  {snapshot.session.technicalOwner && (
                    <span>{text("技术负责人", "Technical owner")}: {snapshot.session.technicalOwner.name}</span>
                  )}
                </div>
              </div>
              <div className="product-pane-actions">
                {activeAgentSettings && (
                  <div className="product-agent-settings" aria-label={text("Agent 模型配置", "Agent model settings")}>
                    <label>
                      <span>{text("模型", "Model")}</span>
                      <select
                        value={activeAgentSettings.model}
                        onChange={(event) => selectActiveModel(event.target.value)}
                        disabled={!canConfigureAgent || isRunning || settingsSaving || models.length === 0}
                        aria-label={text("选择模型", "Select model")}
                      >
                        {models.map((model) => (
                          <option value={model.slug} key={model.slug}>{model.displayName}</option>
                        ))}
                      </select>
                    </label>
                    <label>
                      <span>{text("思考", "Reasoning")}</span>
                      <select
                        value={activeAgentSettings.reasoningEffort}
                        onChange={(event) => void saveAgentSettings({
                          model: activeAgentSettings.model,
                          reasoningEffort: event.target.value,
                        })}
                        disabled={!canConfigureAgent || isRunning || settingsSaving || !activeModel}
                        aria-label={text("选择思考强度", "Select reasoning effort")}
                      >
                        {(activeModel?.supportedReasoningEfforts ?? []).map((effort) => (
                          <option value={effort} key={effort}>
                            {EFFORT_LABELS[effort] ? text(...EFFORT_LABELS[effort]) : effort}
                          </option>
                        ))}
                      </select>
                    </label>
                  </div>
                )}
                {snapshot.session.technicalTaskId && (
                  <button
                    className="technical-task-link"
                    type="button"
                    onClick={() => onOpenTask(snapshot.session.technicalTaskId!)}
                  >
                    {technicalTask
                      ? text(`开发任务 ${technicalTask.identifier}`, `Development issue ${technicalTask.identifier}`)
                      : text("打开技术任务", "Open technical issue")}
                  </button>
                )}
              </div>
            </header>
            <div className="product-message-list">
              {visibleEvents.map((event) => (
                <article className={`product-message role-${event.role}`} key={event.id}>
                  <span>{event.role === "user"
                    ? technicalMode ? text("技术", "Engineering") : text("产品", "Product")
                    : event.role === "assistant"
                      ? "Agent"
                      : text("错误", "Error")}</span>
                  <div className="product-message-content">
                    {event.content && <ReactMarkdown remarkPlugins={[remarkGfm]}>{event.content}</ReactMarkdown>}
                    {eventAttachments(event).length > 0 && (
                      <div className="product-message-attachments">
                        {eventAttachments(event).map((filename, index) => (
                          <span key={`${filename}-${index}`}>
                            <AttachmentIcon color="currentColor" />
                            {filename}
                          </span>
                        ))}
                      </div>
                    )}
                  </div>
                </article>
              ))}
              {isRunning && <div className="product-agent-running">{text("Agent 正在整理…", "Agent is working…")}</div>}
            </div>
            <div className="product-composer">
              {images.length > 0 && (
                <div className="product-composer-images">
                  {images.map((image) => (
                    <div className="product-composer-image" key={image.id}>
                      <img src={image.previewUrl} alt={image.filename} />
                      <button
                        type="button"
                        onClick={() => setImages((current) => current.filter((item) => item.id !== image.id))}
                        aria-label={text(`移除图片 ${image.filename}`, `Remove image ${image.filename}`)}
                        title={text("移除图片", "Remove image")}
                      >×</button>
                    </div>
                  ))}
                </div>
              )}
              <textarea
                value={message}
                onChange={(event) => setMessage(event.target.value)}
                onPaste={pasteImages}
                onKeyDown={(event) => {
                  if (event.key === "Enter" && !event.shiftKey) {
                    event.preventDefault();
                    void sendMessage();
                  }
                }}
                placeholder={technicalMode
                  ? text("继续讨论技术方案…", "Continue the technical design…")
                  : text("继续澄清需求…", "Continue the product discussion…")}
                disabled={technicalMode ? technicalApproved || !canTechnicalWrite : isApproved || !canWrite}
              />
              <input
                ref={imageInputRef}
                className="product-image-input"
                type="file"
                accept="image/png,image/jpeg,image/gif,image/webp"
                multiple
                tabIndex={-1}
                onChange={selectImages}
              />
              <button
                className="icon-button product-image-button"
                type="button"
                onClick={() => imageInputRef.current?.click()}
                disabled={busy || isRunning || images.length >= 10 || (technicalMode
                  ? !canTechnicalWrite || technicalApproved
                  : !canWrite || isApproved)}
                aria-label={text("添加图片", "Add images")}
                title={text("添加图片", "Add images")}
              >
                <AttachmentIcon color="currentColor" />
              </button>
              <button
                className="icon-button product-send-button"
                type="button"
                onClick={() => void sendMessage()}
                disabled={busy || isRunning || (!message.trim() && images.length === 0) || (technicalMode
                  ? !canTechnicalWrite || technicalApproved
                  : !canWrite || isApproved)}
                aria-label={text("发送", "Send")}
                title={text("发送", "Send")}
              >
                <SendIcon color="currentColor" />
              </button>
            </div>
          </section>

          <section className="product-document-pane">
            <header className="product-pane-header">
              <div>
                <div className="collaboration-document-tabs" role="tablist" aria-label={text("协作文档", "Collaboration documents")}>
                  <button
                    type="button"
                    role="tab"
                    aria-selected={documentTab === "product"}
                    className={documentTab === "product" ? "active" : ""}
                    onClick={() => setDocumentTab("product")}
                  >{text("产品方案", "Product spec")}</button>
                  {snapshot.session.status === "handed_off" && (
                    <button
                      type="button"
                      role="tab"
                      aria-selected={documentTab === "technical"}
                      className={documentTab === "technical" ? "active" : ""}
                      onClick={() => setDocumentTab("technical")}
                    >{text("技术方案", "Technical design")}</button>
                  )}
                </div>
                <span>v{documentTab === "technical"
                  ? snapshot.session.technicalDocumentVersion
                  : snapshot.session.documentVersion}</span>
                <code className="collaboration-artifact-path">{snapshot.session.artifactPath}</code>
              </div>
              {documentTab === "product" && canWrite && !isApproved && latestAssistant && (
                <button className="button secondary" type="button" onClick={() => setDocument(latestAssistant)}>
                  {text("采用最近回复", "Use latest response")}
                </button>
              )}
              {documentTab === "technical" && canTechnicalWrite && !technicalApproved && latestAssistant && (
                <button className="button secondary" type="button" onClick={() => setTechnicalDocument(latestAssistant)}>
                  {text("采用最近回复", "Use latest response")}
                </button>
              )}
            </header>
            <textarea
              className="product-document-editor"
              value={documentTab === "technical" ? technicalDocument : document}
              onChange={(event) => documentTab === "technical"
                ? setTechnicalDocument(event.target.value)
                : setDocument(event.target.value)}
              placeholder={documentTab === "technical"
                ? text("技术方案 Markdown", "Technical design Markdown")
                : text("产品方案 Markdown", "Product spec Markdown")}
              readOnly={documentTab === "technical"
                ? technicalApproved || !canTechnicalWrite
                : isApproved || !canWrite}
            />
            <footer className="product-document-actions">
              {documentTab === "technical" ? technicalApproved ? (
                <span>{text(
                  `${snapshot.session.technicalApprovedBy ?? "技术"} 已确认 v${snapshot.session.technicalApprovedVersion ?? snapshot.session.technicalDocumentVersion}`,
                  `Approved by ${snapshot.session.technicalApprovedBy ?? "Engineering"} at v${snapshot.session.technicalApprovedVersion ?? snapshot.session.technicalDocumentVersion}`,
                )}</span>
              ) : !canTechnicalWrite ? (
                <span>{text("产品角色只读查看技术方案", "Technical designs are read-only for product members")}</span>
              ) : (
                <>
                  <button
                    className="button secondary"
                    type="button"
                    onClick={() => void persistTechnicalDocument()}
                    disabled={busy || !technicalDocumentDirty}
                  >
                    {text("保存技术方案", "Save technical design")}
                  </button>
                  <button
                    className="button primary"
                    type="button"
                    onClick={() => void approveTechnical()}
                    disabled={busy || technicalDocumentDirty || !technicalDocument.trim()}
                  >
                    {text("确认方案，进入开发", "Approve and start development")}
                  </button>
                </>
              ) : isApproved ? (
                <span>{text(
                  `${snapshot.session.approvedBy ?? "产品"} 已确认 v${snapshot.session.approvedVersion ?? snapshot.session.documentVersion}`,
                  `Approved by ${snapshot.session.approvedBy ?? "Product"} at v${snapshot.session.approvedVersion ?? snapshot.session.documentVersion}`,
                )}</span>
              ) : !canWrite ? (
                <span>{text("技术角色只读查看产品方案", "Product specs are read-only for technical members")}</span>
              ) : (
                <>
                  <button
                    className="button secondary"
                    type="button"
                    onClick={() => void persistDocument()}
                    disabled={busy || !documentDirty}
                  >
                    {text("保存方案", "Save spec")}
                  </button>
                  <button
                    className="button primary"
                    type="button"
                    onClick={() => void approve()}
                    disabled={busy || documentDirty || !document.trim()}
                  >
                    {text("确认并交给技术", "Approve and hand off")}
                  </button>
                </>
              )}
            </footer>
            {technicalApproved && (
              <section className="product-acceptance-panel">
                <header>
                  <strong>{text("交付验收", "Delivery acceptance")}</strong>
                  {snapshot.session.acceptanceStatus === "accepted" && (
                    <span>{text(
                      `${snapshot.session.acceptanceBy ?? "产品"} 已验收`,
                      `Accepted by ${snapshot.session.acceptanceBy ?? "Product"}`,
                    )}</span>
                  )}
                  {technicalTask?.status === "in_review" && (
                    <span>{text("等待产品确认", "Waiting for product acceptance")}</span>
                  )}
                </header>
                {technicalMode ? (
                  <>
                    {snapshot.session.acceptanceStatus === "changes_requested" && snapshot.session.acceptanceNote && (
                      <p className="acceptance-return-note">
                        {text("产品退回", "Returned by product")}: {snapshot.session.acceptanceNote}
                      </p>
                    )}
                    <textarea
                      value={deliveryNote}
                      onChange={(event) => setDeliveryNote(event.target.value)}
                      placeholder={text("填写实现范围、测试结果和验收地址…", "Add implementation scope, test results, and acceptance URL…")}
                      disabled={snapshot.session.acceptanceStatus === "accepted"}
                    />
                    <div className="delivery-evidence-grid">
                      <label>
                        <span>{text("实现 PR", "Implementation PR")}</span>
                        <input
                          type="url"
                          value={implementationPr}
                          onChange={(event) => setImplementationPr(event.target.value)}
                          placeholder="https://github.com/org/repo/pull/123"
                          disabled={snapshot.session.acceptanceStatus === "accepted"}
                        />
                      </label>
                      <label>
                        <span>{text("共享测试地址", "Shared test URL")}</span>
                        <input
                          type="url"
                          value={testDeploymentUrl}
                          onChange={(event) => setTestDeploymentUrl(event.target.value)}
                          placeholder="https://test.example.com"
                          disabled={snapshot.session.acceptanceStatus === "accepted"}
                        />
                      </label>
                      <label>
                        <span>{text("部署工作流", "Deployment workflow")}</span>
                        <input
                          type="url"
                          value={testDeploymentWorkflowRun}
                          onChange={(event) => setTestDeploymentWorkflowRun(event.target.value)}
                          placeholder="https://github.com/org/repo/actions/runs/123"
                          disabled={snapshot.session.acceptanceStatus === "accepted"}
                        />
                      </label>
                      <label>
                        <span>{text("不可变标签", "Immutable tag")}</span>
                        <input
                          value={testDeploymentImmutableTag}
                          onChange={(event) => setTestDeploymentImmutableTag(event.target.value)}
                          placeholder="manual-test-123-abcdef0"
                          disabled={snapshot.session.acceptanceStatus === "accepted"}
                        />
                      </label>
                      <label className="delivery-evidence-wide">
                        <span>{text("部署包含的 PR 编号", "Deployed PR numbers")}</span>
                        <input
                          value={testDeploymentPrNumbers}
                          onChange={(event) => setTestDeploymentPrNumbers(event.target.value)}
                          placeholder={text("例如 123, 124", "For example 123, 124")}
                          disabled={snapshot.session.acceptanceStatus === "accepted"}
                        />
                      </label>
                    </div>
                    <button
                      className="button primary"
                      type="button"
                      onClick={() => void submitReview()}
                      disabled={busy
                        || !deliveryNote.trim()
                        || !implementationPr.trim()
                        || !testDeploymentUrl.trim()
                        || !testDeploymentWorkflowRun.trim()
                        || !testDeploymentImmutableTag.trim()
                        || !testDeploymentPrNumbers.trim()
                        || snapshot.session.acceptanceStatus === "accepted"
                        || technicalTask?.status === "in_review"}
                    >{text("提交产品验收", "Submit for acceptance")}</button>
                  </>
                ) : (
                  <>
                    <div className="delivery-note-content">
                      {snapshot.session.deliveryNote || text("技术尚未提交验收说明", "Engineering has not submitted delivery evidence")}
                    </div>
                    {snapshot.session.deliverySubmittedAt && (
                      <dl className="delivery-evidence-summary">
                        <div><dt>{text("实现 PR", "Implementation PR")}</dt><dd><a href={snapshot.session.implementationPr ?? "#"} target="_blank" rel="noreferrer">{snapshot.session.implementationPr}</a></dd></div>
                        <div><dt>{text("测试地址", "Test URL")}</dt><dd><a href={snapshot.session.testDeploymentUrl ?? "#"} target="_blank" rel="noreferrer">{snapshot.session.testDeploymentUrl}</a></dd></div>
                        <div><dt>{text("不可变标签", "Immutable tag")}</dt><dd><code>{snapshot.session.testDeploymentImmutableTag}</code></dd></div>
                        <div><dt>{text("PR 列表", "PR list")}</dt><dd>{snapshot.session.testDeploymentPrNumbers.join(", ")}</dd></div>
                      </dl>
                    )}
                    {technicalTask?.status === "in_review" && canWrite && (
                      <>
                        <textarea
                          value={acceptanceNote}
                          onChange={(event) => setAcceptanceNote(event.target.value)}
                          placeholder={text("验收意见；退回修改时必填…", "Acceptance note; required when requesting changes…")}
                        />
                        <div className="acceptance-actions">
                          <button
                            className="button secondary"
                            type="button"
                            onClick={() => void decideAcceptance("changes_requested")}
                            disabled={busy || !acceptanceNote.trim()}
                          >{text("退回修改", "Request changes")}</button>
                          <button
                            className="button primary"
                            type="button"
                            onClick={() => void decideAcceptance("accepted")}
                            disabled={busy}
                          >{text("验收通过", "Accept delivery")}</button>
                        </div>
                      </>
                    )}
                  </>
                )}
              </section>
            )}
          </section>
        </>
      )}
    </div>
  );
}
