import { useCallback, useDeferredValue, useEffect, useMemo, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import {
  approveProductSession,
  approveTechnicalDocument,
  createProductSession,
  getProductSession,
  listProductSessions,
  recordProductAcceptance,
  saveProductDocument,
  saveTechnicalDocument,
  startProductSessionTurn,
  startTechnicalSessionTurn,
  submitProductAcceptanceReview,
  subscribeProductSession,
} from "../api";
import { useTaskboardI18n } from "../i18n";
import type { ProductSession, ProductSessionSnapshot, Task } from "../types";
import { PlusIcon, SendIcon } from "./SemanticIcons";
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
  const [newTitle, setNewTitle] = useState("");
  const [creating, setCreating] = useState(false);
  const [message, setMessage] = useState("");
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
      const session = await createProductSession({ projectId, title });
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

  async function sendMessage() {
    const canSend = technicalMode ? canTechnicalWrite && !technicalApproved : canWrite && !isApproved;
    if (!canSend || !snapshot || !message.trim() || isRunning) return;
    const content = message.trim();
    setBusy(true);
    try {
      if (technicalMode) await startTechnicalSessionTurn(snapshot.session.id, content);
      else await startProductSessionTurn(snapshot.session.id, content);
      setMessage("");
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
                    <ReactMarkdown remarkPlugins={[remarkGfm]}>{event.content}</ReactMarkdown>
                  </div>
                </article>
              ))}
              {isRunning && <div className="product-agent-running">{text("Agent 正在整理…", "Agent is working…")}</div>}
            </div>
            <div className="product-composer">
              <textarea
                value={message}
                onChange={(event) => setMessage(event.target.value)}
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
              <button
                className="icon-button product-send-button"
                type="button"
                onClick={() => void sendMessage()}
                disabled={busy || isRunning || !message.trim() || (technicalMode
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
