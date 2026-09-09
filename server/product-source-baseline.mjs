import { execFile } from "node:child_process";
import { mkdir, mkdtemp, rename, rm, stat } from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

export async function resolveProductSourceBaseline(workspacePath, sourceRoot, processEnv = process.env) {
  const git = async (...args) => {
    const { stdout } = await execFileAsync("git", ["-C", workspacePath, ...args], {
      env: { ...processEnv, GIT_TERMINAL_PROMPT: "0", GCM_INTERACTIVE: "Never" },
      timeout: 15_000,
      maxBuffer: 64 * 1024,
      windowsHide: true,
    });
    return stdout.trim();
  };

  let localBranch = null;
  let localHead = null;
  try {
    localBranch = await git("rev-parse", "--abbrev-ref", "HEAD");
    localHead = await git("rev-parse", "HEAD");
    const remote = await git("ls-remote", "--symref", "origin", "HEAD", "refs/heads/main", "refs/heads/master");
    const entries = remote.split("\n").map((line) => line.split("\t"));
    const defaultRef = entries.find(([value, name]) => name === "HEAD" && value.startsWith("ref: "))?.[0].slice(5);
    const ref = defaultRef ?? entries.find(([, name]) => name === "refs/heads/main")?.[1]
      ?? entries.find(([, name]) => name === "refs/heads/master")?.[1];
    const head = entries.find(([, name]) => name === ref)?.[0]
      ?? entries.find(([value, name]) => name === "HEAD" && !value.startsWith("ref: "))?.[0];
    if (!ref?.startsWith("refs/heads/") || !/^[a-f0-9]{40,64}$/.test(head ?? "")) {
      throw new Error("Remote default branch is unavailable");
    }
    // Fetch only the pinned object when needed; leave branches and FETCH_HEAD untouched.
    try {
      await git("cat-file", "-e", `${head}^{commit}`);
    } catch {
      await git("fetch", "--no-tags", "--no-write-fetch-head", "origin", head);
      await git("cat-file", "-e", `${head}^{commit}`);
    }
    const sourceDirectory = path.join(sourceRoot, head);
    if (!(await stat(sourceDirectory).catch(() => null))?.isDirectory()) {
      await mkdir(sourceRoot, { recursive: true });
      const temporary = await mkdtemp(path.join(sourceRoot, ".prepare-"));
      try {
        const archive = path.join(temporary, "source.tar");
        const extracted = path.join(temporary, "source");
        await mkdir(extracted);
        await git("archive", "--format=tar", `--output=${archive}`, head);
        await execFileAsync("tar", ["-xf", archive, "-C", extracted], {
          env: processEnv,
          timeout: 15_000,
          maxBuffer: 64 * 1024,
          windowsHide: true,
        });
        // Publish only a complete snapshot, including when two conversations prepare it.
        try {
          await rename(extracted, sourceDirectory);
        } catch (error) {
          if (!["EEXIST", "ENOTEMPTY"].includes(error.code)) throw error;
        }
      } finally {
        await rm(temporary, { recursive: true, force: true });
      }
    }
    return {
      status: "verified",
      localBranch,
      localHead,
      branch: ref.slice("refs/heads/".length),
      head,
      sourceDirectory,
      checkedAt: new Date().toISOString(),
    };
  } catch {
    return { status: "unverified", localBranch, localHead };
  }
}

export function productSourceBaselinePrompt(baseline) {
  const local = `项目目录当前分支：${baseline.localBranch ?? "未核实"}；提交：${baseline.localHead ?? "未核实"}。`;
  if (baseline.status !== "verified") {
    return [
      local,
      "本轮未能核实远端主线，当前目录和本地远端引用可能过期。必须向用户说明这一限制。",
      "可以继续澄清需求或报告本地已发现的实现，但不得把本地未找到等同于产品没有该能力，也不得声称已核实最新主线或线上状态。",
    ].join("\n");
  }
  return [
    local,
    `本轮现有能力分析基准：origin/${baseline.branch}，精确提交 ${baseline.head}；远端核实时间：${baseline.checkedAt}。`,
    `已导出的完整主线源码快照目录：${JSON.stringify(baseline.sourceDirectory)}。服务端已完成 Git 核实与导出，你可直接读取其中的文件，无需执行 Git。`,
    "分析现有能力必须读取上述精确提交中的源码和文档，不得把工作目录、当前 HEAD 或可变的本地远端引用当成主线。",
    "使用文件读取工具或 rg --hidden --no-ignore 在上述快照目录中查找、搜索和读取源码，避免缓存目录的忽略规则隐藏文件；所有源码搜索都显式指定快照绝对路径。引用证据时注明提交和快照内的文件路径。",
    "工作目录中的 docs/blueprint.md、docs/blueprint/、产品方案及其他未提交文档仅作补充，须标明其来源；与主线实现冲突时不得用其否定主线已有能力。",
    "已有对话或已保存方案中关于能力缺失的结论也须按本轮基准重新核对，发现错误应明确纠正。类型声明、文档与源码不一致时先在该提交追踪真实调用路径，不要直接判为残留契约。",
    "主线已有实现不等于线上已部署。将主线能力、未合并方案和线上核实结果分开表述；不得切换分支、重置或覆盖用户工作目录。",
  ].join("\n");
}
