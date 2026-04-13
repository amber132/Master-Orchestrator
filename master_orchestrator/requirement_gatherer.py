"""Requirement gathering module: assess goal sufficiency and collect structured requirements.

Inserts an optional clarification phase between project analysis and goal decomposition,
ensuring the GoalDecomposer receives well-defined requirements instead of vague goals.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path

from .auto_model import (
    GatheringRound,
    RequirementQuestion,
    RequirementSpec,
)
from .claude_cli import BudgetTracker, run_claude_task
from .config import ClaudeConfig, LimitsConfig, RequirementConfig
from .model import TaskNode, TaskStatus

logger = logging.getLogger(__name__)


# ── 关键词密度检查 ──

# 常见英文停用词（覆盖 the/a/is/in/on 等，兼顾目标描述中常见的虚词）
_ENGLISH_STOPWORDS: frozenset[str] = frozenset({
    "a", "an", "the", "and", "or", "but", "not", "no", "nor",
    "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "having", "do", "does", "did", "doing",
    "will", "would", "shall", "should", "may", "might", "must", "can", "could",
    "i", "me", "my", "myself", "we", "our", "ours", "ourselves",
    "you", "your", "yours", "yourself", "yourselves",
    "he", "him", "his", "himself", "she", "her", "hers", "herself",
    "it", "its", "itself", "they", "them", "their", "theirs", "themselves",
    "what", "which", "who", "whom", "this", "that", "these", "those",
    "am", "if", "then", "else", "when", "where", "why", "how",
    "all", "each", "every", "both", "few", "more", "most", "other",
    "some", "such", "only", "own", "same", "so", "than", "too", "very",
    "just", "because", "as", "until", "while", "of", "at", "by", "for",
    "with", "about", "against", "between", "through", "during", "before",
    "after", "above", "below", "to", "from", "up", "down", "in", "out",
    "on", "off", "over", "under", "again", "further",
    # 目标描述中常见的低信息量词
    "also", "get", "got", "go", "going", "gone", "make", "made",
    "much", "many", "well", "really", "still", "already", "even",
    "need", "needs", "want", "wants", "like", "likes",
    "thing", "things", "stuff", "something", "anything", "everything", "nothing",
    "please", "thanks", "ok", "okay", "yes", "yeah", "sure",
    # 中文常见停用词（拼音形式不会出现，这里用 Unicode 直接匹配）
})

# 中文停用词（常见虚词和低信息量词）
_CHINESE_STOPWORDS: frozenset[str] = frozenset({
    "的", "了", "在", "是", "我", "有", "和", "就", "不", "人", "都",
    "一", "一个", "上", "也", "很", "到", "说", "要", "去", "你", "会",
    "着", "没有", "看", "好", "自己", "这", "他", "她", "它", "们",
    "把", "被", "让", "给", "从", "向", "对", "与", "为", "以",
    "及", "等", "吗", "吧", "呢", "啊", "哦", "嗯", "么", "那",
    "什么", "怎么", "如何", "哪", "哪些", "为什么", "这个", "那个",
    "可以", "可能", "应该", "需要", "必须", "能够",
    "一些", "这些", "那些", "每个", "所有", "其他", "另外",
    "还", "又", "再", "才", "已经", "正在", "将", "将要",
    "而", "但", "但是", "然而", "虽然", "不过", "如果", "假如",
    "因为", "所以", "因此", "于是", "然后", "接着",
})

# 用于分词的正则：匹配英文单词或单个中文字符（中文按单字切分以计算密度）
_WORD_RE = re.compile(r"[a-zA-Z][a-zA-Z']*[a-zA-Z]|[a-zA-Z]|[一-龥]")


def compute_keyword_density(text: str) -> tuple[float, int, int]:
    """计算目标文本的关键词（实词）密度。

    Returns:
        (density_score, content_word_count, total_word_count)
        density_score 范围 [0.0, 1.0]
    """
    # 提取英文单词和中文词组
    tokens = _WORD_RE.findall(text.lower())
    if not tokens:
        return 0.0, 0, 0

    # 过滤停用词：英文精确匹配，中文精确匹配
    content_words: list[str] = []
    for token in tokens:
        if token in _ENGLISH_STOPWORDS or token in _CHINESE_STOPWORDS:
            continue
        # 过滤掉过短的纯英文 token（单字母通常是噪音）
        if len(token) == 1 and token.isalpha():
            continue
        content_words.append(token)

    total = len(tokens)
    content = len(content_words)
    density = content / total if total > 0 else 0.0
    return density, content, total


# ── Goal 可操作性评估 ──

# 英文常见动词词根（涵盖 add/remove/refactor/fix/implement 等技术场景）
_ENGLISH_VERB_STEMS: frozenset[str] = frozenset({
    # 变更类
    "add", "remov", "delet", "creat", "insert", "updat", "modif", "chang", "replac", "renam",
    "mov", "copi", "merg", "split", "sort", "filter", "group", "reshap",
    # 构建/部署类
    "build", "deploy", "releas", "publish", "install", "configur", "setup", "init",
    "compil", "bundl", "packag", "generat", "export", "import",
    # 重构类
    "refactor", "rewrit", "restructur", "reorganiz", "simplif", "optimiz", "clean",
    "dedup", "extract", "encapsul", "abstract", "generaliz", "decoupl",
    # 修复类
    "fix", "repair", "patch", "resolv", "debug", "troubleshoot", "mitigat", "handl",
    "catch", "fallback", "recov", "rollback",
    # 实现/设计类
    "implement", "develop", "design", "architect", "integrat", "connect", "adapt",
    "extend", "enhanc", "upgrad", "migrat", "port", "convert", "transform",
    # 测试类
    "test", "verif", "valid", "assert", "mock", "stub", "benchmark", "profil",
    # 分析/审查类
    "analyz", "review", "audit", "inspec", "evaluat", "assess", "scan", "detect",
    "monitor", "measur", "track", "log", "trac",
    # 文档类
    "document", "describ", "explain", "clarif", "annotat", "comment",
    # 通用动作
    "enabl", "disabl", "support", "provid", "ensur", "enforc", "prevent", "avoid",
    "allow", "restrict", "limit", "control", "manag", "schedul", "coordinat",
    "automat", "orchestrat", "paralleliz", "serializ", "async", "wait", "notify",
    "retry", "cancel", "abort", "timeout", "throttl", "rat",
    "render", "display", "show", "hide", "animat", "layout", "style",
    "fetch", "request", "respons", "cach", "store", "persist", "load", "save",
})

# 中文动词字符（单字动词，覆盖技术文档常见动作）
_CHINESE_VERB_CHARS: frozenset[str] = frozenset({
    "添加", "删除", "修改", "更新", "创建", "移除", "替换", "重命名",
    "修复", "解决", "处理", "优化", "重构", "实现", "完成", "开发",
    "设计", "构建", "部署", "发布", "安装", "配置", "初始化", "启动",
    "测试", "验证", "检查", "审查", "分析", "评估", "监控", "追踪",
    "集成", "迁移", "升级", "合并", "拆分", "封装", "抽象", "解耦",
    "简化", "清理", "导出", "导入", "生成", "编译", "打包", "运行",
    "禁用", "启用", "支持", "确保", "防止", "允许", "限制", "管理",
    "调度", "编排", "并行", "重试", "取消", "超时", "限流",
    "渲染", "展示", "隐藏", "请求", "缓存", "持久", "加载", "保存",
    "连接", "断开", "发送", "接收", "通知", "订阅", "推送",
    "提取", "过滤", "排序", "分组", "统计", "计算",
    "编写", "编写", "记录", "注释", "说明", "描述",
})

# 技术领域关键词（出现这些词说明目标有具体技术指向）
_TECH_INDICATORS: frozenset[str] = frozenset({
    # 代码/文件
    "api", "cli", "gui", "ui", "ux", "sdk", "lib", "module", "class", "function",
    "method", "interface", "component", "service", "handler", "middleware", "plugin",
    "pipeline", "workflow", "task", "thread", "process", "socket", "queue",
    # 数据/存储
    "database", "sql", "redis", "cache", "orm", "schema", "migration", "model",
    "table", "index", "query", "crud", "repository", "store", "state",
    # 网络/协议
    "http", "rest", "grpc", "websocket", "rpc", "tcp", "udp", "dns",
    "auth", "jwt", "oauth", "token", "session", "cookie", "cors", "ssl",
    # 框架/工具
    "react", "vue", "angular", "svelte", "next", "nuxt",
    "django", "flask", "fastapi", "express", "koa", "spring",
    "docker", "k8s", "kubernetes", "nginx", "ci", "cd", "jenkins", "github",
    "webpack", "vite", "rollup", "esbuild", "babel", "typescript", "javascript",
    "python", "rust", "golang", "java", "kotlin", "swift",
    "pytest", "jest", "mocha", "cypress", "playwright",
    # 质量属性
    "performance", "latency", "throughput", "memory", "cpu", "concurrency",
    "scalability", "availability", "reliability", "security", "encryption",
    "logging", "monitoring", "alerting", "metrics", "tracing",
    "coverage", "lint", "format", "typecheck",
})


def assess_goal_operability(goal: str) -> tuple[float, str]:
    """评估 goal 的可操作性：是否包含动词词根和具体目标描述。

    通过三维度评分：
    1. 动作性（verb_score）：goal 是否包含明确的动作动词
    2. 具体性（specificity_score）：goal 是否包含具体的技术/领域关键词
    3. 结构性（structure_score）：goal 是否有明确的结构（列表、条件、范围等）

    Returns:
        (score, reason) — score 范围 [0.0, 1.0]
        score < 0.3 表示 goal 过于模糊，缺少可操作的动词或目标
    """
    text = goal.strip()
    if not text:
        return 0.0, "目标为空"

    text_lower = text.lower()

    # ── 维度 1：动作性（verb_score）──
    # 检查英文动词词根
    english_tokens = re.findall(r"[a-zA-Z][a-zA-Z']*", text_lower)
    verb_match_count = 0
    matched_verbs: list[str] = []
    for token in english_tokens:
        for stem in _ENGLISH_VERB_STEMS:
            if token.startswith(stem) or token == stem:
                verb_match_count += 1
                matched_verbs.append(token)
                break

    # 检查中文动词
    chinese_verb_count = 0
    matched_cn_verbs: list[str] = []
    for cv in _CHINESE_VERB_CHARS:
        if cv in text:
            chinese_verb_count += 1
            matched_cn_verbs.append(cv)

    total_verb_signals = verb_match_count + chinese_verb_count
    # 至少 1 个动词得 0.5，2 个得 0.75，3+ 个得 1.0
    if total_verb_signals == 0:
        verb_score = 0.0
    elif total_verb_signals == 1:
        verb_score = 0.5
    elif total_verb_signals == 2:
        verb_score = 0.75
    else:
        verb_score = 1.0

    # ── 维度 2：具体性（specificity_score）──
    tech_match_count = 0
    for indicator in _TECH_INDICATORS:
        if indicator in text_lower:
            tech_match_count += 1

    # 1 个技术词得 0.4，2 个得 0.7，3+ 得 1.0
    if tech_match_count == 0:
        specificity_score = 0.0
    elif tech_match_count == 1:
        specificity_score = 0.4
    elif tech_match_count == 2:
        specificity_score = 0.7
    else:
        specificity_score = 1.0

    # ── 维度 3：结构性（structure_score）──
    structure_signals = 0
    # 包含列表标记（1. 2. 或 - 或 • ）
    if re.search(r"(?:^|\n)\s*(?:\d+[.)]\s|[-*•]\s)", text):
        structure_signals += 1
    # 包含条件/范围描述（使用括号、冒号分隔说明）
    if re.search(r"[:：]", text) and len(text) > 30:
        structure_signals += 1
    # 包含验收标准关键词
    acceptance_patterns = r"(?:验收|标准|criteria|acceptance|should|must|shall|需要|确保|保证)"
    if re.search(acceptance_patterns, text_lower):
        structure_signals += 1

    # 0 个信号 0.0，1 个 0.5，2+ 个 1.0
    structure_score = 0.0 if structure_signals == 0 else (0.5 if structure_signals == 1 else 1.0)

    # ── 综合评分 ──
    # 权重：动作性 50%，具体性 30%，结构性 20%
    score = verb_score * 0.5 + specificity_score * 0.3 + structure_score * 0.2

    # 构建原因说明
    reasons: list[str] = []
    if verb_score < 0.5:
        reasons.append(f"缺少动作动词(verb_score={verb_score:.1f})")
    if specificity_score < 0.4:
        reasons.append(f"缺少具体技术描述(specificity={specificity_score:.1f})")
    if structure_score < 0.5:
        reasons.append(f"目标结构不清晰(structure={structure_score:.1f})")

    if reasons:
        reason = f"operability={score:.2f}，{'; '.join(reasons)}"
    else:
        reason = f"operability={score:.2f}，目标可操作性良好"

    # 调试日志
    logger.debug(
        "Goal 可操作性评估: verb=%.2f(%s+%s动词), specificity=%.2f(%d技术词), "
        "structure=%.2f(%d信号) → total=%.2f",
        verb_score, verb_match_count, chinese_verb_count,
        specificity_score, tech_match_count,
        structure_score, structure_signals,
        score,
    )

    return score, reason


# ── JSON 解析工具 ──

def _parse_json_robust(text: str, fallback: dict | list | None = None):
    """4 层回退 JSON 解析：直接 → ```json 块 → 正则提取 → fallback。"""
    # 1. 直接解析
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        pass

    # 2. 提取 ```json ... ``` 块
    m = re.search(r"```json\s*([\s\S]*?)```", text)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    # 3. 正则提取第一个 {} 或 []
    for pattern in [r"\{[\s\S]*\}", r"\[[\s\S]*\]"]:
        m = re.search(pattern, text)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                pass

    # 4. fallback
    logger.warning("JSON 解析全部失败，使用 fallback 值")
    return fallback if fallback is not None else {}


class RequirementGatherer:
    """需求收集器：评估目标充分性，通过多轮问答收集结构化需求。"""

    def __init__(
        self,
        claude_config: ClaudeConfig,
        limits_config: LimitsConfig,
        requirement_config: RequirementConfig,
        budget_tracker: BudgetTracker,
        working_dir: str,
        gather_mode: str = "interactive",
        gather_file: str | None = None,
        max_rounds: int | None = None,
    ):
        self._claude_config = claude_config
        self._limits_config = limits_config
        self._req_config = requirement_config
        self._budget = budget_tracker
        self._working_dir = working_dir
        self._gather_mode = gather_mode
        self._gather_file = gather_file
        self._max_rounds = max_rounds if max_rounds is not None else requirement_config.max_rounds
        self._project_context = ""  # 由 gather() 设置，供 _collect_auto 使用

    # ──────────────────────────────────────────────
    # 主流程
    # ──────────────────────────────────────────────

    def gather(self, goal: str, project_context: str) -> RequirementSpec:
        """完整流程：评估充分性 → 生成问题 → 收集回答 → 合成规格。"""
        self._project_context = project_context  # 缓存供 _collect_auto 使用
        logger.info("开始需求收集，模式=%s，最大轮次=%d", self._gather_mode, self._max_rounds)

        # 1. 评估充分性
        score, verdict = self.assess_sufficiency(goal, project_context)
        logger.info("充分性评估: score=%.2f, verdict=%s", score, verdict)

        # 2. 足够具体 → 直接返回 minimal spec
        if verdict == "sufficient":
            return RequirementSpec(
                original_goal=goal,
                sufficiency_score=score,
                sufficiency_verdict=verdict,
            )

        # 3. 不够具体 → 多轮收集
        rounds: list[GatheringRound] = []
        total_asked = 0
        total_answered = 0

        for round_num in range(1, self._max_rounds + 1):
            logger.info("需求收集第 %d/%d 轮", round_num, self._max_rounds)

            questions = self.generate_questions(goal, project_context, rounds)
            if not questions:
                logger.info("无更多问题需要澄清，结束收集")
                break

            answered = self.collect_answers(questions)

            # 统计
            round_asked = len(answered)
            round_answered = sum(1 for q in answered if q.answered)
            total_asked += round_asked
            total_answered += round_answered

            raw_answers = {q.question_id: q.answer for q in answered if q.answered}
            round_record = GatheringRound(
                round_number=round_num,
                questions=answered,
                raw_answers=raw_answers,
                timestamp=datetime.now(),
            )
            rounds.append(round_record)

            logger.info(
                "第 %d 轮完成: %d 问 %d 答",
                round_num, round_asked, round_answered,
            )

            # 如果用户没回答任何问题，提前结束
            if round_answered == 0:
                logger.info("本轮无有效回答，结束收集")
                break

        # 4. 合成需求规格
        spec = self.synthesize_spec(goal, project_context, rounds)
        spec.sufficiency_score = score
        spec.sufficiency_verdict = verdict
        spec.total_questions_asked = total_asked
        spec.total_questions_answered = total_answered
        return spec

    # ──────────────────────────────────────────────
    # 充分性评估
    # ──────────────────────────────────────────────

    def assess_sufficiency(self, goal: str, project_context: str) -> tuple[float, str]:
        """调用 Claude 评估目标充分性，返回 (score, verdict)。"""
        prompt = f"""你是一个需求分析专家。请评估以下目标描述的充分性，判断是否需要进一步澄清。

## 目标
{goal}

## 项目上下文
{project_context[:3000] if project_context else '（无项目上下文）'}

## 评估维度（每个维度 0-1 分）
1. **验收标准** (acceptance): 目标是否明确了"做到什么程度算完成"？
2. **技术约束** (technical): 是否明确了技术选型、兼容性、性能要求？
3. **范围边界** (scope): 是否明确了做什么、不做什么？
4. **优先级** (priority): 如果有多个子目标，优先级是否清晰？
5. **非功能需求** (non_functional): 安全性、可维护性、可测试性等是否提及？

## 输出格式（严格 JSON）
```json
{{
    "score": 0.65,
    "verdict": "needs_gathering",
    "dimension_scores": {{
        "acceptance": 0.5,
        "technical": 0.8,
        "scope": 0.6,
        "priority": 0.7,
        "non_functional": 0.3
    }},
    "missing_aspects": ["缺少验收标准", "未明确非功能需求"]
}}
```

verdict 取值：
- "sufficient": score >= {self._req_config.sufficiency_threshold}，目标足够具体
- "needs_gathering": score < {self._req_config.sufficiency_threshold}，需要进一步收集
- "ambiguous": score < 0.3，目标非常模糊

请只输出 JSON，不要输出其他内容。"""

        task = TaskNode(
            id="_assess_sufficiency",
            prompt_template=prompt,
            timeout=120,
            model=self._req_config.assessment_model,
            output_format="text",
        )

        result = run_claude_task(
            task=task,
            prompt=prompt,
            claude_config=self._claude_config,
            limits=self._limits_config,
            budget_tracker=self._budget,
            working_dir=self._working_dir,
        )

        if result.status != TaskStatus.SUCCESS or not result.output:
            logger.warning("充分性评估调用失败: %s，默认需要收集", result.error)
            return 0.5, "needs_gathering"

        data = _parse_json_robust(result.output, {"score": 0.5, "verdict": "needs_gathering"})
        score = float(data.get("score", 0.5))
        verdict = data.get("verdict", "needs_gathering")

        # 校验 verdict 合法性
        if verdict not in ("sufficient", "needs_gathering", "ambiguous"):
            verdict = "sufficient" if score >= self._req_config.sufficiency_threshold else "needs_gathering"

        # 关键词密度校准：实词占比过低时降低评分
        density, content_cnt, total_cnt = compute_keyword_density(goal)
        logger.info(
            "关键词密度: density=%.2f, content_words=%d, total_words=%d",
            density, content_cnt, total_cnt,
        )
        if density < 0.3:
            # 密度低于 30% 时按比例降分：score *= density / 0.3
            penalty_factor = density / 0.3
            original_score = score
            score = score * penalty_factor
            logger.warning(
                "关键词密度过低(%.2f < 0.3)，评分从 %.2f 降为 %.2f",
                density, original_score, score,
            )
            # 降分后重新判定 verdict
            if verdict not in ("sufficient", "needs_gathering", "ambiguous"):
                verdict = "sufficient" if score >= self._req_config.sufficiency_threshold else "needs_gathering"

        return score, verdict

    # ──────────────────────────────────────────────
    # 问题生成
    # ──────────────────────────────────────────────

    def generate_questions(
        self,
        goal: str,
        project_context: str,
        previous_rounds: list[GatheringRound],
    ) -> list[RequirementQuestion]:
        """基于项目上下文生成结构化问题。"""
        # 构建前序轮次摘要
        prev_summary = ""
        if previous_rounds:
            parts = []
            for r in previous_rounds:
                for q in r.questions:
                    if q.answered:
                        parts.append(f"- Q: {q.question_text}\n  A: {q.answer}")
            prev_summary = "\n".join(parts)

        max_q = self._req_config.max_questions_per_round

        prompt = f"""你是一个需求分析专家。请基于以下信息，生成 5-{max_q} 个结构化问题来澄清目标需求。

## 目标
{goal}

## 项目上下文
{project_context[:3000] if project_context else '（无项目上下文）'}

{f'## 前序轮次问答{chr(10)}{prev_summary}' if prev_summary else ''}

## 生成要求
1. 优先生成选择题（single_choice / multi_choice / yes_no），减少用户输入负担
2. 基于项目上下文生成具体的选项（如检测到 React 项目，选项应包含 React 相关技术）
3. 不要重复前序轮次已经问过的问题
4. 问题分类覆盖：scope（范围）、tech（技术）、acceptance（验收）、constraint（约束）、priority（优先级）
5. 每个问题附带 context_hint，说明为什么要问这个问题

## 输出格式（严格 JSON 数组）
```json
[
    {{
        "question_id": "q_01",
        "category": "scope",
        "question_text": "这个功能是否需要支持移动端？",
        "question_type": "yes_no",
        "options": ["是", "否"],
        "default": "否",
        "context_hint": "项目中检测到响应式 CSS，可能已有移动端支持"
    }},
    {{
        "question_id": "q_02",
        "category": "tech",
        "question_text": "数据存储方案选择？",
        "question_type": "single_choice",
        "options": ["使用现有 MySQL 数据库", "新建 Redis 缓存", "文件存储"],
        "default": "使用现有 MySQL 数据库",
        "context_hint": "项目已配置 MySQL 连接池"
    }}
]
```

请只输出 JSON 数组，不要输出其他内容。"""

        task = TaskNode(
            id="_generate_questions",
            prompt_template=prompt,
            timeout=180,
            model=self._req_config.question_gen_model,
            output_format="text",
        )

        result = run_claude_task(
            task=task,
            prompt=prompt,
            claude_config=self._claude_config,
            limits=self._limits_config,
            budget_tracker=self._budget,
            working_dir=self._working_dir,
        )

        if result.status != TaskStatus.SUCCESS or not result.output:
            logger.warning("问题生成调用失败: %s", result.error)
            return []

        data = _parse_json_robust(result.output, [])
        if not isinstance(data, list):
            logger.warning("问题生成返回非数组: %s", type(data))
            return []

        questions = []
        for i, item in enumerate(data[:max_q]):
            try:
                q = RequirementQuestion(
                    question_id=item.get("question_id", f"q_{i+1:02d}"),
                    category=item.get("category", "scope"),
                    question_text=item.get("question_text", ""),
                    question_type=item.get("question_type", "text"),
                    options=item.get("options", []),
                    default=item.get("default", ""),
                    context_hint=item.get("context_hint", ""),
                )
                if q.question_text:
                    questions.append(q)
            except Exception as e:
                logger.warning("解析问题 #%d 失败: %s", i, e)

        return questions

    # ──────────────────────────────────────────────
    # 回答收集
    # ──────────────────────────────────────────────

    def collect_answers(self, questions: list[RequirementQuestion]) -> list[RequirementQuestion]:
        """根据 gather_mode 收集回答。"""
        if self._gather_mode == "interactive":
            return self._collect_interactive(questions)
        elif self._gather_mode == "file":
            return self._collect_from_file(questions)
        elif self._gather_mode == "auto":
            return self._collect_auto(questions)
        else:
            logger.warning("未知收集模式: %s，降级为 auto", self._gather_mode)
            return self._collect_auto(questions)

    def _collect_interactive(self, questions: list[RequirementQuestion]) -> list[RequirementQuestion]:
        """CLI 交互模式：逐题显示，用户输入回答。"""
        print("\n" + "=" * 50)
        print("  需求澄清（输入回答，直接回车使用默认值，输入 'skip' 跳过）")
        print("=" * 50 + "\n")

        for q in questions:
            # 显示问题
            print(f"[{q.category}] {q.question_text}")
            if q.context_hint:
                print(f"  (提示: {q.context_hint})")

            if q.question_type in ("single_choice", "multi_choice", "yes_no"):
                # 显示选项
                for i, opt in enumerate(q.options, 1):
                    default_mark = " (默认)" if opt == q.default else ""
                    print(f"  {i}. {opt}{default_mark}")

                if q.question_type == "multi_choice":
                    print("  (多选请用逗号分隔编号，如: 1,3)")

            if q.default:
                prompt_text = f"  回答 [{q.default}]: "
            else:
                prompt_text = "  回答: "

            try:
                raw = input(prompt_text).strip()
            except (EOFError, KeyboardInterrupt):
                print("\n用户中断输入")
                break

            if raw.lower() == "skip":
                continue

            if not raw and q.default:
                raw = q.default

            if not raw:
                continue

            # 解析选项编号
            if q.question_type in ("single_choice", "yes_no") and q.options:
                try:
                    idx = int(raw) - 1
                    if 0 <= idx < len(q.options):
                        raw = q.options[idx]
                except ValueError:
                    pass  # 用户直接输入了文本
            elif q.question_type == "multi_choice" and q.options:
                try:
                    indices = [int(x.strip()) - 1 for x in raw.split(",")]
                    selected = [q.options[i] for i in indices if 0 <= i < len(q.options)]
                    if selected:
                        raw = ", ".join(selected)
                except ValueError:
                    pass

            q.answer = raw
            q.answered = True
            print()

        return questions

    def _collect_from_file(self, questions: list[RequirementQuestion]) -> list[RequirementQuestion]:
        """从 JSON 文件读取预填答案。"""
        if not self._gather_file:
            logger.warning("file 模式但未指定 --gather-file，跳过收集")
            return questions

        file_path = Path(self._gather_file)
        if not file_path.exists():
            logger.warning("答案文件不存在: %s", file_path)
            return questions

        try:
            answers = json.loads(file_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("读取答案文件失败: %s", e)
            return questions

        for q in questions:
            if q.question_id in answers:
                q.answer = str(answers[q.question_id])
                q.answered = True
            elif q.default:
                q.answer = q.default
                q.answered = True

        answered_count = sum(1 for q in questions if q.answered)
        logger.info("从文件读取了 %d/%d 个回答", answered_count, len(questions))
        return questions

    def _collect_auto(self, questions: list[RequirementQuestion]) -> list[RequirementQuestion]:
        """AI 自动回答模式：调用 Claude 基于项目上下文回答。"""
        project_ctx = self._project_context or ''
        questions_text = "\n".join(
            f"- [{q.question_id}] ({q.question_type}) {q.question_text}"
            + (f"\n  选项: {', '.join(q.options)}" if q.options else "")
            + (f"\n  默认: {q.default}" if q.default else "")
            + (f"\n  提示: {q.context_hint}" if q.context_hint else "")
            for q in questions
        )

        prompt = f"""你是一个资深软件工程师，正在帮助澄清项目需求。
请基于项目上下文，为以下问题选择最合理的答案。

## 项目上下文
{project_ctx[:3000] if project_ctx else '（无项目上下文）'}

## 问题列表
{questions_text}

## 输出格式（严格 JSON）
```json
{{
    "q_01": "选择的答案或文本回答",
    "q_02": "选择的答案或文本回答"
}}
```

要求：
1. 选择题请直接填写选项文本（不是编号）
2. 文本题请简洁回答（不超过 100 字）
3. 如果无法判断，使用默认值
4. 请只输出 JSON，不要输出其他内容。"""

        task = TaskNode(
            id="_auto_answer",
            prompt_template=prompt,
            timeout=120,
            model=self._req_config.assessment_model,
            output_format="text",
        )

        result = run_claude_task(
            task=task,
            prompt=prompt,
            claude_config=self._claude_config,
            limits=self._limits_config,
            budget_tracker=self._budget,
            working_dir=self._working_dir,
        )

        if result.status != TaskStatus.SUCCESS or not result.output:
            logger.warning("自动回答调用失败: %s，使用默认值", result.error)
            # fallback: 使用默认值
            for q in questions:
                if q.default:
                    q.answer = q.default
                    q.answered = True
            return questions

        answers = _parse_json_robust(result.output, {})
        if not isinstance(answers, dict):
            answers = {}

        for q in questions:
            if q.question_id in answers:
                q.answer = str(answers[q.question_id])
                q.answered = True
            elif q.default:
                q.answer = q.default
                q.answered = True

        return questions

    # ──────────────────────────────────────────────
    # 需求合成
    # ──────────────────────────────────────────────

    def synthesize_spec(
        self,
        goal: str,
        project_context: str,
        rounds: list[GatheringRound],
    ) -> RequirementSpec:
        """将多轮问答合成为 RequirementSpec。"""
        # 构建问答摘要
        qa_text = ""
        for r in rounds:
            qa_text += f"\n### 第 {r.round_number} 轮\n"
            for q in r.questions:
                if q.answered:
                    qa_text += f"- Q: {q.question_text}\n  A: {q.answer}\n"

        prompt = f"""你是一个需求分析专家。请将以下目标和多轮问答合成为结构化的需求规格。

## 原始目标
{goal}

## 项目上下文
{project_context[:2000] if project_context else '（无）'}

## 多轮问答记录
{qa_text if qa_text else '（无问答记录）'}

## 输出格式（严格 JSON）
```json
{{
    "scope": "功能范围的简洁描述（1-3 句话）",
    "acceptance_criteria": [
        "验收标准 1",
        "验收标准 2"
    ],
    "technical_constraints": [
        "技术约束 1"
    ],
    "non_functional_requirements": [
        "非功能需求 1"
    ],
    "priority_order": [
        "最高优先级的子目标",
        "次高优先级的子目标"
    ],
    "excluded_scope": [
        "明确不做的事情"
    ]
}}
```

要求：
1. 基于问答记录提取具体的需求，不要编造用户没提到的内容
2. 验收标准要可验证（可以写成测试用例的形式）
3. 如果问答中没有涉及某个字段，留空数组即可
4. 请只输出 JSON，不要输出其他内容。"""

        task = TaskNode(
            id="_synthesize_spec",
            prompt_template=prompt,
            timeout=120,
            model=self._req_config.synthesis_model,
            output_format="text",
        )

        result = run_claude_task(
            task=task,
            prompt=prompt,
            claude_config=self._claude_config,
            limits=self._limits_config,
            budget_tracker=self._budget,
            working_dir=self._working_dir,
        )

        if result.status != TaskStatus.SUCCESS or not result.output:
            logger.warning("需求合成调用失败: %s，返回基础 spec", result.error)
            return RequirementSpec(original_goal=goal, rounds=rounds)

        data = _parse_json_robust(result.output, {})
        if not isinstance(data, dict):
            data = {}

        return RequirementSpec(
            original_goal=goal,
            scope=data.get("scope", ""),
            acceptance_criteria=data.get("acceptance_criteria", []),
            technical_constraints=data.get("technical_constraints", []),
            non_functional_requirements=data.get("non_functional_requirements", []),
            priority_order=data.get("priority_order", []),
            excluded_scope=data.get("excluded_scope", []),
            rounds=rounds,
        )
