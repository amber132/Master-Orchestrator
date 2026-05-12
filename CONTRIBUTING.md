# 参与贡献

感谢你对 Master Orchestrator 感兴趣。这份文档说明本项目的贡献方式、开发流程和提交规范。

## 开始之前

```bash
# Fork 后克隆你的仓库
git clone https://github.com/<your-username>/Master-Orchestrator.git
cd Master-Orchestrator

# 创建开发环境
python -m venv .venv
source .venv/bin/activate  # Windows 可使用 .venv\Scripts\activate
pip install -e ".[dev]"

# 运行测试，确认环境可用
python -m pytest -q
```

## 开发流程

1. 从 `main` 创建分支：

   ```bash
   git checkout -b feat/your-feature
   ```

2. 按现有代码风格完成改动。

3. 为改动补充或更新测试。

4. 运行测试：

   ```bash
   python -m pytest -q
   ```

5. 使用清晰的提交信息：

   ```bash
   git commit -m "feat: add support for custom validation rules"
   ```

6. Push 后提交 Pull Request。

## Commit 规范

本项目采用 [Conventional Commits](https://www.conventionalcommits.org/)：

- `feat:`：新增功能
- `fix:`：修复缺陷
- `docs:`：文档变更
- `refactor:`：代码重构
- `test:`：新增或更新测试
- `chore:`：维护性改动

## 代码风格

- 使用 Python 3.11+ 和类型标注
- 优先沿用仓库内已有模式
- 函数职责保持聚焦，模块边界保持清晰
- 公开 API 使用 Google 风格 docstring

## 反馈问题

- Bug 和功能建议请使用 GitHub Issues
- 请提供复现步骤、预期行为和实际行为
- 如果有日志或错误信息，请一并附上

## 当前欢迎贡献的方向

- **Provider 集成**：支持更多 AI 编码工具
- **中文文档**：教程、示例、翻译和实践经验
- **测试覆盖**：边界场景、集成测试和真实流程 smoke test
- **Simple 模式**：新的验证器、批量任务模式和执行策略

## 交流

可以在 GitHub Discussion 提问，也可以在已有 issue 下补充上下文。
