# Simple Isolation

支持三种模式：

- `none`
  - 默认
  - 直接在源仓库执行
- `copy`
  - 在 `simple.copy_root_dir` 下创建镜像工作区
  - 成功后回写目标文件
- `worktree`
  - 显式启用
  - 在 Windows 路径预算不足或非 Git 仓库时自动降级到 `copy`

Windows 场景建议默认 `none` 或 `copy`。
