# Simple Validation

`simple` 模式使用固定顺序的多级验证管线：

1. 执行退出状态
2. 目标文件命中
3. 目标文件内容变更
4. 非授权文件变更检查
5. 语法检查
6. pattern 检查
7. verify command 检查
8. copy-back 一致性检查

失败分类包括：

- `no_change`
- `wrong_file_changed`
- `syntax_error`
- `pattern_missing`
- `verify_command_failed`
- `timeout`
- `rate_limited`
- `auth_expired`
- `resource_exhausted`
- `copyback_conflict`
