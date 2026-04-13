"""
Guardrail 模块 - 输入输出安全检查

提供 InputGuardrail 和 OutputGuardrail 类，用于检测敏感信息、注入攻击、长度限制等安全问题。
"""

import re
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class GuardrailResult:
    """Guardrail 检查结果"""
    passed: bool
    violations: list[str] = field(default_factory=list)
    
    def __bool__(self) -> bool:
        """支持布尔判断：if result: ..."""
        return self.passed


class InputGuardrail:
    """
    输入 Guardrail - 检查 prompt 安全性
    
    检查规则：
    1. 敏感信息模式（API key、password、token 等）
    2. prompt 长度上限
    3. 可疑指令注入模式
    """
    
    # 敏感信息正则模式
    SENSITIVE_PATTERNS = [
        (r'(?i)(api[_-]?key|apikey)\s*[:=]\s*["\']?([a-zA-Z0-9_\-]{20,})', "API Key"),
        (r'(?i)(password|passwd|pwd)\s*[:=]\s*["\']?([^\s"\']{6,})', "Password"),
        (r'(?i)(token|auth[_-]?token)\s*[:=]\s*["\']?([a-zA-Z0-9_\-\.]{20,})', "Token"),
        (r'(?i)(secret[_-]?key|secret)\s*[:=]\s*["\']?([a-zA-Z0-9_\-]{20,})', "Secret Key"),
        (r'(?i)(access[_-]?key[_-]?id)\s*[:=]\s*["\']?([A-Z0-9]{16,})', "Access Key ID"),
        (r'(?i)(private[_-]?key)\s*[:=]\s*["\']?(-----BEGIN.*?-----)', "Private Key"),
        (r'sk-[a-zA-Z0-9]{20,}', "OpenAI API Key"),  # OpenAI 格式
        (r'ghp_[a-zA-Z0-9]{36,}', "GitHub Personal Access Token"),  # GitHub PAT
        (r'gho_[a-zA-Z0-9]{36,}', "GitHub OAuth Token"),
    ]
    
    # 可疑指令注入模式
    INJECTION_PATTERNS = [
        (r'(?i)ignore\s+(previous|all|above)\s+(instructions?|prompts?|rules?)', "指令覆盖攻击"),
        (r'(?i)(disregard|forget)\s+(previous|all|above)', "指令遗忘攻击"),
        (r'(?i)you\s+are\s+now\s+(a|an)\s+\w+', "角色劫持攻击"),
        (r'(?i)system\s*:\s*you\s+(must|should|are)', "系统提示注入"),
        (r'(?i)new\s+instructions?\s*:', "新指令注入"),
        (r'(?i)override\s+(system|default|original)', "覆盖系统设置"),
        (r'(?i)jailbreak|DAN\s+mode', "越狱攻击"),
        (r'(?i)reveal\s+(your|the)\s+(prompt|instructions?|system)', "提示泄露攻击"),
    ]
    
    def __init__(
        self,
        max_length: int = 100_000,
        check_sensitive: bool = True,
        check_injection: bool = True,
    ):
        """
        初始化 InputGuardrail
        
        Args:
            max_length: prompt 最大长度（字符数）
            check_sensitive: 是否检查敏感信息
            check_injection: 是否检查注入攻击
        """
        self.max_length = max_length
        self.check_sensitive = check_sensitive
        self.check_injection = check_injection
    
    def check(self, prompt: str) -> GuardrailResult:
        """
        检查 prompt 安全性
        
        Args:
            prompt: 待检查的 prompt 文本
            
        Returns:
            GuardrailResult: 检查结果
        """
        violations = []
        
        # 1. 长度检查
        if len(prompt) > self.max_length:
            violations.append(
                f"Prompt 长度超限: {len(prompt)} > {self.max_length} 字符"
            )
        
        # 2. 敏感信息检查
        if self.check_sensitive:
            for pattern, name in self.SENSITIVE_PATTERNS:
                matches = re.findall(pattern, prompt)
                if matches:
                    violations.append(
                        f"检测到敏感信息 ({name}): 发现 {len(matches)} 处匹配"
                    )
        
        # 3. 注入攻击检查
        if self.check_injection:
            for pattern, attack_type in self.INJECTION_PATTERNS:
                if re.search(pattern, prompt):
                    violations.append(
                        f"检测到可疑指令注入: {attack_type}"
                    )
        
        return GuardrailResult(
            passed=len(violations) == 0,
            violations=violations
        )


class OutputGuardrail:
    """
    输出 Guardrail - 检查 Claude 输出安全性
    
    检查规则：
    1. 敏感信息泄露（API key、密码等）
    2. 输出长度异常
    3. 恶意代码模式（如 rm -rf、DROP TABLE 等）
    """
    
    # 敏感信息模式（复用 InputGuardrail 的模式）
    SENSITIVE_PATTERNS = InputGuardrail.SENSITIVE_PATTERNS
    
    # 恶意代码模式
    MALICIOUS_CODE_PATTERNS = [
        (r'(?i)\brm\s+-rf\s+/', "危险的文件删除命令"),
        (r'(?i)\bdrop\s+(table|database)\b', "危险的数据库删除命令"),
        (r'(?i)\bformat\s+[a-z]:', "危险的磁盘格式化命令"),
        (r'(?i)\bdel\s+/[fqs]\s+', "危险的批量删除命令"),
        (r'(?i):\(\)\{.*\|.*&\s*\}', "Fork 炸弹"),
        (r'(?i)eval\s*\(.*input\(', "危险的 eval 注入"),
        (r'(?i)exec\s*\(.*input\(', "危险的 exec 注入"),
        (r'(?i)__import__\s*\(\s*["\']os["\']', "可疑的动态导入"),
    ]
    
    def __init__(
        self,
        max_length: int = 500_000,
        check_sensitive: bool = True,
        check_malicious: bool = True,
    ):
        """
        初始化 OutputGuardrail
        
        Args:
            max_length: 输出最大长度（字符数）
            check_sensitive: 是否检查敏感信息泄露
            check_malicious: 是否检查恶意代码
        """
        self.max_length = max_length
        self.check_sensitive = check_sensitive
        self.check_malicious = check_malicious
    
    def check(self, output: str) -> GuardrailResult:
        """
        检查输出安全性
        
        Args:
            output: 待检查的输出文本
            
        Returns:
            GuardrailResult: 检查结果
        """
        violations = []
        
        # 1. 长度检查
        if len(output) > self.max_length:
            violations.append(
                f"输出长度异常: {len(output)} > {self.max_length} 字符"
            )
        
        # 2. 敏感信息泄露检查
        if self.check_sensitive:
            for pattern, name in self.SENSITIVE_PATTERNS:
                matches = re.findall(pattern, output)
                if matches:
                    violations.append(
                        f"输出包含敏感信息 ({name}): 发现 {len(matches)} 处匹配"
                    )
        
        # 3. 恶意代码检查
        if self.check_malicious:
            for pattern, danger_type in self.MALICIOUS_CODE_PATTERNS:
                if re.search(pattern, output):
                    violations.append(
                        f"输出包含危险代码: {danger_type}"
                    )
        
        return GuardrailResult(
            passed=len(violations) == 0,
            violations=violations
        )


# 便捷工厂函数
def create_default_input_guardrail() -> InputGuardrail:
    """创建默认配置的 InputGuardrail"""
    return InputGuardrail(
        max_length=100_000,
        check_sensitive=True,
        check_injection=True,
    )


def create_default_output_guardrail() -> OutputGuardrail:
    """创建默认配置的 OutputGuardrail"""
    return OutputGuardrail(
        max_length=500_000,
        check_sensitive=True,
        check_malicious=True,
    )
