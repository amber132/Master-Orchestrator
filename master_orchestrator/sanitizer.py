"""Prompt sanitizer for cleaning and validating user inputs."""

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class SanitizeResult:
    """Prompt 清洗结果"""
    cleaned_text: str
    warnings: list[str] = field(default_factory=list)


class PromptSanitizer:
    """Prompt 清洗器，用于截断超长内容、转义危险模式、移除不可见字符"""
    
    # 危险模式定义
    DANGEROUS_PATTERNS = [
        (r'```{3,}', '```'),  # 连续反引号（3个以上）
        (r'<\|im_start\|>', '[SYSTEM_TOKEN]'),  # 系统指令注入
        (r'<\|im_end\|>', '[SYSTEM_TOKEN]'),
        (r'<\|endoftext\|>', '[SYSTEM_TOKEN]'),
        (r'###\s*Instruction:', '### User Instruction:'),  # 指令注入
        (r'###\s*System:', '### User System:'),
    ]
    
    # 不可见字符范围（保留常见空白符：空格、制表符、换行）
    INVISIBLE_CHARS = re.compile(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f-\x9f]')
    
    def sanitize(self, text: str, max_length: int = 50000) -> SanitizeResult:
        """
        清洗 prompt 文本
        
        Args:
            text: 原始文本
            max_length: 最大长度限制
            
        Returns:
            SanitizeResult: 包含清洗后的文本和警告信息
        """
        if not isinstance(text, str):
            return SanitizeResult(
                cleaned_text="",
                warnings=[f"输入类型错误：期望 str，实际 {type(text).__name__}"]
            )
        
        warnings = []
        cleaned = text
        
        # 1. 移除不可见字符
        invisible_count = len(self.INVISIBLE_CHARS.findall(cleaned))
        if invisible_count > 0:
            cleaned = self.INVISIBLE_CHARS.sub('', cleaned)
            warnings.append(f"移除了 {invisible_count} 个不可见字符")
        
        # 2. 转义危险模式
        for pattern, replacement in self.DANGEROUS_PATTERNS:
            matches = re.findall(pattern, cleaned)
            if matches:
                cleaned = re.sub(pattern, replacement, cleaned)
                warnings.append(f"转义了危险模式 '{pattern}'，共 {len(matches)} 处")
        
        # 3. 截断超长内容
        original_length = len(cleaned)
        if original_length > max_length:
            cleaned = cleaned[:max_length]
            truncated = original_length - max_length
            warnings.append(f"文本超长，截断了 {truncated} 个字符（原长度 {original_length}，限制 {max_length}）")
        
        # 4. strip 首尾空白
        stripped_length = len(cleaned)
        cleaned = cleaned.strip()
        if len(cleaned) < stripped_length:
            warnings.append(f"移除了首尾空白字符")
        
        return SanitizeResult(cleaned_text=cleaned, warnings=warnings)
