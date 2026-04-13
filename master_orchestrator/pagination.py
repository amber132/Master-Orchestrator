"""分页工具模块，用于处理大规模数据的分页。"""

from __future__ import annotations

import math
from typing import Iterator, TypeVar

from .config import PaginationConfig
from .exceptions import OrchestratorError

T = TypeVar('T')


class PaginationError(OrchestratorError):
    """分页操作失败时抛出。"""


class Paginator:
    """分页器，用于将大列表分页处理。

    示例：
        >>> items = list(range(100))
        >>> paginator = Paginator(items, page_size=10)
        >>> paginator.total_pages
        10
        >>> paginator.get_page(1)
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        >>> for page in paginator.iter_pages():
        ...     print(len(page))
        10
        10
        ...
    """

    def __init__(self, items: list, page_size: int = 50):
        """初始化分页器。

        Args:
            items: 要分页的列表
            page_size: 每页大小，必须 > 0

        Raises:
            PaginationError: 当 page_size <= 0 时
        """
        if page_size <= 0:
            raise PaginationError(
                f"page_size must be > 0, got {page_size}",
                context={"page_size": page_size}
            )

        self._items = items
        self._page_size = page_size

    @property
    def total_pages(self) -> int:
        """返回总页数。"""
        if not self._items:
            return 0
        return math.ceil(len(self._items) / self._page_size)

    def get_page(self, page: int) -> list:
        """获取指定页的数据（页码从 1 开始）。

        Args:
            page: 页码，从 1 开始

        Returns:
            指定页的数据列表

        Raises:
            PaginationError: 当页码 < 1 或超出范围时
        """
        if page < 1:
            raise PaginationError(
                f"page must be >= 1, got {page}",
                context={"page": page, "total_pages": self.total_pages}
            )

        if page > self.total_pages:
            raise PaginationError(
                f"page {page} exceeds total_pages {self.total_pages}",
                context={"page": page, "total_pages": self.total_pages}
            )

        # 空列表特殊处理
        if not self._items:
            return []

        start_idx = (page - 1) * self._page_size
        end_idx = start_idx + self._page_size
        return self._items[start_idx:end_idx]

    def iter_pages(self) -> Iterator[list]:
        """迭代所有页。

        Yields:
            每一页的数据列表
        """
        for page_num in range(1, self.total_pages + 1):
            yield self.get_page(page_num)


def paginate_results(items: list[T], config: PaginationConfig) -> list[list[T]]:
    """将大列表按配置分页，并限制总数量。

    Args:
        items: 要分页的列表
        config: 分页配置

    Returns:
        分页后的列表的列表，每个子列表最多包含 page_size 个元素，
        总元素数不超过 max_items

    示例：
        >>> from .config import PaginationConfig
        >>> config = PaginationConfig(page_size=10, max_items=25)
        >>> items = list(range(100))
        >>> pages = paginate_results(items, config)
        >>> len(pages)
        3
        >>> sum(len(p) for p in pages)
        25
    """
    # 先截断到 max_items
    truncated = items[:config.max_items]

    # 空列表直接返回
    if not truncated:
        return []

    # 使用 Paginator 分页
    paginator = Paginator(truncated, page_size=config.page_size)
    return list(paginator.iter_pages())
