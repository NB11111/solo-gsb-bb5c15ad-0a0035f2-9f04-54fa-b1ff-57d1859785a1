"""请求/响应模型。"""
from __future__ import annotations

import re

from pydantic import BaseModel, Field, field_validator, model_validator

from . import settings

_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")


def normalize_sha256(value: str) -> str:
    """规整 SHA-256：去空白、转小写、剥离可选 sha256: 前缀，返回 64 位 hex。

    整文件摘要字段与 X-Chunk-SHA256 请求头共用；格式非法抛 ValueError。
    """
    value = value.strip().lower()
    if value.startswith("sha256:"):
        value = value[len("sha256:"):]
    if not _SHA256_HEX.match(value):
        raise ValueError("sha256 must be 64 lowercase hex characters")
    return value


class CreateSessionRequest(BaseModel):
    filename: str = Field(..., description="原始文件名")
    total_size: int = Field(..., ge=0, description="文件总字节数")
    sha256: str = Field(..., description="整文件 SHA-256，64 位小写十六进制（可带 sha256: 前缀）")
    chunk_size: int = Field(
        default=0, ge=0, description="建议分片大小（字节），仅作提示；0 表示无建议"
    )

    @field_validator("filename")
    @classmethod
    def _validate_filename(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("filename must not be empty")
        if len(value) > settings.max_filename_length:
            raise ValueError(f"filename too long (max {settings.max_filename_length})")
        return value

    @field_validator("total_size")
    @classmethod
    def _validate_size(cls, value: int) -> int:
        if settings.max_upload_bytes and value > settings.max_upload_bytes:
            raise ValueError(
                f"total_size exceeds server limit {settings.max_upload_bytes} bytes"
            )
        return value

    @field_validator("sha256")
    @classmethod
    def _validate_sha256(cls, value: str) -> str:
        return normalize_sha256(value)


class MissingRange(BaseModel):
    start: int
    end: int


class SessionResponse(BaseModel):
    session_id: str
    filename: str
    total_size: int
    sha256_expected: str
    chunk_size: int
    status: str
    bytes_received: int
    complete: bool
    fully_received: bool
    missing_ranges: list[MissingRange]
    created_at: str
    expires_at: str
    last_access_at: str
    completed_at: str | None = None
    download_url: str | None = None


class ChunkResponse(BaseModel):
    session_id: str
    received: MissingRange
    bytes_received: int
    missing_ranges: list[MissingRange]
    fully_received: bool
    expires_at: str


class CopyChunkRequest(BaseModel):
    """服务端复制分片请求：从已完成来源会话的成品中复制一段。"""

    source_session_id: str = Field(
        ..., min_length=1, max_length=128, description="已完成的来源会话 ID"
    )
    source_start: int = Field(..., ge=0, description="来源起始偏移（含）")
    source_end: int = Field(..., ge=0, description="来源结束偏移（不含），半开区间")
    target_start: int = Field(
        ..., ge=0, description="目标会话中的起始偏移（含），长度沿用来源区间"
    )

    @model_validator(mode="after")
    def _validate_range(self) -> "CopyChunkRequest":
        if self.source_end <= self.source_start:
            raise ValueError("invalid source range: require source_start < source_end")
        return self


# ---------------------------------------------------------------------------
# 受控下载票据
# ---------------------------------------------------------------------------


class CreateTicketRequest(BaseModel):
    expires_in_minutes: int = Field(
        ..., ge=1, le=1440, description="票据有效期（分钟），1～1440"
    )
    max_downloads: int = Field(
        ..., ge=1, le=100, description="下载次数上限，1～100"
    )


class TicketInfo(BaseModel):
    """票据的非敏感元数据（撤销接口返回）。"""

    ticket_id: str
    session_id: str
    created_at: str
    expires_at: str
    max_downloads: int
    remaining_downloads: int
    revoked: bool
    download_url: str


class TicketResponse(TicketInfo):
    # 明文随机票据，仅在签发响应中返回这一次；服务端只存它的哈希。
    ticket: str = Field(..., description="凭票下载令牌（明文仅本次返回，请妥善保管）")


# ---------------------------------------------------------------------------
# 运维只读汇总
# ---------------------------------------------------------------------------


class StorageStatusSummary(BaseModel):
    """单个状态的汇总行（四种状态固定各一行，计数为零也保留）。"""

    status: str = Field(
        ..., description="会话状态：uploading / completed / aborted / expired"
    )
    session_count: int = Field(..., ge=0, description="该状态的会话数量")
    declared_bytes: int = Field(
        ..., ge=0, description="声明字节数：所有会话 total_size 之和"
    )
    stored_bytes: int = Field(
        ...,
        ge=0,
        description=(
            "实际占用的逻辑字节数：uploading 取 bytes_received 之和，"
            "completed 取 total_size 之和，aborted / expired 恒为 0"
        ),
    )


class StorageSummaryResponse(BaseModel):
    statuses: list[StorageStatusSummary] = Field(
        ...,
        description="按 uploading、completed、aborted、expired 固定顺序排列的四行",
    )
    total: StorageStatusSummary = Field(
        ..., description="四行求和的总计（status 字段为 total）"
    )
