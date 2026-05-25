import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase

SESSION_TTL = timedelta(hours=1)


def _now():
    return datetime.now(timezone.utc)


def _expires():
    return datetime.now(timezone.utc) + SESSION_TTL


class Base(DeclarativeBase):
    pass


class Session(Base):
    __tablename__ = "sessions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(50), default="New session")
    status = Column(String(20), default="active")  # active, disconnected, expired
    created_at = Column(DateTime(timezone=True), default=_now)
    last_active_at = Column(DateTime(timezone=True), default=_now)
    expires_at = Column(DateTime(timezone=True), default=_expires)


class Message(Base):
    __tablename__ = "messages"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    session_id = Column(
        UUID(as_uuid=True), ForeignKey("sessions.id", ondelete="CASCADE"), nullable=False
    )
    role = Column(String(20), nullable=False)  # user, assistant, tool
    content = Column(Text, nullable=True)
    tool_calls = Column(JSONB, nullable=True)
    tool_call_id = Column(String(100), nullable=True)
    created_at = Column(DateTime(timezone=True), default=_now)

    __table_args__ = (
        UniqueConstraint("session_id", "tool_call_id", name="uq_messages_session_tool_call"),
    )


class ToolCallDispatch(Base):
    __tablename__ = "tool_call_dispatch"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tool_call_id = Column(String(100), nullable=False, unique=True)
    session_id = Column(UUID(as_uuid=True), ForeignKey("sessions.id", ondelete="CASCADE"), nullable=False)
    worker_name = Column(String(100), nullable=False)
    status = Column(String(20), default="dispatched")
    dispatched_at = Column(DateTime(timezone=True), default=_now)
    acked_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    retry_count = Column(Integer, default=0)


class Worker(Base):
    __tablename__ = "workers"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(100), unique=True, nullable=False)
    status = Column(String(20), default="disconnected")
    capabilities = Column(JSONB, default=list)
    last_seen = Column(DateTime(timezone=True), default=_now)
