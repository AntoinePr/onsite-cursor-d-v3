import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, ForeignKey, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase


def _now():
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Usage(Base):
    __tablename__ = "usage"
    __table_args__ = (
        UniqueConstraint("event_id", "usage_type", name="uq_event_usage_type"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_id = Column(String(200), nullable=False, index=True)
    org_id = Column(Integer, nullable=False, index=True)
    session_id = Column(String(100), nullable=False, index=True)
    provider = Column(String(50), nullable=False)
    model = Column(String(100), nullable=False)
    event_type = Column(String(50), nullable=False)
    usage_type = Column(String(100), nullable=False)
    quantity = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), default=_now)


class Cost(Base):
    __tablename__ = "costs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    usage_id = Column(UUID(as_uuid=True), ForeignKey("usage.id", ondelete="CASCADE"), nullable=False)
    usage_type = Column(String(100), nullable=False)
    unit_cost = Column(Numeric(20, 12), nullable=False)
    total_cost = Column(Numeric(20, 12), nullable=False)
    created_at = Column(DateTime(timezone=True), default=_now)
