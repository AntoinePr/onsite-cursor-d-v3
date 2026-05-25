from pydantic import BaseModel, Field


class WSMessage(BaseModel):
    """Base WebSocket message envelope."""

    type: str
    payload: dict = Field(default_factory=dict)


class ToolCallRequest(BaseModel):
    tool_call_id: str
    function_name: str
    arguments: dict = Field(default_factory=dict)


class ToolCallResult(BaseModel):
    tool_call_id: str
    result: str
    error: str | None = None


class WorkerRegistration(BaseModel):
    worker_name: str
    capabilities: list[str] = Field(default_factory=list)
