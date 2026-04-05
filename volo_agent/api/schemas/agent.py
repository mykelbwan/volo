from pydantic import BaseModel, ConfigDict, Field


class AgentTurnRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    message: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    username: str | None = None
    thread_id: str = Field(min_length=1)
    conversation_id: str | None = None
    selected_task_number: int | None = Field(default=None, ge=1)
    client_message_id: str | None = None
    client_nonce: str | None = None


class AgentTurnResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    assistant_message: str | None = None
    conversation_id: str = Field(min_length=1)
    thread_id: str = Field(min_length=1)
    selected_task_number: int | None = Field(default=None, ge=1)
    allocated_new_thread: bool = False
    blocked: bool = False
    blocked_message: str | None = None
