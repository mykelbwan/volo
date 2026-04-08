from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from intent_hub.ontology.trigger import TriggerCondition


class IntentStatus(str, Enum):
    INCOMPLETE = "incomplete"
    COMPLETE = "complete"


class Intent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    intent_type: str
    slots: Dict[str, Any] = Field(default_factory=dict)
    missing_slots: List[str] = Field(default_factory=list)
    constraints: Optional[Any] = None
    confidence: float = Field(default=0.0)
    status: IntentStatus
    raw_input: str
    clarification_prompt: Optional[str] = None
    condition: Optional[TriggerCondition] = None


class ExecutionPlan(BaseModel):
    model_config = ConfigDict(extra="ignore")
    intent_type: str
    chain: str
    parameters: Dict[str, Any] = Field(default_factory=dict)
    constraints: Optional[Any] = None
