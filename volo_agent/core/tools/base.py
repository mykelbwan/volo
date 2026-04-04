from typing import Any, Callable, Dict, Optional, Type

from pydantic import BaseModel, Field

from core.memory.ledger import ErrorCategory


class Tool(BaseModel):
    """
    Metadata for a tool in the registry with support for adaptive recovery.
    """

    name: str
    description: str
    func: Callable
    on_suggest_fix: Optional[Callable] = None
    args_schema: Optional[Type[BaseModel]] = None
    category: str = "general"
    timeout_seconds: Optional[float] = None

    async def run(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Runs the tool's function, optionally validating inputs first.
        """
        if self.args_schema:
            # Create a copy for validation that replaces markers with dummy values
            validation_args = args.copy()
            for k, v in validation_args.items():
                if isinstance(v, str) and "{{" in v:
                    validation_args[k] = "0.0"

            self.args_schema(**validation_args)

        return await self.func(args)

    def suggest_fix(
        self,
        error_category: ErrorCategory,
        current_args: Dict[str, Any],
        error_msg: str,
    ) -> Optional[Dict[str, Any]]:
        if self.on_suggest_fix:
            return self.on_suggest_fix(error_category, current_args, error_msg)
        return None


class Registry(BaseModel):
    tools: Dict[str, Tool] = Field(default_factory=dict)

    def register(self, tool: Tool):
        self.tools[tool.name] = tool

    def get(self, name: str) -> Optional[Tool]:
        return self.tools.get(name)
