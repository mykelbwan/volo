from __future__ import annotations

import asyncio
from typing import Any, Mapping

from .base import BridgeRequest
from .candidate import BridgeCandidate, BridgeCandidateOrigin, BridgeCandidateTrust
from .registry import get_bridge_providers


async def collect_candidates(
    *,
    request: BridgeRequest,
    route_meta: Mapping[str, Any] | None = None,
) -> tuple[list[BridgeCandidate], list[str]]:
    candidates: list[BridgeCandidate] = []
    diagnostics: list[str] = []

    providers = get_bridge_providers()
    route_meta_mapping: Mapping[str, Any] = (
        route_meta if isinstance(route_meta, Mapping) else {}
    )

    supported_providers = [provider for provider in providers if provider.supports(request)]

    for provider in supported_providers:
        provider_name = str(provider.name or "unknown")

        if route_meta_mapping:
            try:
                planned_quote = provider.quote_from_route_meta(
                    request=request,
                    route_meta=route_meta_mapping,
                )
            except Exception as exc:
                diagnostics.append(f"{provider_name}:planned:{exc}")
            else:
                if planned_quote is not None:
                    try:
                        provider.validate_route_meta(
                            request=request,
                            route_meta=route_meta_mapping,
                        )
                    except Exception as exc:
                        diagnostics.append(f"{provider_name}:planned:{exc}")
                    else:
                        candidates.append(
                            BridgeCandidate(
                                provider=provider,
                                quote=planned_quote,
                                origin=BridgeCandidateOrigin.PLANNED,
                                trust_level=BridgeCandidateTrust.HIGH,
                            )
                        )

    async def _dynamic_candidate(
        provider_name: str,
        provider: Any,
    ) -> tuple[BridgeCandidate | None, str | None]:
        try:
            dynamic_quote = await provider.quote_dynamic(request)
        except Exception as exc:
            return None, f"{provider_name}:dynamic:{exc}"
        if dynamic_quote is None:
            return None, None
        return (
            BridgeCandidate(
                provider=provider,
                quote=dynamic_quote,
                origin=BridgeCandidateOrigin.DYNAMIC,
                trust_level=BridgeCandidateTrust.MEDIUM,
            ),
            None,
        )

    dynamic_results = await asyncio.gather(
        *[
            _dynamic_candidate(str(provider.name or "unknown"), provider)
            for provider in supported_providers
        ]
    )
    for dynamic_candidate, dynamic_diag in dynamic_results:
        if dynamic_diag:
            diagnostics.append(dynamic_diag)
        if dynamic_candidate is not None:
            candidates.append(dynamic_candidate)

    return candidates, diagnostics
