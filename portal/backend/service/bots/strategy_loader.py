"""Clean database-based strategy loading for bot runtime.

This module provides a clean interface for loading strategies from the database
with all their relationships, replacing the confusing dict-based approach.
"""

from __future__ import annotations

import logging
from typing import List

from sqlalchemy import select

from ...db import db
from ...db.models import (
    ATMTemplateRecord,
    StrategyIndicatorLink as StrategyIndicatorLinkDB,
    StrategyInstrumentLink as StrategyInstrumentLinkDB,
    StrategyRecord,
    StrategyRuleRecord,
    StrategyVariantRecord,
)
from ..risk.atm import normalise_template
from engines.bot_runtime.strategy.models import Strategy, StrategyIndicatorLink, StrategyInstrumentLink
from risk import normalise_risk_config
from utils.log_context import build_log_context, with_log_context

logger = logging.getLogger(__name__)


class StrategyLoader:
    """Load strategies from database with clean contracts and strong typing.

    This replaces the confusing pattern of loading strategies into dicts
    and passing them through multiple layers. Instead, strategies are loaded
    fresh from the database with proper typing.
    """

    @staticmethod
    def fetch_strategy(strategy_id: str, runtime_config: dict | None = None) -> Strategy:
        """Fetch strategy with all relationships from database.

        Args:
            strategy_id: Strategy ID to load

        Returns:
            Strategy domain model with all relationships loaded

        Raises:
            ValueError: If strategy not found or database not available
        """
        if not db.available:
            raise ValueError("Database not available")

        with db.session() as session:
            # Fetch strategy record
            strategy_rec = session.get(StrategyRecord, strategy_id)
            if not strategy_rec:
                raise ValueError(f"Strategy not found: {strategy_id}")

            runtime_payload = dict(runtime_config or {})
            variant_id = str(runtime_payload.get("strategy_variant_id") or "").strip() or None
            variant_name = str(runtime_payload.get("strategy_variant_name") or "").strip() or None
            resolved_params = runtime_payload.get("resolved_params")
            if not isinstance(resolved_params, dict):
                resolved_params = {}

            variant_rec = None
            if variant_id:
                variant_rec = session.get(StrategyVariantRecord, variant_id)
                if variant_rec and str(variant_rec.strategy_id or "").strip() != strategy_id:
                    raise ValueError(
                        f"Strategy variant {variant_id} does not belong to strategy {strategy_id}"
                    )
                if variant_rec and not resolved_params:
                    resolved_params = dict(variant_rec.param_overrides or {})

            selected_atm_template_id = (
                (variant_rec.atm_template_id if variant_rec else None)
                or strategy_rec.atm_template_id
            )

            # Fetch ATM template if linked
            atm_template = None
            if selected_atm_template_id:
                template_rec = session.get(ATMTemplateRecord, selected_atm_template_id)
                if template_rec:
                    atm_template = normalise_template(template_rec.template)

            # Fetch indicator links
            indicator_links_db = session.execute(
                select(StrategyIndicatorLinkDB).where(StrategyIndicatorLinkDB.strategy_id == strategy_id)
            ).scalars().all()

            indicator_links = [
                StrategyIndicatorLink(
                    id=link.id,
                    strategy_id=link.strategy_id,
                    indicator_id=link.indicator_id,
                    # REMOVED: indicator_snapshot - will load fresh from DB when needed
                )
                for link in indicator_links_db
            ]

            # Fetch instrument links
            instrument_links_db = session.execute(
                select(StrategyInstrumentLinkDB).where(StrategyInstrumentLinkDB.strategy_id == strategy_id)
            ).scalars().all()

            instrument_links = [
                StrategyInstrumentLink(
                    id=link.id,
                    strategy_id=link.strategy_id,
                    instrument_id=link.instrument_id,
                    instrument_snapshot=link.instrument_snapshot or {},
                )
                for link in instrument_links_db
            ]

            # Fetch strategy rules
            rules_db = session.execute(
                select(StrategyRuleRecord).where(StrategyRuleRecord.strategy_id == strategy_id)
            ).scalars().all()

            # Convert rules to dict keyed by rule_id
            rules = {rule.id: rule.to_dict() for rule in rules_db}

            context = build_log_context(
                strategy_id=strategy_id,
                indicators=len(indicator_links),
                instruments=len(instrument_links),
                rules=len(rules),
            )
            logger.debug(with_log_context("strategy_loaded", context))

            # Build domain model
            return Strategy(
                id=strategy_rec.id,
                name=strategy_rec.name,
                timeframe=strategy_rec.timeframe,
                datasource=strategy_rec.datasource,
                exchange=strategy_rec.exchange,
                atm_template_id=selected_atm_template_id,
                atm_template=atm_template,
                risk_config=normalise_risk_config(
                    runtime_payload.get("risk_config")
                    if isinstance(runtime_payload.get("risk_config"), dict)
                    else strategy_rec.risk_config
                ),
                indicator_links=indicator_links,
                instrument_links=instrument_links,
                rules=rules,
                variant_name=variant_name,
                resolved_params=dict(resolved_params or {}),
            )

    @staticmethod
    def fetch_strategies(strategy_ids: List[str]) -> List[Strategy]:
        """Batch fetch multiple strategies.

        Args:
            strategy_ids: List of strategy IDs to load

        Returns:
            List of Strategy domain models

        Raises:
            ValueError: If any strategy not found
        """
        return [StrategyLoader.fetch_strategy(strategy_id) for strategy_id in strategy_ids]

    @staticmethod
    def strategy_exists(strategy_id: str) -> bool:
        """Check if strategy exists without loading full data.

        Args:
            strategy_id: Strategy ID to check

        Returns:
            True if strategy exists, False otherwise
        """
        if not db.available:
            return False

        with db.session() as session:
            strategy_rec = session.get(StrategyRecord, strategy_id)
            return strategy_rec is not None
