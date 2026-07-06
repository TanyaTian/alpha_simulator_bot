"""
AlphaSignalFactory — Barra-Style Noise-Hedging Variant Generator

Reframes signal "optimization" as cross-sectional risk attribution:
remove noise from known risk factors (size, sector, industry, volatility,
liquidity) via group operations and vector neutralization, revealing the
underlying economic signal.

Architecture:
  AlphaSignalFactory
  ├── process_signals()             Entry: query → fetch → generate → insert
  ├── _fetch_base_signals()         Query stage_one_base_signals
  ├── _read_alpha_details()         Fetch expression + settings from BRAIN
  ├── _generate_hedged_variants()   Core: CS-hedged expression variants
  └── prepare_simulation_data()     Format for pending-simulation table
"""

from dao.stage_one_signal_dao import StageOneSignalDAO
from dao.alpha_list_pending_simulated_dao import AlphaListPendingSimulatedDAO
from ace_lib import start_session, check_session_and_relogin, get_simulation_result_json
from datetime import datetime
from logger import Logger
import random


class AlphaSignalFactory:
    """Generate noise-hedged alpha expression variants for simulation."""

    # ── Region Configuration ───────────────────────────────────────────

    # Universal simple group names (applicable to both USA and GLB)
    UNIVERSAL_GROUPS = [
        'market', 'sector', 'industry', 'subindustry', 'exchange'
    ]

    # USA-specific group fields (from USA.md)
    USA_GROUPS = [
        'pv13_h_min2_3000_sector',
        'pv13_r2_min20_3000_sector',
        'pv13_r2_min2_3000_sector',
        'pv13_h_min2_focused_pureplay_3000_sector',
        'sta1_top3000c50',
        'sta1_allc20',
        'sta1_allc10',
        'sta1_top3000c20',
        'sta1_allc5',
        'sta2_top3000_fact3_c50',
        'sta2_top3000_fact4_c20',
        'sta2_top3000_fact4_c10',
        'mdl10_group_name',
    ]

    # GLB-specific group fields (from GLB.md)
    GLB_GROUPS = [
        'sta1_allc20',
        'sta1_allc10',
        'sta1_allc50',
        'sta1_allc5',
        'pv13_2_sector',
        'pv13_10_sector',
        'pv13_3l_scibr',
        'pv13_2l_scibr',
        'pv13_1l_scibr',
        'pv13_52_minvol_1m_all_delay_1_sector',
        'pv13_52_minvol_1m_sector',
        'sta3_pvgroup2_sector',
        'sta3_pvgroup3_sector',
    ]

    # GLB-specific cartesian product groups (country cross-sections)
    GLB_CARTESIAN_GROUPS = [
        'group_cartesian_product(country, market)',
        'group_cartesian_product(country, industry)',
        'group_cartesian_product(country, subindustry)',
        'group_cartesian_product(country, exchange)',
        'group_cartesian_product(country, sector)',
    ]

    # Custom bucket definitions (both regions)
    CUSTOM_BUCKETS = [
        "bucket(rank(cap), range='0.1, 1, 0.1')",
        "bucket(rank(aggregate_share_count_all_owners), range='0.1, 1, 0.1')",
        "bucket(rank(ts_std_dev(returns, 20)), range='0.1, 1, 0.1')",
        "bucket(rank(close*volume), range='0.1, 1, 0.1')",
        "bucket(group_rank(cap, sector), range='0.1, 1, 0.1')",
    ]

    # Vector neutralization targets (risk factors to orthogonalize against)
    VECTORS = ['cap', 'aggregate_share_count_all_owners', 'mdl77_nlvolcap']

    # Time-series vector neutralization window lengths (calendar days)
    TS_VECTOR_NEUT_WINDOWS = [252, 63, 21]

    # ── Lifecycle ──────────────────────────────────────────────────────

    def __init__(self):
        self.logger = Logger()
        self.stage_one_dao = StageOneSignalDAO()
        self.pending_dao = AlphaListPendingSimulatedDAO()
        self.session = start_session()

    # ── Region Helpers ─────────────────────────────────────────────────

    def _get_groups_for_region(self, region):
        """Return the full list of group expressions for *region*."""
        groups = list(self.UNIVERSAL_GROUPS)
        if region == 'USA':
            groups += self.USA_GROUPS
        elif region == 'GLB':
            groups += self.GLB_GROUPS + self.GLB_CARTESIAN_GROUPS
        groups += self.CUSTOM_BUCKETS
        return groups

    # ── Expression Helpers ─────────────────────────────────────────────

    @staticmethod
    def _split_multiline(expression):
        """Return ``(body_lines, result_expr)``; body_lines is None for single-line."""
        stripped = expression.strip()
        if '\n' not in stripped:
            return None, stripped
        lines = stripped.split('\n')
        body = '\n'.join(lines[:-1])
        result = lines[-1].strip()
        return body, result

    @staticmethod
    def _wrap_expression(body, result_expr, wrapper_template):
        """Apply *wrapper_template.format(result_expr)*, preserving multi-line body."""
        wrapped = wrapper_template.format(result_expr)
        if body:
            return f"{body}\n            {wrapped}"
        return wrapped

    # ── Public ─────────────────────────────────────────────────────────

    def process_signals(
        self,
        dataset_id,
        date_time,
        region,
        priority=5,
        mode='normal',
        margin_limit=0.0,
        sharpe_limit=0.0,
        fitness_limit=0.0,
    ):
        """
        Main entry point: fetch, hedge, insert.

        Args:
            dataset_id:    Dataset identifier (e.g., "analyst_estimates")
            date_time:     Date string (e.g., "20260512") or list of date strings
                           (e.g., ["20260510", "20260511", "20260512"]). A list
                           queries alphas across multiple timestamps, which is
                           typical when a dataset's data spans several dates.
            region:        "USA" or "GLB"
            priority:      Simulation queue priority (default 5)
            mode:          'test' (process ~10 alphas) or 'normal' (all)
            margin_limit:  IS margin threshold
            sharpe_limit:  IS sharpe threshold
            fitness_limit: IS fitness threshold
        """
        self.session = check_session_and_relogin(self.session)

        # 1. Fetch base alpha IDs from database
        alpha_ids = self._fetch_base_signals(dataset_id, date_time, region)
        if not alpha_ids:
            self.logger.info(
                f"No signals: dataset={dataset_id} dates={date_time} region={region}"
            )
            return None

        self.logger.info(
            f"Found {len(alpha_ids)} base signals for {region}/{dataset_id}"
        )

        # 2. Sample if in test mode
        if mode == 'test':
            alpha_ids = random.sample(alpha_ids, min(10, len(alpha_ids)))
            self.logger.info(f"Test mode: sampled {len(alpha_ids)} alphas")

        # 3. Read details, filter, generate hedged variants
        settings_dict = {}
        all_records = []  # (alpha_id, expression, decay, region)
        skipped = 0

        for alpha_id in alpha_ids:
            details = self._read_alpha_details(alpha_id)
            if not details:
                skipped += 1
                continue

            expression = details.get('expression')
            if not expression:
                skipped += 1
                continue

            # Performance filtering
            is_stats = details.get('is_stats', {})
            margin = is_stats.get('margin', 0)
            sharpe = is_stats.get('sharpe', 0)
            fitness = is_stats.get('fitness', 0)

            if margin < margin_limit or sharpe < sharpe_limit or fitness < fitness_limit:
                self.logger.debug(
                    f"Filtered {alpha_id}: margin={margin:.4f} "
                    f"sharpe={sharpe:.2f} fitness={fitness:.2f}"
                )
                skipped += 1
                continue

            settings = details.get('settings', {})
            alpha_region = settings.get('region', region)
            decay = settings.get('decay', 0)
            settings_dict[alpha_id] = settings

            # Generate hedged variants
            variants = self._generate_hedged_variants(expression, region)
            for v_expr in variants:
                all_records.append((alpha_id, v_expr, decay, alpha_region))

        accepted = len(alpha_ids) - skipped
        self.logger.info(
            f"Generated {len(all_records)} variants from {accepted} alphas "
            f"(skipped {skipped})"
        )

        if not all_records:
            self.logger.info("No records to insert")
            return None

        # 4. Shuffle then batch insert
        random.shuffle(all_records)
        db_records = self.prepare_simulation_data(all_records, settings_dict, priority)
        inserted = self.pending_dao.batch_insert(db_records)
        self.logger.info(f"Inserted {inserted} records into pending-simulated table")
        return db_records

    # ── Internal: Data Fetching ────────────────────────────────────────

    def _fetch_base_signals(self, dataset_id, date_times, region):
        """Query ``stage_one_base_signals``, return list of alpha_id strings."""
        rows = self.stage_one_dao.get_alphas_by_dataset_region(
            dataset_id, date_times, region
        )
        if not rows:
            return []
        return [r['alpha_id'] for r in rows]

    def _read_alpha_details(self, alpha_id):
        """
        Fetch expression and settings from the BRAIN platform.

        Returns:
            dict with keys ``expression``, ``settings``, ``is_stats``, or None.
        """
        details = get_simulation_result_json(self.session, alpha_id)
        if not details:
            self.logger.warning(f"No simulation result for alpha {alpha_id}")
            return None

        regular_data = details.get('regular', {})
        expression = regular_data.get('code')
        if not expression:
            self.logger.debug(f"Alpha {alpha_id}: no expression code")
            return None

        return {
            'expression': expression,
            'settings': details.get('settings', {}),
            'is_stats': details.get('is', {}),
        }

    # ── Internal: Hedging Variant Generation ──────────────────────────

    def _generate_hedged_variants(self, expression, region):
        """
        Core: generate CS-hedged expression variants.

        Five layers of hedging:

        Layer 1 — Single Group Op
            group_neutralize / group_zscore / group_rank / group_scale
            against each known group.
            Purpose: remove sector/industry/country bias, normalize within groups.
            group_scale (min-max → [0,1]) prevents sectors with inherently larger
            signal amplitudes from dominating the portfolio.

        Layer 2 — Cross-Sectional Normalization & Vector Neutralization
            normalize: market-mean removal (built-in cs demeaning).
            vector_neut: orthogonalize against size, ownership, vol factors.
            winsorize: clamp extreme outliers to ±Nσ, preventing concentration risk
            from a single extreme signal value.

        Layer 3 — Combined (vector + group, 2 CS layers)
            Size-hedged then group-neutralized/zscored via sequential nesting.

        Layer 4 — Integrated group_vector_neut (platform-native, 1 CS layer)
            group_vector_neut(x, y, g): orthogonolize x against y within each
            group g. Same risk-attribution effect as Layer 3 but as a single
            operator call.

        Layer 5 — Time-Series Vector Neutralization
            ts_vector_neut(x, y, d): remove the time-series projection of the
            signal onto a risk factor. Complements CS hedging by stripping
            temporal factor exposures (e.g., if your signal co-moves with
            market cap over time).

        Returns:
            List of expression strings.
        """
        variants = []
        groups = self._get_groups_for_region(region)
        body, result_expr = self._split_multiline(expression)

        def w(template):
            """Shorthand: wrap *result_expr* with *template*."""
            return self._wrap_expression(body, result_expr, template)

        # ── Layer 1: Single Group Operations ──
        for group in groups:
            variants.append(w(f"group_neutralize({{}}, densify({group}))"))
            variants.append(w(f"group_zscore({{}}, densify({group}))"))
            variants.append(w(f"group_rank({{}}, densify({group}))"))
            # Min-max normalize to [0,1] within each group — prevents
            # sectors with larger signal amplitude from dominating
            variants.append(w(f"group_scale({{}}, densify({group}))"))

        # ── Layer 2: Cross-Sectional Normalization, Outlier Treatment & Vector Neutralization ──
        # Market-mean removal (equivalent to group_neutralize against market)
        variants.append(w("normalize({})"))
        variants.append(w("normalize({}, useStd=true)"))

        # Outlier treatment — clamp extreme signal values to prevent
        # concentration risk from a single extreme observation
        variants.append(w("winsorize({}, std=3)"))
        variants.append(w("winsorize({}, std=4)"))

        # Vector neutralization against risk factors
        for vector in self.VECTORS:
            variants.append(w(f"vector_neut({{}}, {vector})"))

        # ── Layer 3: Combined — vector_neut then group_neutralize / group_zscore ──
        for vector in self.VECTORS:
            for group in groups:
                # Size-hedged → sector-normalized
                variants.append(w(
                    f"group_neutralize(vector_neut({{}}, {vector}), densify({group}))"
                ))
                # Size-hedged → group-zscored
                variants.append(w(
                    f"group_zscore(vector_neut({{}}, {vector}), densify({group}))"
                ))

        # ── Layer 4: Integrated group_vector_neut (platform-native) ──
        # Orthogonolize against risk factor within each group in one step.
        # This is a native BRAIN operator that combines group + vector
        # neutralization, discovered via platform operator audit.
        for vector in self.VECTORS:
            for group in groups:
                variants.append(w(
                    f"group_vector_neut({{}}, {vector}, densify({group}))"
                ))

        # ── Layer 5: Time-Series Vector Neutralization ──
        # Remove temporal factor co-movement. If the signal's time-series
        # is correlated with a risk factor (e.g., moves with cap over time),
        # ts_vector_neut strips that time-series projection, complementing
        # the cross-sectional hedging above.
        for vector in self.VECTORS:
            for d in self.TS_VECTOR_NEUT_WINDOWS:
                variants.append(w(f"ts_vector_neut({{}}, {vector}, {d})"))

        return variants

    # ── Internal: Record Formatting ────────────────────────────────────

    def prepare_simulation_data(self, alpha_records, settings_dict, priority):
        """
        Format records for ``alpha_list_pending_simulated_table``.

        Args:
            alpha_records: List of ``(alpha_id, expression, decay, region)``
            settings_dict: ``{alpha_id: settings_dict}``
            priority:      Task priority

        Returns:
            List of dicts ready for batch insert.
        """
        records = []
        for alpha_id, expression, decay, region in alpha_records:
            signal_settings = settings_dict.get(alpha_id, {})

            sim_settings = {
                'instrumentType': 'EQUITY',
                'region': region,
                'universe': signal_settings.get('universe', 'TOP3000'),
                'delay': signal_settings.get('delay', 1),
                'decay': decay,
                'neutralization': signal_settings.get('neutralization', 'NONE'),
                'truncation': signal_settings.get('truncation', 0.01),
                'pasteurization': 'ON',
                'testPeriod': 'P0Y',
                'unitHandling': 'VERIFY',
                'nanHandling': 'OFF',
                'language': 'FASTEXPR',
                'visualization': signal_settings.get('visualization', False),
                'maxTrade': signal_settings.get('maxTrade', 'OFF'),
            }

            records.append({
                'type': 'REGULAR',
                'settings': str(sim_settings),
                'regular': expression,
                'priority': priority,
                'region': region,
                'created_at': datetime.now(),
            })

        return records


# ── CLI Entry Point ────────────────────────────────────────────────────

def main():
    """
    Run AlphaSignalFactory from the command line.

    Usage::

        python src/alpha_signal_factory.py

    Customize *config* dict below for specific dataset/date/region runs.
    """
    logger = Logger()
    logger.info("Starting Alpha Signal Factory (Noise-Hedging)")

    config = {
        'dataset_id': 'analyst10',
        'date_time': ['20260704', '20260703'],
        'region': 'GLB',
        'priority': 2,
        'mode': 'normal',
        'margin_limit': 0.0,
        'sharpe_limit': 0.8,
        'fitness_limit': 0.3,
    }

    try:
        factory = AlphaSignalFactory()
        factory.process_signals(**config)
        logger.info("Alpha signal processing completed successfully")
    except Exception as e:
        logger.error(f"Alpha signal processing failed: {e}")
        raise


if __name__ == "__main__":
    main()
