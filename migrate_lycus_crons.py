#!/usr/bin/env python3
"""Migrate Lycus cron jobs to Khronos/Temporal scheduling system."""

import json
import sys
sys.path.insert(0, '/home/waymore/khronos/python')

from khronos_client import KhronosClient
from khronos_client.types import ScheduleSpec, WorkflowAction, OverlapPolicy, Timeouts

# ── Job definitions mapped from ~/.autolycus/cron/jobs.json ────────────────

JOBS = [
    # 1. arxiv-latex-factory-daily (event-driven chain entry point)
    {
        "schedule_id": "arxiv-latex-factory-daily",
        "workflow_name": "lycus-arxiv-latex-factory",
        "description": "Search for new solvmanifolds papers and generate pedagogical documents",
        # Originally every 99999m (event-driven). Model as daily in Khronos.
        "spec": ScheduleSpec(cron_expressions=["0 6 * * *"]),  # Daily at 6 AM
        "args": {
            "prompt": 'Load the arxiv-latex-factory skill and execute it with subject="solvmanifolds". Search for new papers, check tracking, and generate pedagogical documents for any uncovered papers. Report what was done.',
            "skills": "arxiv-latex-factory",
            "toolsets": "search,file,terminal,web",
            "lycus_origin_id": "9d549483e571",
        },
        "policy": OverlapPolicy.SKIP,
        "timeouts": Timeouts(execution_timeout_secs=7200),  # 2 hours for research pipeline
    },
    # 2. mathNEXUS-knowledge-graph-update (triggered by arxiv on_success)
    {
        "schedule_id": "mathNEXUS-knowledge-graph-update",
        "workflow_name": "lycus-mathNEXUS-pipeline",
        "description": "Extract knowledge graphs from new arXiv papers into mathNEXUS",
        # Originally every 99999m (event-driven). Model as daily, offset by 1 hour.
        "spec": ScheduleSpec(cron_expressions=["0 7 * * *"]),  # Daily at 7 AM
        "args": {
            "prompt": "Run the mathNEXUS knowledge graph pipeline: check ~/Documents/AI_researched_math/arxiv_factory/ for new subdirectories, run extract_mindmap.py, merge_concept_graph.py, discovery_engine.py. Report results.",
            "toolsets": "terminal,file",
            "lycus_origin_id": "4d68c1e4f35a",
        },
        "policy": OverlapPolicy.SKIP,
        "timeouts": Timeouts(execution_timeout_secs=7200),
    },
    # 3. mathLaboratory-research-agent (triggered by mathNEXUS on_success)
    {
        "schedule_id": "mathLaboratory-research-agent",
        "workflow_name": "lycus-mathLaboratory-agent",
        "description": "Research agent that finds opportunities using 5 strategies",
        # Originally every 99999m (event-driven). Model as daily, offset by another hour.
        "spec": ScheduleSpec(cron_expressions=["0 8 * * *"]),  # Daily at 8 AM
        "args": {
            "prompt": "Run the mathLaboratory research agent: cd ~/Documents/AI_researched_math/mathLaboratory && python laboratory_agent.py run. Report opportunities found, documents generated, and interesting research directions.",
            "toolsets": "terminal,file",
            "lycus_origin_id": "af4073e39b20",
        },
        "policy": OverlapPolicy.SKIP,
        "timeouts": Timeouts(execution_timeout_secs=7200),
    },
    # 4. memory-condensation-cycle (every 6 hours)
    {
        "schedule_id": "memory-condensation-cycle",
        "workflow_name": "lycus-memory-condenser",
        "description": "Condense memory layers L0->L1->L2 opportunistically every 6 hours",
        # Originally every 360m = 6 hours. Cron: 0 */6 * * *
        "spec": ScheduleSpec(cron_expressions=["0 */6 * * *"]),
        "args": {
            "prompt": "Run the memory condensation engine opportunistically. Execute L0->L1->L2 condensation pipeline at /home/waymore/compiled/autolycus-agent/scripts/memory_condenser.py",
            "lycus_origin_id": "ccf14ac55fbe",
        },
        "policy": OverlapPolicy.SKIP,
        "timeouts": Timeouts(execution_timeout_secs=3600),  # 1 hour
    },
    # 5. cron-failure-notifier (every 5 minutes) - script-only job
    {
        "schedule_id": "cron-failure-notifier",
        "workflow_name": "lycus-cron-notifier",
        "description": "Check for failed cron jobs and send alerts every 5 minutes",
        # Originally every 5m. Cron: */5 * * * *
        "spec": ScheduleSpec(cron_expressions=["*/5 * * * *"]),
        "args": {
            "script": "cron_notifier.py",
            "no_agent": "true",
            "lycus_origin_id": "4e34e4d5e57c",
        },
        "policy": OverlapPolicy.SKIP,
        "timeouts": Timeouts(execution_timeout_secs=300),  # 5 minutes max
    },
    # 6. searxng-health-check (every 30 minutes)
    {
        "schedule_id": "searxng-health-check",
        "workflow_name": "lycus-searxng-healthcheck",
        "description": "Verify SearXNG works end-to-end via MCP tool every 30 minutes",
        # Originally every 30m. Cron: */30 * * * *
        "spec": ScheduleSpec(cron_expressions=["*/30 * * * *"]),
        "args": {
            "prompt": 'Run an actual SearXNG search via the MCP tool to verify it works end-to-end. Use mcp_searxng_searxng_web_search with query="test health check" and num_results=3.',
            "toolsets": "web",
            "lycus_origin_id": "4f793a9807a9",
        },
        "policy": OverlapPolicy.SKIP,
        "timeouts": Timeouts(execution_timeout_secs=600),  # 10 minutes max
    },
    # 7. searxng-429-trigger (every 5 minutes)
    {
        "schedule_id": "searxng-429-trigger",
        "workflow_name": "lycus-searxng-error-reactive",
        "description": "Reactive check for SearXNG 429 errors every 5 minutes",
        # Originally every 5m. Cron: */5 * * * *
        "spec": ScheduleSpec(cron_expressions=["*/5 * * * *"]),
        "args": {
            "prompt": 'SearXNG 429/error reactive check. Check recent errors in ~/.autolycus/logs/errors.log for SearXNG failures and run an actual MCP search test.',
            "model": "qwen/qwen3.6-27b-mtp",
            "provider": "custom",
            "lycus_origin_id": "6a123928e074",
        },
        "policy": OverlapPolicy.SKIP,
        "timeouts": Timeouts(execution_timeout_secs=600),  # 10 minutes max
    },
]


def main():
    print("=" * 70)
    print("Khronos Migration: Lycus Cron Jobs -> Temporal Schedules")
    print("=" * 70)

    with KhronosClient('localhost', 50053) as client:
        # Check existing schedules first
        existing = {s.schedule_id for s in client.list_schedules()}
        print(f"\nExisting schedules on server: {len(existing)}")
        if existing:
            for sid in existing:
                print(f"  - {sid}")

        created = []
        skipped = []
        errors = []

        for job in JOBS:
            sid = job["schedule_id"]
            wf_name = job["workflow_name"]
            desc = job["description"]

            if sid in existing:
                print(f"\n⊘ SKIP {sid} (already exists)")
                skipped.append(sid)
                continue

            # Build the action with args from the original Lycus job
            action = WorkflowAction(
                workflow_name=wf_name,
                args={k: str(v) for k, v in job["args"].items()},
                task_queue="default",
                timeouts=job.get("timeouts", Timeouts()),
            )

            try:
                result_id = client.create_schedule(
                    schedule_id=sid,
                    spec=job["spec"],
                    action=action,
                    policy=job.get("policy", OverlapPolicy.SKIP),
                )
                created.append(sid)
                print(f"\n✓ CREATED {sid}")
                print(f"  Workflow: {wf_name}")
                print(f"  Description: {desc}")
                spec_desc = job["spec"].cron_expressions or [f"interval={job['spec'].interval_seconds}s"]
                print(f"  Schedule: {', '.join(spec_desc)}")
                print(f"  Policy: {job.get('policy', OverlapPolicy.SKIP).value}")

            except Exception as e:
                errors.append((sid, str(e)))
                print(f"\n✗ ERROR creating {sid}: {e}")

        # ── Summary ────────────────────────────────────────────────
        print("\n" + "=" * 70)
        print("MIGRATION SUMMARY")
        print("=" * 70)
        print(f"  Created: {len(created)}")
        for sid in created:
            print(f"    ✓ {sid}")

        if skipped:
            print(f"\n  Skipped (already existed): {len(skipped)}")
            for sid in skipped:
                print(f"    ⊘ {sid}")

        if errors:
            print(f"\n  Errors: {len(errors)}")
            for sid, err in errors:
                print(f"    ✗ {sid}: {err}")

        # Verify by listing all schedules
        print("\n" + "-" * 70)
        print("ALL SCHEDULES ON KHRONOS SERVER:")
        print("-" * 70)
        all_schedules = client.list_schedules()
        for s in sorted(all_schedules, key=lambda x: x.schedule_id):
            spec_str = ""
            if s.spec:
                if s.spec.cron_expressions:
                    spec_str = f"cron={','.join(s.spec.cron_expressions)}"
                elif s.spec.interval_seconds > 0:
                    spec_str = f"interval={s.spec.interval_seconds}s"

            action_str = ""
            if s.action:
                action_str = f"wf={s.action.workflow_name}"

            print(f"  {s.schedule_id:<45} {spec_str:<35} {action_str}")

        total_expected = len(JOBS)
        actual_count = len(all_schedules)
        if actual_count == total_expected:
            print(f"\n✓ SUCCESS: All {total_expected} jobs migrated to Khronos.")
        else:
            print(f"\n⚠ PARTIAL: Expected {total_expected}, found {actual_count} on server.")

    return 0 if not errors else 1


if __name__ == "__main__":
    sys.exit(main())
