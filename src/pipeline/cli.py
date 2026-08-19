from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from pipeline.acquisition.download import AcquisitionError
from pipeline.config import ConfigError, load_config


logger = logging.getLogger("pipeline")


def _configure_logging() -> None:
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(logging.Formatter("%(message)s"))
    stdout_handler.addFilter(lambda record: record.levelno < logging.WARNING)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.setFormatter(logging.Formatter("%(message)s"))

    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)


def _cmd_validate_config(args: argparse.Namespace) -> int:
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        logger.error("config invalid: %s", exc)
        return 1
    print("config valid")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        logger.error("config invalid: %s", exc)
        return 1

    raw_path = args.raw_path or os.environ.get("PIPELINE_RAW_PATH")
    if not raw_path:
        logger.error("no raw path; pass --raw-path or set PIPELINE_RAW_PATH")
        return 1
    raw_root = Path(raw_path)
    staging_schema = args.staging_schema or os.environ.get("PIPELINE_STAGING_SCHEMA")
    if not staging_schema:
        logger.error("no staging schema; pass --staging-schema or set PIPELINE_STAGING_SCHEMA")
        return 1
    logger.info(
        "config ok; period=%s..%s; raw_path=%s staging=%s",
        args.period_start, args.period_end, raw_root, staging_schema,
    )

    # Imported here, not at module scope: `fleet replay` must reach a broker without
    # Spark on its import path, and it shares this entry point. run_pipeline reaches
    # staging, which imports pyspark, so it comes in here too.
    from pipeline.pipeline import run_pipeline
    from pipeline.spark import local_session

    spark = local_session("pipeline-run")

    try:
        run_pipeline(
            spark, config, raw_root, staging_schema, args.period_start, args.period_end,
        )
    except AcquisitionError as exc:
        logger.error("acquisition failed: %s", exc)
        return 1
    return 0


def _schema(args) -> str:
    schema = getattr(args, "staging_schema", None) or os.environ.get("PIPELINE_STAGING_SCHEMA")
    if not schema:
        raise SystemExit("no staging schema; pass --staging-schema or set PIPELINE_STAGING_SCHEMA")
    return schema


def _cmd_fleet_replay(args: argparse.Namespace) -> int:
    from pipeline import fleet

    raw_path = args.raw_path or os.environ.get("PIPELINE_RAW_PATH")
    if not raw_path:
        logger.error("no raw path; pass --raw-path or set PIPELINE_RAW_PATH")
        return 1
    fleet.replay(args.period_start, args.period_end, Path(raw_path), load_config(args.config),
                 limit=args.limit, inject_invalid=args.inject_invalid)
    return 0


def _cmd_fleet_receipts(args: argparse.Namespace) -> int:
    from pipeline import fleet

    fleet.receipts(idle_timeout=args.idle_timeout)
    return 0


def _cmd_fleet_ingest(args: argparse.Namespace) -> int:
    from pipeline import fleet

    fleet.ingest(_schema(args), run_id=args.run_id)
    return 0


def _cmd_fleet_drop_raw(args: argparse.Namespace) -> int:
    from pipeline import fleet

    fleet.drop_raw(_schema(args))
    return 0


def _cmd_fleet_verify(args: argparse.Namespace) -> int:
    from pipeline import fleet

    try:
        fleet.verify(_schema(args), args.expect_quarantined)
    except ValueError as exc:
        logger.error("%s", exc)
        return 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser(
        "validate-config", help="validate a config file without touching data"
    )
    validate.add_argument("--config", required=True)
    validate.set_defaults(func=_cmd_validate_config)

    run = subparsers.add_parser(
        "run", help="acquire a period range and stage its weather")
    run.add_argument("--config", required=True)
    run.add_argument("--period-start", required=True)
    run.add_argument("--period-end", required=True)
    run.add_argument(
        "--raw-path", default=None,
        help="raw storage root (or set PIPELINE_RAW_PATH)",
    )
    run.add_argument(
        "--staging-schema", default=None,
        help="catalog.schema staging tables are written to (or set PIPELINE_STAGING_SCHEMA)",
    )
    run.set_defaults(func=_cmd_run)

    fleet = subparsers.add_parser("fleet", help="the Kafka trip and receipt path")
    fleet_commands = fleet.add_subparsers(dest="fleet_command", required=True)

    replay = fleet_commands.add_parser("replay", help="publish acquired trips as events")
    replay.add_argument("--config", required=True)
    replay.add_argument("--period-start", required=True)
    replay.add_argument("--period-end", required=True)
    replay.add_argument("--raw-path", default=None, help="or set PIPELINE_RAW_PATH")
    replay.add_argument("--limit", type=int, default=None, help="rows per period")
    replay.add_argument("--inject-invalid", type=int, default=0,
                        help="also publish N records the marts must refuse")
    replay.set_defaults(func=_cmd_fleet_replay)

    receipts = fleet_commands.add_parser(
        "receipts", help="read completed trips and post the receipt each one owes")
    receipts.add_argument("--idle-timeout", type=float, default=15.0)
    receipts.set_defaults(func=_cmd_fleet_receipts)

    ingest = fleet_commands.add_parser("ingest", help="land Kafka records in raw Delta")
    ingest.add_argument("--staging-schema", default=None, help="or set PIPELINE_STAGING_SCHEMA")
    ingest.add_argument("--run-id", default=None)
    ingest.set_defaults(func=_cmd_fleet_ingest)

    drop_raw = fleet_commands.add_parser(
        "drop-raw", help="forget ingested records; only correct when the broker is reset")
    drop_raw.add_argument("--staging-schema", default=None, help="or set PIPELINE_STAGING_SCHEMA")
    drop_raw.set_defaults(func=_cmd_fleet_drop_raw)

    verify = fleet_commands.add_parser("verify", help="reconcile the published counts")
    verify.add_argument("--staging-schema", default=None, help="or set PIPELINE_STAGING_SCHEMA")
    verify.add_argument("--expect-quarantined", type=int, required=True,
                        help="how many records the run deliberately made unusable")
    verify.set_defaults(func=_cmd_fleet_verify)

    return parser


def main(argv: list[str] | None = None) -> int:
    _configure_logging()
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
