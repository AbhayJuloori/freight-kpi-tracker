"""Command-line entry point for the portable Freight v2 engine."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from freight_v2 import __version__
from freight_v2.config import SeedSource
from freight_v2.export import export_portfolio_bundle
from freight_v2.run_builder import accept_run, build_run, resolve_run


def build_parser() -> argparse.ArgumentParser:
    """Build the root parser; workflow subcommands are added by later phases."""
    parser = argparse.ArgumentParser(
        prog="freight-v2",
        description="Portable freight investigation and anomaly evaluation engine.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subcommands = parser.add_subparsers(dest="command", required=True)

    build = subcommands.add_parser("build", help="build and validate an immutable data run")
    build.add_argument(
        "--seed-source", choices=[source.value for source in SeedSource], required=True
    )
    build.add_argument("--faf5-path", type=Path)
    build.add_argument("--rows", type=int, default=5_000)
    build.add_argument("--output", type=Path, required=True)
    build.add_argument("--random-seed", type=int, default=42)
    build.add_argument("--anomaly-seed", type=int, default=43)
    build.add_argument("--fixture-name", default="cli")

    validate = subcommands.add_parser("validate", help="validate an immutable data run")
    validate.add_argument("--artifact-root", type=Path, required=True)
    run_choice = validate.add_mutually_exclusive_group(required=True)
    run_choice.add_argument("--run")
    run_choice.add_argument("--latest", action="store_true")

    accept = subcommands.add_parser("accept", help="mark one validated run as accepted")
    accept.add_argument("--artifact-root", type=Path, required=True)
    accept_choice = accept.add_mutually_exclusive_group(required=True)
    accept_choice.add_argument("--run")
    accept_choice.add_argument("--latest", action="store_true")

    export = subcommands.add_parser(
        "export-portfolio", help="export a validated public portfolio evidence bundle"
    )
    export.add_argument("--artifact-root", type=Path, required=True)
    export_choice = export.add_mutually_exclusive_group(required=True)
    export_choice.add_argument("--run")
    export_choice.add_argument("--latest", action="store_true")
    export.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Freight v2 command-line interface."""
    arguments = build_parser().parse_args(argv)
    if arguments.command == "build":
        path = build_run(
            artifact_root=arguments.output,
            seed_source=SeedSource(arguments.seed_source),
            rows=arguments.rows,
            random_seed=arguments.random_seed,
            anomaly_seed=arguments.anomaly_seed,
            faf5_path=arguments.faf5_path,
            fixture_name=arguments.fixture_name,
        )
        print(path)
    elif arguments.command == "validate":
        path = resolve_run(
            arguments.artifact_root,
            run_id=arguments.run,
            latest=arguments.latest,
        )
        print(path)
    elif arguments.command == "accept":
        print(
            accept_run(
                arguments.artifact_root,
                run_id=arguments.run,
                latest=arguments.latest,
            )
        )
    elif arguments.command == "export-portfolio":
        run_path = resolve_run(
            arguments.artifact_root,
            run_id=arguments.run,
            latest=arguments.latest,
        )
        print(export_portfolio_bundle(run_path, arguments.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
