"""CLI entry-point for querying Garmin training summaries."""
from __future__ import annotations

import argparse
from typing import List

from query import query


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Ask questions about your Garmin training history."
    )
    parser.add_argument("question", help="Natural-language question to answer")
    parser.add_argument(
        "-n",
        "--n-results",
        type=int,
        default=5,
        help="Number of summaries to retrieve from the vector store (default: 5)",
    )
    parser.add_argument(
        "--show-context",
        action="store_true",
        help="Print the retrieved summaries and metadata before the answer",
    )

    args = parser.parse_args(argv)

    answer, context = query(args.question, n_results=args.n_results)

    if args.show_context:
        print("Retrieved context:")
        for idx, item in enumerate(context, start=1):
            meta = item.get("metadata", {})
            print(f"\n[{idx}] {item.get('document', '')}")
            if meta:
                for key, value in meta.items():
                    print(f"    {key}: {value}")

        print("\nAnswer:")

    print(answer)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
