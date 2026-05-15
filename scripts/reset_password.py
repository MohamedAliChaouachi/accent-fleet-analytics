#!/usr/bin/env python
"""
Operator-side password reset.

When the password printed by `seed_auth.py` is lost (or you want to
rotate a service account credential before /v1/admin/users/{id}/reset-password
is reachable), this script updates `auth.users.password_hash` directly
with a fresh argon2id hash of the supplied plaintext.

Uses the same Argon2id parameters as seed_auth.py and the M3 admin
reset route, so hashes produced here verify identically.

Usage
-----
    python scripts/reset_password.py --email superadmin@accent.local \\
                                     --password 'whatever-you-want'

    # Refuses passwords <12 chars by default. Pass --allow-weak to override —
    # only sensible for the dev compose stack where the API isn't internet-
    # exposed. Production rotations should go through the admin API once
    # M3 is live so the change is audit-logged.
    python scripts/reset_password.py --email x@y.local \\
                                     --password 'short' --allow-weak

Exit codes
----------
    0  success
    2  bad CLI args
    3  user not found
    4  password rejected (too short, --allow-weak not set)
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Make src/ importable when run as a plain script (mirrors seed_auth.py).
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from argon2 import PasswordHasher
from sqlalchemy import text

from accent_fleet.db.engine import get_engine

# Keep parameters in lockstep with scripts/seed_auth.py — different costs
# would still verify because argon2 encodes them in the hash string, but
# new hashes should match the rest of the corpus so re-hash decisions
# (auth.passwords.needs_rehash) don't flip-flop.
HASHER = PasswordHasher(
    time_cost=3,
    memory_cost=64 * 1024,
    parallelism=4,
    hash_len=32,
    salt_len=16,
)

logger = logging.getLogger("accent_fleet.reset_password")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--email", required=True, help="auth.users.email of the target")
    p.add_argument("--password", required=True, help="new plaintext password")
    p.add_argument(
        "--allow-weak",
        action="store_true",
        help="Allow passwords shorter than 12 chars (dev only).",
    )
    args = p.parse_args(argv)

    logging.basicConfig(
        level="INFO", format="%(asctime)s %(levelname)s %(name)s | %(message)s"
    )

    if len(args.password) < 12 and not args.allow_weak:
        print(
            f"ERROR: password is only {len(args.password)} characters. "
            "Pass --allow-weak to override (dev only).",
            file=sys.stderr,
        )
        return 4

    new_hash = HASHER.hash(args.password)

    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "UPDATE auth.users SET password_hash = :h "
                "WHERE email = :e "
                "RETURNING user_id, tenant_id, role"
            ),
            {"h": new_hash, "e": args.email},
        ).first()

        if row is None:
            print(f"ERROR: no user with email '{args.email}'", file=sys.stderr)
            return 3

        conn.commit()

    print(
        f"OK: reset password for {args.email} "
        f"(user_id={row.user_id}, tenant_id={row.tenant_id}, role={row.role})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
