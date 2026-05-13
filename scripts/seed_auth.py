#!/usr/bin/env python
"""
One-shot seed script for auth.users.

Creates one `tenant_admin` user per row in `auth.tenants` plus one global
`superadmin`. Generated passwords are printed to stdout ONCE — they are
not stored anywhere except the argon2id hash in `auth.users.password_hash`,
which is computationally infeasible to reverse.

The operator MUST capture the printed credentials before closing the
terminal (1Password, Bitwarden, paper, whatever the runbook says). After
the terminal scrolls away the only recovery path is
`POST /v1/admin/users/{id}/reset-password`, which will exist once M3
ships.

Idempotent semantics:
  - Skips tenant_admin seeding for any tenant that already has a user
    with role='tenant_admin'. No password printed in that case.
  - Skips superadmin seeding if any superadmin already exists. No
    password printed in that case.
  - Re-running the script is therefore safe but will appear to do
    nothing — that's deliberate. A second run cannot reset an existing
    user's password.

Usage:
    python scripts/seed_auth.py
    python scripts/seed_auth.py --password-length 24
    python scripts/seed_auth.py --superadmin-email admin@accent.local
    python scripts/seed_auth.py --dry-run

Rollback (M2):
    TRUNCATE auth.users CASCADE;
    -- and re-run this script.
"""

from __future__ import annotations

import argparse
import logging
import re
import secrets
import string
import sys
from pathlib import Path

# Make src/ importable when run as a plain script (mirrors run_batch.py).
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from argon2 import PasswordHasher
from sqlalchemy import text

from accent_fleet.db.engine import get_engine

# OWASP-recommended Argon2id parameters as of 2024:
#   time_cost=3, memory_cost=64 MiB, parallelism=4
# argon2-cffi's defaults are close to this; we make them explicit so future
# operators see the parameter contract directly in code rather than relying
# on a library's drift over versions.
HASHER = PasswordHasher(
    time_cost=3,
    memory_cost=64 * 1024,  # KiB → 64 MiB
    parallelism=4,
    hash_len=32,
    salt_len=16,
)

# Email validation kept minimal on purpose. The seed script writes local-
# only identifiers ("admin@tenant_5.local") and the API will validate
# user-provided emails through pydantic EmailStr in M3. A strict regex
# here would only reject our own conventions.
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def generate_password(length: int = 20) -> str:
    """
    Generate a high-entropy password using the system CSPRNG.

    Default 20 characters drawn from [A-Za-z0-9] + a small punctuation set
    gives ~119 bits of entropy — well past the OWASP threshold for a
    "strong machine-generated password". The punctuation set is restricted
    to characters that copy/paste reliably across terminals and password
    managers (no spaces, quotes, or backticks).
    """
    if length < 12:
        raise ValueError(
            "Refusing to generate a password shorter than 12 characters."
        )
    alphabet = string.ascii_letters + string.digits + "-_.!@#$%^&*+="
    return "".join(secrets.choice(alphabet) for _ in range(length))


def slug_for_tenant(display_name: str, tenant_id: int) -> str:
    """
    Build the local-part of the admin's email from the tenant's display
    name, falling back to `tenant_<id>` if the display name is unusable.

    Idempotency note: this is deterministic for a given (display_name,
    tenant_id), which means re-running the script will look for the same
    email address and the "skip existing" guard will fire.
    """
    safe = re.sub(r"[^a-z0-9]+", "_", display_name.lower()).strip("_")
    if not safe:
        safe = f"tenant_{tenant_id}"
    return safe


def seed(
    *,
    password_length: int,
    superadmin_email: str,
    dry_run: bool,
) -> int:
    if not _EMAIL_RE.match(superadmin_email):
        print(
            f"ERROR: --superadmin-email '{superadmin_email}' is not a "
            "valid email address.",
            file=sys.stderr,
        )
        return 2

    created: list[tuple[str, str, str]] = []  # (role, email, password)
    skipped: list[tuple[str, str]] = []        # (role, email)

    engine = get_engine()
    # One transaction per seeding action keeps a partial failure (e.g.
    # the third tenant's INSERT raises) from leaving an inconsistent set
    # of half-seeded users behind. Each successful row commits standalone.
    with engine.connect() as conn:
        # --- Tenants → tenant_admin ----------------------------------
        tenants = conn.execute(
            text(
                "SELECT tenant_id, display_name "
                "FROM auth.tenants "
                "WHERE is_active = TRUE "
                "ORDER BY tenant_id"
            )
        ).all()

        if not tenants:
            print(
                "WARN: auth.tenants is empty. Run sql/50_auth.sql first "
                "to backfill from warehouse.dim_tenant.",
                file=sys.stderr,
            )

        for tenant_id, display_name in tenants:
            email = f"admin@{slug_for_tenant(display_name, tenant_id)}.local"

            existing_admin = conn.execute(
                text(
                    "SELECT 1 FROM auth.users "
                    "WHERE tenant_id = :tid AND role = 'tenant_admin' "
                    "LIMIT 1"
                ),
                {"tid": tenant_id},
            ).scalar()
            if existing_admin:
                skipped.append(("tenant_admin", f"tenant_id={tenant_id}"))
                continue

            password = generate_password(password_length)
            password_hash = HASHER.hash(password)

            if dry_run:
                created.append(("tenant_admin", email, "<dry-run>"))
                continue

            with conn.begin():
                conn.execute(
                    text(
                        "INSERT INTO auth.users "
                        "  (tenant_id, email, password_hash, role) "
                        "VALUES (:tid, :email, :hash, 'tenant_admin')"
                    ),
                    {
                        "tid": tenant_id,
                        "email": email,
                        "hash": password_hash,
                    },
                )
            created.append(("tenant_admin", email, password))

        # --- Superadmin --------------------------------------------------
        existing_super = conn.execute(
            text(
                "SELECT 1 FROM auth.users WHERE role = 'superadmin' LIMIT 1"
            )
        ).scalar()
        if existing_super:
            skipped.append(("superadmin", "(at least one already exists)"))
        else:
            password = generate_password(password_length)
            password_hash = HASHER.hash(password)
            if dry_run:
                created.append(("superadmin", superadmin_email, "<dry-run>"))
            else:
                with conn.begin():
                    conn.execute(
                        text(
                            "INSERT INTO auth.users "
                            "  (tenant_id, email, password_hash, role) "
                            "VALUES (NULL, :email, :hash, 'superadmin')"
                        ),
                        {"email": superadmin_email, "hash": password_hash},
                    )
                created.append(("superadmin", superadmin_email, password))

    # --- Report --------------------------------------------------------
    # Print credentials to stdout exactly once. The leading banner makes
    # this section easy to spot in scrolled output, and the trailing
    # banner reminds the operator to capture them before they scroll off.
    if created:
        print()
        print("=" * 72)
        print(" SEEDED CREDENTIALS — CAPTURE THESE NOW")
        print(" (these passwords are NOT stored anywhere recoverable)")
        print("=" * 72)
        for role, email, password in created:
            print(f"  role={role:<14}  email={email}")
            print(f"  password={password}")
            print("-" * 72)
        print(
            " Store these in your password manager BEFORE closing this "
            "terminal."
        )
        print("=" * 72)
        print()

    if skipped:
        print("Skipped (already exists):")
        for role, what in skipped:
            print(f"  - {role}: {what}")

    if not created and not skipped:
        print("Nothing to do — auth.tenants is empty and no superadmin needed.")

    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--password-length",
        type=int,
        default=20,
        help="Length of generated passwords (>=12). Default 20.",
    )
    parser.add_argument(
        "--superadmin-email",
        default="superadmin@accent.local",
        help="Email for the global superadmin user.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Hash and print but don't INSERT. Useful for sanity-checking.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )

    return seed(
        password_length=args.password_length,
        superadmin_email=args.superadmin_email,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    raise SystemExit(main())
