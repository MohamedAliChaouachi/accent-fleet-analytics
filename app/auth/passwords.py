"""
Argon2id password hashing.

Wraps argon2-cffi with project-fixed parameters so the choice is
auditable in one place rather than scattered across call sites. The
parameters match `scripts/seed_auth.py` so a hash produced by either
path verifies through the other.

Threat model alignment (docs/auth_design.md §2 threat #6):
  - The hash includes salt + Argon2 parameters in its PHC string, so
    verification is self-contained. Even with a dumped users table, an
    attacker would still need to run Argon2id at our cost-per-attempt
    against each password guess.
  - The `verify()` wrapper raises `InvalidPasswordError` on every failure
    path (wrong password, malformed hash) so callers cannot accidentally
    leak which one it was. Login endpoints map both to the same generic
    "invalid credentials" response.
"""

from __future__ import annotations

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError

# Project-fixed Argon2id parameters; must match scripts/seed_auth.py.
_HASHER = PasswordHasher(
    time_cost=3,
    memory_cost=64 * 1024,  # KiB → 64 MiB
    parallelism=4,
    hash_len=32,
    salt_len=16,
)


class InvalidPasswordError(Exception):
    """Raised by `verify()` on any failure path."""


def hash_password(password: str) -> str:
    """Return the PHC-encoded Argon2id hash for `password`."""
    return _HASHER.hash(password)


def verify_password(password: str, stored_hash: str) -> None:
    """
    Verify `password` against the stored Argon2id hash.

    Raises `InvalidPasswordError` on any failure. Returns None on success
    (the caller didn't need the boolean; raising forces them to handle
    the error explicitly instead of falling through with truthy/falsy).
    """
    try:
        _HASHER.verify(stored_hash, password)
    except (VerifyMismatchError, Exception) as exc:  # pylint: disable=broad-except
        # argon2-cffi raises a few different exception classes
        # (InvalidHash, VerifyMismatchError, ...). All of them are
        # "this password did not authenticate" from our perspective.
        raise InvalidPasswordError(str(exc)) from exc


def needs_rehash(stored_hash: str) -> bool:
    """
    True if the stored hash's parameters drift from the current ones.

    The login flow can call this after a successful verify, recompute
    a fresh hash, and update the DB row — that's how we silently
    upgrade users to stronger parameters when we bump `_HASHER`.
    """
    return _HASHER.check_needs_rehash(stored_hash)
