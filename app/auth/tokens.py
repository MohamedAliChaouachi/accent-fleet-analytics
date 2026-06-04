"""
JWT issuance + verification.

Access tokens are HS256-signed JWTs (15 min TTL by default). Refresh
tokens are opaque UUIDs whose state lives in `auth.refresh_tokens` —
this module only mints and verifies the JWT half.

Dual-key rotation:
  - `JWT_SIGNING_KEY` signs every newly-issued token.
  - During a rotation window the operator sets
    `JWT_SIGNING_KEY_PREVIOUS` to the outgoing key. Verification tries
    the current key first, then the previous. New tokens are never
    signed with the previous key — so once existing tokens expire the
    operator can drop `JWT_SIGNING_KEY_PREVIOUS` and the window closes.

The token shape is OIDC-compatible (`sub`, `iat`, `exp`, plus custom
`tnt`/`role` claims) so a later swap to Azure AD or any other OIDC IdP
is a question of changing the issuer, not the consumer.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Any

import jwt
from jwt.exceptions import PyJWTError

from accent_fleet.config import settings
from app.auth.principal import Principal, Role

ALGORITHM = "HS256"


class TokenError(Exception):
    """Base class for any token verification failure."""


class TokenExpiredError(TokenError):
    """The token signature checked out but `exp` is in the past."""


class TokenInvalidError(TokenError):
    """Signature mismatch, malformed JWT, missing claims, etc."""


class SigningKeyMissingError(RuntimeError):
    """
    Raised when settings.jwt_signing_key is empty.

    The middleware's `enforce` mode treats this as a fatal startup
    error — we'd rather refuse to start than accept requests with a
    silently-misconfigured signing key.
    """


@dataclass(frozen=True, slots=True)
class IssuedTokens:
    access_token: str
    expires_at: int          # unix-epoch seconds
    refresh_token_id: str    # UUID string; the opaque value handed to client


# Return the configured signing key, or raise if it is unset/empty.
def _require_signing_key() -> str:
    key = settings().jwt_signing_key
    if not key:
        raise SigningKeyMissingError(
            "JWT_SIGNING_KEY is not set. Refusing to issue or verify "
            "tokens with an empty key."
        )
    return key


def issue_access_token(principal: Principal) -> tuple[str, int]:
    """
    Return (token, expires_at_epoch_seconds) for `principal`.

    Includes `jti` so a future revocation list can blacklist a specific
    access token if we ever need to (e.g. a known-leaked one). At
    v0.9.0 we don't read `jti` on verify — it's reserved for forward
    compatibility, costs nothing now, and would be a breaking change
    to add later.
    """
    # Compute issue/expiry timestamps from the configured access TTL.
    s = settings()
    key = _require_signing_key()
    now = int(time.time())
    exp = now + int(s.jwt_access_ttl_seconds)
    # Assemble the OIDC-compatible claim set, including a unique jti.
    payload: dict[str, Any] = {
        "sub": str(principal.user_id),
        "tnt": principal.tenant_id,  # None for superadmin; OK in JSON
        "role": principal.role,
        "email": principal.email,
        "iat": now,
        "exp": exp,
        "jti": uuid.uuid4().hex,
    }
    # Sign the payload with the current key and return token + expiry.
    token = jwt.encode(payload, key, algorithm=ALGORITHM)
    return token, exp


def issue_refresh_token_id() -> str:
    """
    Return a fresh refresh-token UUID.

    The opaque UUID is what the client stores; the server-side row in
    `auth.refresh_tokens` is what makes logout actually revoke. We
    intentionally do NOT mint a JWT here — there's no benefit when the
    client treats the value as opaque, and a stateless refresh token
    would defeat the whole "logout revokes" property (threat #7).
    """
    return str(uuid.uuid4())


def verify_access_token(token: str) -> dict[str, Any]:
    """
    Return the decoded payload dict for a valid access token.

    Tries the current signing key first, then the previous one (if
    set) for the rotation window. Raises `TokenExpiredError` on `exp` in
    the past, `TokenInvalidError` on everything else.
    """
    # Build the candidate key list: current first, then previous if rotating.
    s = settings()
    current = _require_signing_key()
    keys = [current]
    if s.jwt_signing_key_previous:
        keys.append(s.jwt_signing_key_previous)

    # Try each key until one decodes; track the last failure for reporting.
    last_error: Exception | None = None
    for key in keys:
        try:
            return jwt.decode(
                token,
                key,
                algorithms=[ALGORITHM],
                # PyJWT treats `null` claim values as "missing" when listed
                # in `require`, which would reject superadmin tokens whose
                # `tnt` is legitimately null. We validate `tnt` ourselves in
                # `principal_from_payload` instead — it accepts null for
                # superadmin and rejects null for other roles.
                options={"require": ["sub", "exp", "role"]},
            )
        except jwt.ExpiredSignatureError as exc:
            # Expiry is independent of which key signed — short-circuit
            # immediately instead of trying the previous key.
            raise TokenExpiredError(str(exc)) from exc
        except PyJWTError as exc:
            last_error = exc
            continue

    # No key accepted the token: surface it as an invalid-token error.
    raise TokenInvalidError(str(last_error) if last_error else "token rejected")


def principal_from_payload(payload: dict[str, Any]) -> Principal:
    """
    Build a Principal from a verified JWT payload.

    Raises `TokenInvalidError` if the claims are present but inconsistent
    (e.g. role=tenant_user but tnt is None) — the verify step alone
    can't catch that because pyjwt doesn't know our invariants.
    """
    # Extract and coerce the claims; any shape error is a malformed token.
    try:
        user_id = int(payload["sub"])
        tenant_id_raw = payload.get("tnt")
        tenant_id = int(tenant_id_raw) if tenant_id_raw is not None else None
        role: Role = payload["role"]
        email = payload.get("email", "")
    except (KeyError, TypeError, ValueError) as exc:
        raise TokenInvalidError(f"malformed claims: {exc}") from exc

    # Construct the Principal; its invariant check may reject the claims.
    try:
        return Principal(
            user_id=user_id,
            tenant_id=tenant_id,
            role=role,
            email=email,
        )
    except ValueError as exc:
        # Principal.__post_init__ enforces the (role, tenant_id) bijection.
        raise TokenInvalidError(str(exc)) from exc
