// Thin fetch wrapper used by every typed API module.
//
// Auth model: the access token lives in AuthContext state. We can't import
// the context from a non-React module, so the context registers a getter
// here on mount. Each request reads the current token via the getter, so
// background refreshes are picked up automatically without re-binding the
// fetch function.
//
// On a 401 we invoke the unauthorized handler (also registered by the
// AuthContext) so the user is bounced to /login. We deliberately don't
// retry-with-refresh here — the refresh timer in AuthContext fires 30s
// before expiry, so a 401 in practice means the token was revoked or the
// server clock skewed, neither of which a retry would fix.

const API_BASE_URL: string = import.meta.env.VITE_API_BASE_URL ?? "/v1";

let getAccessToken: () => string | null = () => null;
let onUnauthorized: () => void = () => {};

export function registerAuthBindings(opts: {
  getAccessToken: () => string | null;
  onUnauthorized: () => void;
}): void {
  getAccessToken = opts.getAccessToken;
  onUnauthorized = opts.onUnauthorized;
}

export class ApiError extends Error {
  status: number;
  body: unknown;
  constructor(status: number, message: string, body: unknown) {
    super(message);
    this.status = status;
    this.body = body;
  }
}

interface RequestOptions {
  method?: "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
  body?: unknown;
  // Query params; numbers/strings/booleans only. Arrays repeat the key.
  query?: Record<string, string | number | boolean | Array<string | number> | null | undefined>;
  // Skip the Authorization header (login, refresh).
  anonymous?: boolean;
  signal?: AbortSignal;
}

function buildUrl(path: string, query?: RequestOptions["query"]): string {
  const url = new URL(`${API_BASE_URL}${path}`, window.location.origin);
  if (query) {
    for (const [key, value] of Object.entries(query)) {
      if (value === null || value === undefined) continue;
      if (Array.isArray(value)) {
        for (const v of value) url.searchParams.append(key, String(v));
      } else {
        url.searchParams.set(key, String(value));
      }
    }
  }
  return url.toString();
}

export async function request<T>(path: string, opts: RequestOptions = {}): Promise<T> {
  const headers: Record<string, string> = { Accept: "application/json" };
  if (!opts.anonymous) {
    const token = getAccessToken();
    if (token) headers.Authorization = `Bearer ${token}`;
  }
  if (opts.body !== undefined) headers["Content-Type"] = "application/json";

  const res = await fetch(buildUrl(path, opts.query), {
    method: opts.method ?? "GET",
    headers,
    body: opts.body !== undefined ? JSON.stringify(opts.body) : undefined,
    signal: opts.signal,
  });

  if (res.status === 401 && !opts.anonymous) {
    onUnauthorized();
  }

  if (res.status === 204) {
    return undefined as T;
  }

  const text = await res.text();
  const body: unknown = text ? safeJson(text) : null;
  if (!res.ok) {
    const detail =
      (body && typeof body === "object" && "detail" in body && typeof (body as { detail: unknown }).detail === "string"
        ? (body as { detail: string }).detail
        : null) ?? res.statusText;
    throw new ApiError(res.status, detail, body);
  }
  return body as T;
}

function safeJson(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}
