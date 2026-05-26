import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { login as apiLogin, logout as apiLogout, me as apiMe, refresh as apiRefresh } from "@/api/auth";
import { registerAuthBindings } from "@/api/client";
import type { MeResponse, TokenPair } from "@/api/types";

const REFRESH_TOKEN_STORAGE_KEY = "accent.refresh_token";

// Schedule the next refresh this many ms before declared expiry.
const REFRESH_CUSHION_MS = 30_000;

interface AuthState {
  accessToken: string | null;
  expiresAt: number | null;
  user: MeResponse | null;
}

interface AuthContextValue extends AuthState {
  status: "initializing" | "anonymous" | "authenticated";
  login: (email: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<AuthState>({
    accessToken: null,
    expiresAt: null,
    user: null,
  });
  const [status, setStatus] = useState<AuthContextValue["status"]>("initializing");

  // Stash the access token in a ref so the API client gets the latest
  // value on every request without re-binding the fetch function.
  const accessTokenRef = useRef<string | null>(null);
  const refreshTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const clearTimer = useCallback(() => {
    if (refreshTimerRef.current) {
      clearTimeout(refreshTimerRef.current);
      refreshTimerRef.current = null;
    }
  }, []);

  const clearAuth = useCallback(() => {
    clearTimer();
    accessTokenRef.current = null;
    localStorage.removeItem(REFRESH_TOKEN_STORAGE_KEY);
    setState({ accessToken: null, expiresAt: null, user: null });
    setStatus("anonymous");
  }, [clearTimer]);

  // Forward declaration so applyTokens can reference scheduleRefresh.
  const scheduleRefreshRef = useRef<(expiresAt: number) => void>(() => {});

  const applyTokens = useCallback(
    async (pair: TokenPair, opts: { fetchUser?: boolean } = {}) => {
      accessTokenRef.current = pair.access_token;
      // Persist refresh token in localStorage. v1 trade-off documented
      // in the migration plan: XSS-vulnerable but simple; an httpOnly
      // cookie set by an nginx-side proxy is a follow-up.
      localStorage.setItem(REFRESH_TOKEN_STORAGE_KEY, pair.refresh_token);

      let user = state.user;
      if (opts.fetchUser || !user) {
        try {
          user = await apiMe();
        } catch {
          // /auth/me failure with a fresh token is unexpected — fall
          // back to a minimal record so the user can still get past
          // the gate. They'll see "—" in the header until the next
          // navigation refetches.
          user = null;
        }
      }
      setState({
        accessToken: pair.access_token,
        expiresAt: pair.expires_at,
        user,
      });
      setStatus("authenticated");
      scheduleRefreshRef.current(pair.expires_at);
    },
    [state.user],
  );

  const doRefresh = useCallback(async () => {
    const stored = localStorage.getItem(REFRESH_TOKEN_STORAGE_KEY);
    if (!stored) {
      clearAuth();
      return;
    }
    try {
      const pair = await apiRefresh(stored);
      await applyTokens(pair);
    } catch {
      clearAuth();
    }
  }, [applyTokens, clearAuth]);

  const scheduleRefresh = useCallback(
    (expiresAt: number) => {
      clearTimer();
      const delay = Math.max(0, expiresAt * 1000 - Date.now() - REFRESH_CUSHION_MS);
      refreshTimerRef.current = setTimeout(() => {
        void doRefresh();
      }, delay);
    },
    [clearTimer, doRefresh],
  );

  // Keep the ref in sync so applyTokens (which is defined before
  // scheduleRefresh) can still call it without a circular dep.
  scheduleRefreshRef.current = scheduleRefresh;

  // Bootstrap on mount: try the stored refresh token, fall through to
  // anonymous on any failure.
  useEffect(() => {
    let cancelled = false;
    void (async () => {
      const stored = localStorage.getItem(REFRESH_TOKEN_STORAGE_KEY);
      if (!stored) {
        if (!cancelled) setStatus("anonymous");
        return;
      }
      try {
        const pair = await apiRefresh(stored);
        if (cancelled) return;
        await applyTokens(pair, { fetchUser: true });
      } catch {
        if (!cancelled) clearAuth();
      }
    })();
    return () => {
      cancelled = true;
      clearTimer();
    };
    // applyTokens / clearAuth are stable enough — running this exactly once
    // on mount is the desired behaviour.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Bridge to the imperative fetch client.
  useEffect(() => {
    registerAuthBindings({
      getAccessToken: () => accessTokenRef.current,
      onUnauthorized: () => {
        // Any 401 from the API means the token is no longer accepted.
        // Drop state and let RequireAuth bounce to /login.
        clearAuth();
      },
    });
  }, [clearAuth]);

  const login = useCallback(
    async (email: string, password: string) => {
      const pair = await apiLogin(email, password);
      await applyTokens(pair, { fetchUser: true });
    },
    [applyTokens],
  );

  const logout = useCallback(async () => {
    const stored = localStorage.getItem(REFRESH_TOKEN_STORAGE_KEY);
    if (stored) {
      try {
        await apiLogout(stored);
      } catch {
        // Server-side revocation failed; clear locally anyway so the
        // user isn't stuck logged in if e.g. the server is unreachable.
      }
    }
    clearAuth();
  }, [clearAuth]);

  const value = useMemo<AuthContextValue>(
    () => ({ ...state, status, login, logout }),
    [state, status, login, logout],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used inside <AuthProvider>");
  return ctx;
}
