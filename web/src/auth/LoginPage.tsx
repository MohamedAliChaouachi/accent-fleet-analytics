import { useState, type FormEvent } from "react";
import { Navigate, useLocation, useNavigate } from "react-router-dom";
import { useAuth } from "./AuthContext";
import { ApiError } from "@/api/client";

export function LoginPage() {
  const { status, login } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  if (status === "authenticated") {
    const from = (location.state as { from?: { pathname?: string } } | null)?.from?.pathname;
    return <Navigate to={from ?? "/executive"} replace />;
  }

  async function onSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      await login(email, password);
      navigate("/executive", { replace: true });
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        setError("Invalid email or password.");
      } else if (err instanceof ApiError && err.status === 429) {
        setError("Too many attempts. Wait a few minutes and try again.");
      } else {
        setError("Login failed. Is the API reachable?");
      }
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="flex min-h-full items-center justify-center px-4 py-12">
      <form
        onSubmit={onSubmit}
        className="w-full max-w-sm rounded-lg bg-white p-8 shadow-md"
      >
        <h1 className="mb-1 text-xl font-semibold text-brand">Accent Fleet Analytics</h1>
        <p className="mb-6 text-sm text-slate-500">Sign in to continue.</p>

        <label className="mb-3 block">
          <span className="mb-1 block text-sm font-medium text-slate-700">Email</span>
          {/*
            type="text" + inputMode="email" rather than type="email" on
            purpose: seeded system identities use `.local` with underscored
            subdomains (e.g. admin@tenant_235.local) which the browser's
            built-in email validator rejects per RFC 1035, even though the
            API accepts them via the loose pattern in app/schemas/auth.py.
            inputMode keeps the mobile @-key hint without the strict check.
          */}
          <input
            type="text"
            inputMode="email"
            autoComplete="username"
            spellCheck={false}
            autoCapitalize="off"
            required
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            className="block w-full rounded-md border border-slate-300 px-3 py-2 text-sm focus:border-brand-accent focus:outline-none focus:ring-1 focus:ring-brand-accent"
          />
        </label>

        <label className="mb-4 block">
          <span className="mb-1 block text-sm font-medium text-slate-700">Password</span>
          <input
            type="password"
            autoComplete="current-password"
            required
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            className="block w-full rounded-md border border-slate-300 px-3 py-2 text-sm focus:border-brand-accent focus:outline-none focus:ring-1 focus:ring-brand-accent"
          />
        </label>

        {error ? (
          <p role="alert" className="mb-4 text-sm text-risk-critical">
            {error}
          </p>
        ) : null}

        <button
          type="submit"
          disabled={submitting}
          className="block w-full rounded-md bg-brand px-3 py-2 text-sm font-medium text-white hover:bg-brand-accent disabled:cursor-not-allowed disabled:opacity-60"
        >
          {submitting ? "Signing in…" : "Sign in"}
        </button>
      </form>
    </div>
  );
}
