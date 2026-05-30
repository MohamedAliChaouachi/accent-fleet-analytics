import { useState, type FormEvent } from "react";
import { Navigate, useLocation, useNavigate } from "react-router-dom";
import { motion } from "framer-motion";
import {
  ArrowRight,
  BarChart3,
  Eye,
  EyeOff,
  Loader2,
  Lock,
  Mail,
  ShieldCheck,
  Truck,
} from "lucide-react";
import { useAuth } from "./AuthContext";
import { ApiError } from "@/api/client";

// Iconic Accent corner-triangle lime — the one off-palette accent we
// keep, since it's load-bearing for brand recognition.
const ACCENT_LIME = "#c4d62e";

function AccentMark({ className = "" }: { className?: string }) {
  return (
    <svg viewBox="0 0 48 48" className={className} fill="none" aria-hidden="true">
      <path d="M10 42 L24 6 L38 42 L30.5 42 L24 23 L17.5 42 Z" fill="currentColor" />
      <path d="M29 6 L42 6 L35.5 18.5 Z" fill={ACCENT_LIME} />
    </svg>
  );
}

function Wordmark({ className = "" }: { className?: string }) {
  return (
    <span className={`text-2xl font-extrabold tracking-tight ${className}`}>
      accent
      <span
        className="ml-0.5 inline-block h-2 w-2 rounded-[2px] align-baseline"
        style={{ backgroundColor: ACCENT_LIME }}
      />
    </span>
  );
}

const FEATURES = [
  { icon: BarChart3, label: "Real-time fleet performance dashboards" },
  { icon: Truck, label: "Complete vehicle lifecycle tracking" },
  { icon: ShieldCheck, label: "Secure, role-based tenant access" },
];

export function LoginPage() {
  const { status, login } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
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

  const inputClass =
    "block w-full rounded-lg border border-input bg-background/60 py-2.5 pl-10 pr-3 text-sm text-foreground placeholder:text-muted-foreground/60 transition focus:border-accent focus:outline-none focus:ring-2 focus:ring-accent/40";

  return (
    <div className="relative grid min-h-full overflow-hidden bg-background lg:grid-cols-2">
      {/* Ambient gradient blobs */}
      <div className="pointer-events-none absolute -left-32 -top-32 h-96 w-96 rounded-full bg-primary/20 blur-3xl" />
      <div className="pointer-events-none absolute -bottom-40 right-0 h-96 w-96 rounded-full bg-accent/15 blur-3xl" />

      {/* Branded panel */}
      <div className="relative hidden flex-col justify-between overflow-hidden bg-gradient-brand p-12 text-primary-foreground lg:flex">
        <div
          className="pointer-events-none absolute -right-24 -top-24 h-80 w-80 rounded-full opacity-20"
          style={{ backgroundColor: ACCENT_LIME }}
        />
        <div
          className="pointer-events-none absolute -left-16 bottom-10 h-64 w-64 rounded-full opacity-10"
          style={{ backgroundColor: ACCENT_LIME }}
        />

        <div className="relative z-10 flex items-center gap-2.5">
          <AccentMark className="h-10 w-10 text-white" />
          <Wordmark className="text-white" />
        </div>

        <motion.div
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5, ease: [0.16, 1, 0.3, 1] }}
          className="relative z-10 max-w-md"
        >
          <h1 className="text-4xl font-extrabold leading-tight">
            Fleet Analytics,
            <br />
            <span style={{ color: ACCENT_LIME }}>driven by data.</span>
          </h1>
          <p className="mt-4 text-lg text-primary-foreground/70">
            Monitor your fleet, control costs, and make smarter decisions in real time.
          </p>

          <div className="mt-10 space-y-4">
            {FEATURES.map(({ icon: Icon, label }, i) => (
              <motion.div
                key={label}
                initial={{ opacity: 0, x: -8 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ duration: 0.4, delay: 0.15 + i * 0.08, ease: "easeOut" }}
                className="flex items-center gap-3"
              >
                <div
                  className="flex h-9 w-9 items-center justify-center rounded-lg"
                  style={{ backgroundColor: "rgba(196,214,46,0.15)" }}
                >
                  <Icon className="h-5 w-5" style={{ color: ACCENT_LIME }} />
                </div>
                <span className="text-primary-foreground/90">{label}</span>
              </motion.div>
            ))}
          </div>
        </motion.div>

        <p className="relative z-10 text-sm text-primary-foreground/60">
          © {new Date().getFullYear()} Accent Tunisie. All rights reserved.
        </p>
      </div>

      {/* Form panel */}
      <div className="relative z-10 flex items-center justify-center px-4 py-12 sm:px-12">
        <motion.div
          initial={{ opacity: 0, y: 16 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.45, ease: [0.16, 1, 0.3, 1] }}
          className="w-full max-w-sm"
        >
          <div className="mb-8 flex justify-center lg:hidden">
            <div className="flex items-center gap-2.5 text-primary">
              <AccentMark className="h-9 w-9" />
              <Wordmark className="text-foreground" />
            </div>
          </div>

          <div className="rounded-2xl border border-border/60 bg-card/80 p-8 shadow-elevated backdrop-blur-xl">
            <div className="mb-6">
              <h2 className="text-2xl font-bold text-foreground">Welcome back</h2>
              <p className="mt-1 text-sm text-muted-foreground">
                Sign in to access your dashboard.
              </p>
            </div>

            <form onSubmit={onSubmit} className="space-y-5">
              <div>
                <label
                  htmlFor="email"
                  className="mb-1.5 block text-sm font-medium text-foreground"
                >
                  Email
                </label>
                {/*
                  type="text" + inputMode="email" rather than type="email" on
                  purpose: seeded system identities use `.local` with underscored
                  subdomains (e.g. admin@tenant_235.local) which the browser's
                  built-in email validator rejects per RFC 1035, even though the
                  API accepts them via the loose pattern in app/schemas/auth.py.
                  inputMode keeps the mobile @-key hint without the strict check.
                */}
                <div className="relative">
                  <Mail className="pointer-events-none absolute left-3 top-1/2 h-5 w-5 -translate-y-1/2 text-muted-foreground" />
                  <input
                    id="email"
                    type="text"
                    inputMode="email"
                    autoComplete="username"
                    spellCheck={false}
                    autoCapitalize="off"
                    required
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    placeholder="you@accent.tn"
                    className={inputClass}
                  />
                </div>
              </div>

              <div>
                <label
                  htmlFor="password"
                  className="mb-1.5 block text-sm font-medium text-foreground"
                >
                  Password
                </label>
                <div className="relative">
                  <Lock className="pointer-events-none absolute left-3 top-1/2 h-5 w-5 -translate-y-1/2 text-muted-foreground" />
                  <input
                    id="password"
                    type={showPassword ? "text" : "password"}
                    autoComplete="current-password"
                    required
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    placeholder="••••••••"
                    className={`${inputClass} pr-10`}
                  />
                  <button
                    type="button"
                    onClick={() => setShowPassword((s) => !s)}
                    className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground transition hover:text-foreground"
                    aria-label={showPassword ? "Hide password" : "Show password"}
                  >
                    {showPassword ? <EyeOff className="h-5 w-5" /> : <Eye className="h-5 w-5" />}
                  </button>
                </div>
              </div>

              {error ? (
                <motion.p
                  initial={{ opacity: 0, y: -4 }}
                  animate={{ opacity: 1, y: 0 }}
                  role="alert"
                  className="rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive"
                >
                  {error}
                </motion.p>
              ) : null}

              <button
                type="submit"
                disabled={submitting}
                className="group flex w-full items-center justify-center gap-2 rounded-lg bg-primary px-3 py-2.5 text-sm font-semibold text-primary-foreground shadow-glow transition hover:opacity-95 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {submitting ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin" />
                    Signing in…
                  </>
                ) : (
                  <>
                    Sign in
                    <ArrowRight className="h-4 w-4 transition-transform group-hover:translate-x-0.5" />
                  </>
                )}
              </button>
            </form>
          </div>

          <p className="mt-6 text-center text-xs text-muted-foreground">
            Accent Fleet Analytics · Secure tenant access
          </p>
        </motion.div>
      </div>
    </div>
  );
}
