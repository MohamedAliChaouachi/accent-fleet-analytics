import type { Config } from "tailwindcss";
import animate from "tailwindcss-animate";

// Design system for Accent Fleet Analytics.
//
// Colors are driven by CSS variables (HSL channels) declared in
// src/styles/index.css so that dark/light theme switching only flips the
// :root variables — no component needs theme-conditional classes.
//
// Token groups:
//   - brand:   navy (primary actions, nav surfaces)
//   - accent:  teal/cyan (CTAs, links, highlights)
//   - ai:      magenta/violet (AI-generated content indicators)
//   - risk:    semantic ramp (low/moderate/high/critical)
//   - surface: bg / card / elevated / muted (use as classes via Tailwind)
export default {
  darkMode: "class",
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    container: {
      center: true,
      padding: "1.5rem",
      screens: {
        "2xl": "1440px",
      },
    },
    extend: {
      colors: {
        border: "hsl(var(--border) / <alpha-value>)",
        input: "hsl(var(--input) / <alpha-value>)",
        ring: "hsl(var(--ring) / <alpha-value>)",
        background: "hsl(var(--background) / <alpha-value>)",
        foreground: "hsl(var(--foreground) / <alpha-value>)",
        primary: {
          DEFAULT: "hsl(var(--primary) / <alpha-value>)",
          foreground: "hsl(var(--primary-foreground) / <alpha-value>)",
        },
        secondary: {
          DEFAULT: "hsl(var(--secondary) / <alpha-value>)",
          foreground: "hsl(var(--secondary-foreground) / <alpha-value>)",
        },
        muted: {
          DEFAULT: "hsl(var(--muted) / <alpha-value>)",
          foreground: "hsl(var(--muted-foreground) / <alpha-value>)",
        },
        accent: {
          DEFAULT: "hsl(var(--accent) / <alpha-value>)",
          foreground: "hsl(var(--accent-foreground) / <alpha-value>)",
        },
        destructive: {
          DEFAULT: "hsl(var(--destructive) / <alpha-value>)",
          foreground: "hsl(var(--destructive-foreground) / <alpha-value>)",
        },
        popover: {
          DEFAULT: "hsl(var(--popover) / <alpha-value>)",
          foreground: "hsl(var(--popover-foreground) / <alpha-value>)",
        },
        card: {
          DEFAULT: "hsl(var(--card) / <alpha-value>)",
          foreground: "hsl(var(--card-foreground) / <alpha-value>)",
        },
        // Branded palette — kept as concrete hex aliases for chart fills,
        // gradients, and one-off styling outside the semantic token system.
        brand: {
          50: "#eef4fb",
          100: "#d6e4f3",
          200: "#aac6e6",
          300: "#7ea7d8",
          400: "#5189cb",
          500: "#2a6cb0",
          600: "#1f558b",
          700: "#1e3a5f",
          800: "#162a45",
          900: "#0e1c2d",
          950: "#070f19",
          DEFAULT: "hsl(var(--primary) / <alpha-value>)",
        },
        ai: {
          50: "#faf5ff",
          100: "#f1e5ff",
          200: "#e2cbff",
          300: "#cca6ff",
          400: "#a974f0",
          500: "#8b5cf6",
          600: "#7c3aed",
          700: "#6d28d9",
          800: "#5b21b6",
          900: "#4c1d95",
          DEFAULT: "hsl(var(--ai) / <alpha-value>)",
          foreground: "hsl(var(--ai-foreground) / <alpha-value>)",
          muted: "hsl(var(--ai-muted) / <alpha-value>)",
        },
        risk: {
          low: "hsl(var(--risk-low) / <alpha-value>)",
          moderate: "hsl(var(--risk-moderate) / <alpha-value>)",
          high: "hsl(var(--risk-high) / <alpha-value>)",
          critical: "hsl(var(--risk-critical) / <alpha-value>)",
        },
        success: "hsl(var(--success) / <alpha-value>)",
        warning: "hsl(var(--warning) / <alpha-value>)",
        info: "hsl(var(--info) / <alpha-value>)",
      },
      fontFamily: {
        sans: [
          "Inter",
          "ui-sans-serif",
          "system-ui",
          "-apple-system",
          "Segoe UI",
          "Roboto",
          "sans-serif",
        ],
        mono: [
          "JetBrains Mono",
          "ui-monospace",
          "SFMono-Regular",
          "Menlo",
          "Monaco",
          "Consolas",
          "monospace",
        ],
      },
      fontSize: {
        "2xs": ["0.6875rem", { lineHeight: "1rem" }],
      },
      spacing: {
        // 8px-based scale already covered by Tailwind defaults (1=4, 2=8, 3=12…).
        // Add named macro slots for shell chrome.
        sidebar: "16rem",
        "sidebar-collapsed": "4rem",
        topbar: "3.5rem",
      },
      borderRadius: {
        lg: "var(--radius)",
        md: "calc(var(--radius) - 2px)",
        sm: "calc(var(--radius) - 4px)",
      },
      boxShadow: {
        // Layered, low-saturation shadows that work on dark + light bg.
        card: "0 1px 2px 0 rgb(15 23 42 / 0.04), 0 1px 3px 0 rgb(15 23 42 / 0.06)",
        elevated:
          "0 4px 6px -1px rgb(15 23 42 / 0.08), 0 2px 4px -2px rgb(15 23 42 / 0.06)",
        glow: "0 0 0 1px hsl(var(--ring) / 0.4), 0 8px 24px -8px hsl(var(--primary) / 0.35)",
        "ai-glow":
          "0 0 0 1px hsl(var(--ai) / 0.35), 0 8px 24px -8px hsl(var(--ai) / 0.45)",
      },
      backgroundImage: {
        "gradient-ai":
          "linear-gradient(135deg, hsl(var(--ai)) 0%, hsl(var(--accent)) 100%)",
        "gradient-brand":
          "linear-gradient(135deg, hsl(var(--primary)) 0%, hsl(var(--accent)) 100%)",
        "gradient-surface":
          "linear-gradient(180deg, hsl(var(--card)) 0%, hsl(var(--background)) 100%)",
      },
      keyframes: {
        "accordion-down": {
          from: { height: "0" },
          to: { height: "var(--radix-accordion-content-height)" },
        },
        "accordion-up": {
          from: { height: "var(--radix-accordion-content-height)" },
          to: { height: "0" },
        },
        "fade-in": {
          from: { opacity: "0", transform: "translateY(4px)" },
          to: { opacity: "1", transform: "translateY(0)" },
        },
        "fade-in-fast": {
          from: { opacity: "0" },
          to: { opacity: "1" },
        },
        shimmer: {
          "0%": { backgroundPosition: "-200% 0" },
          "100%": { backgroundPosition: "200% 0" },
        },
        "pulse-soft": {
          "0%, 100%": { opacity: "1" },
          "50%": { opacity: "0.6" },
        },
        "caret-blink": {
          "0%, 70%, 100%": { opacity: "1" },
          "20%, 50%": { opacity: "0" },
        },
      },
      animation: {
        "accordion-down": "accordion-down 0.2s ease-out",
        "accordion-up": "accordion-up 0.2s ease-out",
        "fade-in": "fade-in 240ms cubic-bezier(0.16, 1, 0.3, 1) both",
        "fade-in-fast": "fade-in-fast 160ms ease-out both",
        shimmer: "shimmer 2s linear infinite",
        "pulse-soft": "pulse-soft 2s ease-in-out infinite",
        "caret-blink": "caret-blink 1.25s ease-in-out infinite",
      },
    },
  },
  plugins: [animate],
} satisfies Config;
