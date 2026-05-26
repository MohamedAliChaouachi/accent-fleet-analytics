import type { Config } from "tailwindcss";

// Risk-tier palette — shared across KPI cards, chart legends, and the
// device-level risk badge so every surface labels the same row the same
// color.
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        risk: {
          low: "#2ecc71",
          moderate: "#f1c40f",
          high: "#e67e22",
          critical: "#e74c3c",
        },
        brand: {
          DEFAULT: "#1f3a5f",
          accent: "#2a9df4",
        },
      },
    },
  },
  plugins: [],
} satisfies Config;
