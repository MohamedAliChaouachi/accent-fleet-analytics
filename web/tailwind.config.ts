import type { Config } from "tailwindcss";

// Risk-tier palette mirrors RISK_COLORS in dashboard/lib/theme.py so
// the React app and the Streamlit page render the same colors during
// the side-by-side migration window.
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
