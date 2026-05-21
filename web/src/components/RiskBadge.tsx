import { RISK_COLORS } from "@/lib/colors";

type Size = "sm" | "md" | "lg";

interface RiskBadgeProps {
  category: "low" | "moderate" | "high" | "critical" | string | null | undefined;
  size?: Size;
}

const SIZE_CLASS: Record<Size, string> = {
  sm: "px-2 py-0.5 text-[11px]",
  md: "px-2.5 py-1 text-xs",
  lg: "px-3 py-1.5 text-sm",
};

export function RiskBadge({ category, size = "sm" }: RiskBadgeProps) {
  const key = (category ?? "low") as string;
  const color = RISK_COLORS[key] ?? "#94a3b8";
  return (
    <span
      className={`inline-flex items-center rounded-full font-semibold uppercase tracking-wide text-white ${SIZE_CLASS[size]}`}
      style={{ backgroundColor: color }}
    >
      {category ?? "\u2014"}
    </span>
  );
}
