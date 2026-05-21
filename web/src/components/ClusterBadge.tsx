import { clusterPersona } from "@/lib/clusters";

type Size = "sm" | "md" | "lg";

interface ClusterBadgeProps {
  clusterId: number | null | undefined;
  size?: Size;
  showName?: boolean;
}

const SIZE_CLASS: Record<Size, string> = {
  sm: "px-2 py-0.5 text-[11px] gap-1",
  md: "px-2.5 py-1 text-xs gap-1.5",
  lg: "px-3 py-1.5 text-sm gap-2",
};

export function ClusterBadge({ clusterId, size = "sm", showName = true }: ClusterBadgeProps) {
  const persona = clusterPersona(clusterId);
  return (
    <span
      className={`inline-flex items-center rounded-full font-medium ring-1 ring-inset ${SIZE_CLASS[size]}`}
      style={{
        color: persona.color,
        backgroundColor: `${persona.color}14`,
        boxShadow: `inset 0 0 0 1px ${persona.color}33`,
      }}
      title={persona.description}
    >
      <span aria-hidden>{persona.icon}</span>
      {showName ? <span className="truncate">{persona.short}</span> : null}
    </span>
  );
}
