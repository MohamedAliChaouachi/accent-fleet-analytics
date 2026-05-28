import { TrendingUp } from "lucide-react";
import { Badge, type BadgeProps } from "@/components/ui/Badge";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/Tooltip";

export type Confidence = "high" | "medium" | "low";

const COPY: Record<Confidence, { label: string; explainer: string; variant: BadgeProps["variant"] }> = {
  high: {
    label: "High confidence",
    explainer:
      "The model found a direct match for your question in the schema and the query passed safety checks.",
    variant: "success",
  },
  medium: {
    label: "Medium confidence",
    explainer:
      "The model made some inferences. Verify the result against your domain knowledge before sharing.",
    variant: "warning",
  },
  low: {
    label: "Low confidence",
    explainer:
      "The model had limited signal and may have guessed. Edit the SQL or rephrase the question if results look off.",
    variant: "destructive",
  },
};

interface ConfidenceBadgeProps {
  confidence: Confidence;
  /** When true (default) the badge gets a tooltip with the explainer text. */
  explain?: boolean;
}

// Visual indicator of how confident the model is in the generated SQL.
// Backend doesn't expose a score yet — until it does, deriveConfidence()
// below produces a heuristic from the response shape (row count, summary
// length). When the API grows a real `confidence` field, the consumer
// just passes it in unchanged.
export function ConfidenceBadge({
  confidence,
  explain = true,
}: ConfidenceBadgeProps) {
  const { label, explainer, variant } = COPY[confidence];

  const badge = (
    <Badge variant={variant} className="gap-1">
      <TrendingUp className="size-3" />
      {label}
    </Badge>
  );

  if (!explain) return badge;
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button type="button" className="rounded-full">
          {badge}
        </button>
      </TooltipTrigger>
      <TooltipContent className="max-w-xs">{explainer}</TooltipContent>
    </Tooltip>
  );
}

/** Heuristic confidence until the backend exposes a real score.
 * - Empty / 1-row result with a short summary → low
 * - Reasonable row count and a coherent summary → high
 * - Anything in between → medium */
export function deriveConfidence(opts: {
  rowCount: number;
  summaryLength: number;
}): Confidence {
  const { rowCount, summaryLength } = opts;
  if (rowCount === 0 || summaryLength < 20) return "low";
  if (rowCount >= 3 && summaryLength >= 60) return "high";
  return "medium";
}
