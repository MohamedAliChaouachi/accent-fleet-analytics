import { forwardRef, type HTMLAttributes } from "react";
import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/cn";

// Semantic badge with explicit risk variants so consumers don't have to
// re-derive risk color → badge style mapping in every dashboard.
const badgeVariants = cva(
  "inline-flex items-center gap-1 rounded-full border px-2.5 py-0.5 text-2xs font-medium uppercase tracking-wide transition-colors",
  {
    variants: {
      variant: {
        default: "border-border bg-secondary text-secondary-foreground",
        outline: "border-border bg-transparent text-foreground",
        primary: "border-primary/30 bg-primary/10 text-primary",
        accent: "border-accent/30 bg-accent/10 text-accent",
        ai: "border-ai/30 bg-ai/10 text-ai",
        success: "border-success/30 bg-success/10 text-success",
        warning: "border-warning/30 bg-warning/10 text-warning",
        destructive:
          "border-destructive/30 bg-destructive/10 text-destructive",
        "risk-low": "border-risk-low/30 bg-risk-low/10 text-risk-low",
        "risk-moderate":
          "border-risk-moderate/30 bg-risk-moderate/10 text-risk-moderate",
        "risk-high": "border-risk-high/30 bg-risk-high/10 text-risk-high",
        "risk-critical":
          "border-risk-critical/30 bg-risk-critical/10 text-risk-critical",
      },
    },
    defaultVariants: { variant: "default" },
  },
);

export interface BadgeProps
  extends HTMLAttributes<HTMLSpanElement>,
    VariantProps<typeof badgeVariants> {}

export const Badge = forwardRef<HTMLSpanElement, BadgeProps>(
  ({ className, variant, ...props }, ref) => (
    <span
      ref={ref}
      className={cn(badgeVariants({ variant }), className)}
      {...props}
    />
  ),
);
Badge.displayName = "Badge";

export { badgeVariants };
