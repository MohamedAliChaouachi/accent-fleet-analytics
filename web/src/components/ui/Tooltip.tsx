import { forwardRef } from "react";
import * as RTooltip from "@radix-ui/react-tooltip";
import { cn } from "@/lib/cn";

// Wrap your app once in <TooltipProvider> (done in main.tsx) so all
// tooltips share a single delay timer.
export const TooltipProvider = RTooltip.Provider;
export const Tooltip = RTooltip.Root;
export const TooltipTrigger = RTooltip.Trigger;

export const TooltipContent = forwardRef<
  React.ElementRef<typeof RTooltip.Content>,
  React.ComponentPropsWithoutRef<typeof RTooltip.Content>
>(({ className, sideOffset = 6, ...props }, ref) => (
  <RTooltip.Portal>
    <RTooltip.Content
      ref={ref}
      sideOffset={sideOffset}
      className={cn(
        "z-50 overflow-hidden rounded-md border border-border bg-popover px-2.5 py-1.5 text-xs text-popover-foreground shadow-elevated",
        "data-[state=delayed-open]:animate-fade-in-fast",
        className,
      )}
      {...props}
    />
  </RTooltip.Portal>
));
TooltipContent.displayName = RTooltip.Content.displayName;
