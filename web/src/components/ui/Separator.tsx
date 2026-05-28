import { forwardRef } from "react";
import * as RSeparator from "@radix-ui/react-separator";
import { cn } from "@/lib/cn";

export const Separator = forwardRef<
  React.ElementRef<typeof RSeparator.Root>,
  React.ComponentPropsWithoutRef<typeof RSeparator.Root>
>(
  (
    { className, orientation = "horizontal", decorative = true, ...props },
    ref,
  ) => (
    <RSeparator.Root
      ref={ref}
      decorative={decorative}
      orientation={orientation}
      className={cn(
        "shrink-0 bg-border",
        orientation === "horizontal" ? "h-px w-full" : "h-full w-px",
        className,
      )}
      {...props}
    />
  ),
);
Separator.displayName = RSeparator.Root.displayName;
