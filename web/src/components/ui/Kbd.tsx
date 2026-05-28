import type { HTMLAttributes } from "react";
import { cn } from "@/lib/cn";

// Inline keyboard key marker. Use to advertise shortcuts in the topbar
// search ("⌘K"), assistant input ("⌘↵"), etc.
export function Kbd({
  className,
  children,
  ...props
}: HTMLAttributes<HTMLElement>) {
  return (
    <kbd
      className={cn(
        "inline-flex h-5 min-w-5 items-center justify-center rounded border border-border bg-muted px-1 font-mono text-2xs font-medium text-muted-foreground",
        className,
      )}
      {...props}
    >
      {children}
    </kbd>
  );
}
