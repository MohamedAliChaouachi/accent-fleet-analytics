import { forwardRef, type InputHTMLAttributes, type ReactNode } from "react";
import { cn } from "@/lib/cn";

export interface InputProps extends InputHTMLAttributes<HTMLInputElement> {
  // Slot for a leading icon (search glass, calendar, etc).
  leadingIcon?: ReactNode;
  // Slot for trailing content (keyboard shortcut hint, clear button).
  trailing?: ReactNode;
}

export const Input = forwardRef<HTMLInputElement, InputProps>(
  ({ className, leadingIcon, trailing, type = "text", ...props }, ref) => {
    return (
      <div
        className={cn(
          "group flex h-9 items-center gap-2 rounded-md border border-input bg-background px-3",
          "transition-colors focus-within:border-ring focus-within:ring-2 focus-within:ring-ring focus-within:ring-offset-2 focus-within:ring-offset-background",
          "hover:border-ring/60",
          props.disabled && "opacity-50 pointer-events-none",
        )}
      >
        {leadingIcon ? (
          <span className="flex shrink-0 text-muted-foreground [&_svg]:size-4">
            {leadingIcon}
          </span>
        ) : null}
        <input
          ref={ref}
          type={type}
          className={cn(
            "flex-1 bg-transparent text-sm text-foreground placeholder:text-muted-foreground",
            "focus:outline-none disabled:cursor-not-allowed",
            "file:border-0 file:bg-transparent file:text-sm file:font-medium",
            className,
          )}
          {...props}
        />
        {trailing ? (
          <span className="flex shrink-0 items-center gap-1 text-xs text-muted-foreground">
            {trailing}
          </span>
        ) : null}
      </div>
    );
  },
);
Input.displayName = "Input";
