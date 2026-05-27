import { forwardRef } from "react";
import * as RSelect from "@radix-ui/react-select";
import { Check, ChevronDown } from "lucide-react";
import { cn } from "@/lib/cn";

// Radix-backed Select with shadcn-style theming. Use:
//   <Select value={v} onValueChange={setV}>
//     <SelectTrigger><SelectValue placeholder="..." /></SelectTrigger>
//     <SelectContent>
//       <SelectItem value="a">A</SelectItem>
//     </SelectContent>
//   </Select>

export const Select = RSelect.Root;
export const SelectGroup = RSelect.Group;
export const SelectValue = RSelect.Value;

export const SelectTrigger = forwardRef<
  React.ElementRef<typeof RSelect.Trigger>,
  React.ComponentPropsWithoutRef<typeof RSelect.Trigger>
>(({ className, children, ...props }, ref) => (
  <RSelect.Trigger
    ref={ref}
    className={cn(
      "flex h-9 w-full items-center justify-between gap-2 rounded-md border border-input bg-background px-3 text-sm",
      "transition-colors hover:border-ring/60",
      "data-[placeholder]:text-muted-foreground",
      "focus:outline-none focus:border-ring focus:ring-2 focus:ring-ring focus:ring-offset-2 focus:ring-offset-background",
      "disabled:cursor-not-allowed disabled:opacity-50",
      "[&>span]:line-clamp-1",
      className,
    )}
    {...props}
  >
    {children}
    <RSelect.Icon asChild>
      <ChevronDown className="size-4 opacity-60" />
    </RSelect.Icon>
  </RSelect.Trigger>
));
SelectTrigger.displayName = RSelect.Trigger.displayName;

export const SelectContent = forwardRef<
  React.ElementRef<typeof RSelect.Content>,
  React.ComponentPropsWithoutRef<typeof RSelect.Content>
>(({ className, children, position = "popper", ...props }, ref) => (
  <RSelect.Portal>
    <RSelect.Content
      ref={ref}
      position={position}
      sideOffset={4}
      className={cn(
        "z-50 min-w-[8rem] overflow-hidden rounded-md border border-border bg-popover text-popover-foreground shadow-elevated",
        "data-[state=open]:animate-fade-in-fast",
        position === "popper" &&
          "data-[side=bottom]:translate-y-1 data-[side=top]:-translate-y-1",
        className,
      )}
      {...props}
    >
      <RSelect.Viewport
        className={cn(
          "p-1",
          position === "popper" &&
            "h-[var(--radix-select-trigger-height)] w-full min-w-[var(--radix-select-trigger-width)]",
        )}
      >
        {children}
      </RSelect.Viewport>
    </RSelect.Content>
  </RSelect.Portal>
));
SelectContent.displayName = RSelect.Content.displayName;

export const SelectLabel = forwardRef<
  React.ElementRef<typeof RSelect.Label>,
  React.ComponentPropsWithoutRef<typeof RSelect.Label>
>(({ className, ...props }, ref) => (
  <RSelect.Label
    ref={ref}
    className={cn(
      "px-2 py-1.5 text-2xs font-semibold uppercase tracking-wide text-muted-foreground",
      className,
    )}
    {...props}
  />
));
SelectLabel.displayName = RSelect.Label.displayName;

export const SelectItem = forwardRef<
  React.ElementRef<typeof RSelect.Item>,
  React.ComponentPropsWithoutRef<typeof RSelect.Item>
>(({ className, children, ...props }, ref) => (
  <RSelect.Item
    ref={ref}
    className={cn(
      "relative flex w-full cursor-default select-none items-center rounded-sm py-1.5 pl-8 pr-2 text-sm outline-none",
      "focus:bg-secondary focus:text-secondary-foreground",
      "data-[disabled]:pointer-events-none data-[disabled]:opacity-50",
      className,
    )}
    {...props}
  >
    <span className="absolute left-2 flex size-4 items-center justify-center">
      <RSelect.ItemIndicator>
        <Check className="size-4 text-accent" />
      </RSelect.ItemIndicator>
    </span>
    <RSelect.ItemText>{children}</RSelect.ItemText>
  </RSelect.Item>
));
SelectItem.displayName = RSelect.Item.displayName;

export const SelectSeparator = forwardRef<
  React.ElementRef<typeof RSelect.Separator>,
  React.ComponentPropsWithoutRef<typeof RSelect.Separator>
>(({ className, ...props }, ref) => (
  <RSelect.Separator
    ref={ref}
    className={cn("-mx-1 my-1 h-px bg-border", className)}
    {...props}
  />
));
SelectSeparator.displayName = RSelect.Separator.displayName;
