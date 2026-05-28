import { forwardRef } from "react";
import * as RDropdown from "@radix-ui/react-dropdown-menu";
import { Check, ChevronRight } from "lucide-react";
import { cn } from "@/lib/cn";

// Minimal subset of shadcn's dropdown — enough for top-bar user menu,
// tenant switcher overflow, table row actions, etc.

export const DropdownMenu = RDropdown.Root;
export const DropdownMenuTrigger = RDropdown.Trigger;
export const DropdownMenuGroup = RDropdown.Group;
export const DropdownMenuPortal = RDropdown.Portal;
export const DropdownMenuSub = RDropdown.Sub;
export const DropdownMenuRadioGroup = RDropdown.RadioGroup;

export const DropdownMenuSubTrigger = forwardRef<
  React.ElementRef<typeof RDropdown.SubTrigger>,
  React.ComponentPropsWithoutRef<typeof RDropdown.SubTrigger>
>(({ className, children, ...props }, ref) => (
  <RDropdown.SubTrigger
    ref={ref}
    className={cn(
      "flex cursor-default select-none items-center gap-2 rounded-sm px-2 py-1.5 text-sm outline-none",
      "focus:bg-secondary data-[state=open]:bg-secondary",
      className,
    )}
    {...props}
  >
    {children}
    <ChevronRight className="ml-auto size-4 opacity-60" />
  </RDropdown.SubTrigger>
));
DropdownMenuSubTrigger.displayName = RDropdown.SubTrigger.displayName;

export const DropdownMenuSubContent = forwardRef<
  React.ElementRef<typeof RDropdown.SubContent>,
  React.ComponentPropsWithoutRef<typeof RDropdown.SubContent>
>(({ className, ...props }, ref) => (
  <RDropdown.SubContent
    ref={ref}
    className={cn(
      "z-50 min-w-[8rem] overflow-hidden rounded-md border border-border bg-popover p-1 text-popover-foreground shadow-elevated",
      className,
    )}
    {...props}
  />
));
DropdownMenuSubContent.displayName = RDropdown.SubContent.displayName;

export const DropdownMenuContent = forwardRef<
  React.ElementRef<typeof RDropdown.Content>,
  React.ComponentPropsWithoutRef<typeof RDropdown.Content>
>(({ className, sideOffset = 6, ...props }, ref) => (
  <RDropdown.Portal>
    <RDropdown.Content
      ref={ref}
      sideOffset={sideOffset}
      className={cn(
        "z-50 min-w-[10rem] overflow-hidden rounded-md border border-border bg-popover p-1 text-popover-foreground shadow-elevated",
        "data-[state=open]:animate-fade-in-fast",
        className,
      )}
      {...props}
    />
  </RDropdown.Portal>
));
DropdownMenuContent.displayName = RDropdown.Content.displayName;

export const DropdownMenuItem = forwardRef<
  React.ElementRef<typeof RDropdown.Item>,
  React.ComponentPropsWithoutRef<typeof RDropdown.Item> & {
    inset?: boolean;
  }
>(({ className, inset, ...props }, ref) => (
  <RDropdown.Item
    ref={ref}
    className={cn(
      "relative flex cursor-default select-none items-center gap-2 rounded-sm px-2 py-1.5 text-sm outline-none transition-colors",
      "focus:bg-secondary focus:text-secondary-foreground",
      "data-[disabled]:pointer-events-none data-[disabled]:opacity-50",
      inset && "pl-8",
      className,
    )}
    {...props}
  />
));
DropdownMenuItem.displayName = RDropdown.Item.displayName;

export const DropdownMenuCheckboxItem = forwardRef<
  React.ElementRef<typeof RDropdown.CheckboxItem>,
  React.ComponentPropsWithoutRef<typeof RDropdown.CheckboxItem>
>(({ className, children, checked, ...props }, ref) => (
  <RDropdown.CheckboxItem
    ref={ref}
    className={cn(
      "relative flex cursor-default select-none items-center rounded-sm py-1.5 pl-8 pr-2 text-sm outline-none",
      "focus:bg-secondary focus:text-secondary-foreground",
      className,
    )}
    checked={checked}
    {...props}
  >
    <span className="absolute left-2 flex size-4 items-center justify-center">
      <RDropdown.ItemIndicator>
        <Check className="size-4" />
      </RDropdown.ItemIndicator>
    </span>
    {children}
  </RDropdown.CheckboxItem>
));
DropdownMenuCheckboxItem.displayName = RDropdown.CheckboxItem.displayName;

export const DropdownMenuLabel = forwardRef<
  React.ElementRef<typeof RDropdown.Label>,
  React.ComponentPropsWithoutRef<typeof RDropdown.Label> & { inset?: boolean }
>(({ className, inset, ...props }, ref) => (
  <RDropdown.Label
    ref={ref}
    className={cn(
      "px-2 py-1.5 text-2xs font-semibold uppercase tracking-wide text-muted-foreground",
      inset && "pl-8",
      className,
    )}
    {...props}
  />
));
DropdownMenuLabel.displayName = RDropdown.Label.displayName;

export const DropdownMenuSeparator = forwardRef<
  React.ElementRef<typeof RDropdown.Separator>,
  React.ComponentPropsWithoutRef<typeof RDropdown.Separator>
>(({ className, ...props }, ref) => (
  <RDropdown.Separator
    ref={ref}
    className={cn("-mx-1 my-1 h-px bg-border", className)}
    {...props}
  />
));
DropdownMenuSeparator.displayName = RDropdown.Separator.displayName;

export function DropdownMenuShortcut({
  className,
  ...props
}: React.HTMLAttributes<HTMLSpanElement>) {
  return (
    <span
      className={cn(
        "ml-auto text-2xs tracking-widest text-muted-foreground",
        className,
      )}
      {...props}
    />
  );
}
