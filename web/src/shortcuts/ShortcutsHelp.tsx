// "?" overlay — a definitive cheat-sheet driven by the SHORTCUTS
// registry. Rendered as a Radix Dialog so Esc-to-close and focus
// trapping come for free.

import * as Dialog from "@radix-ui/react-dialog";
import { Keyboard, X } from "lucide-react";
import { useMemo } from "react";
import { Button } from "@/components/ui/Button";
import { Kbd } from "@/components/ui/Kbd";
import { cn } from "@/lib/cn";
import { SHORTCUTS, renderChord, type ShortcutSpec } from "./registry";

interface ShortcutsHelpProps {
  open: boolean;
  onOpenChange: (v: boolean) => void;
}

export function ShortcutsHelp({ open, onOpenChange }: ShortcutsHelpProps) {
  const isMac = useMemo(
    () =>
      typeof navigator !== "undefined" &&
      /mac|iphone|ipad/i.test(navigator.platform),
    [],
  );

  const groups = useMemo(() => {
    const m = new Map<ShortcutSpec["group"], ShortcutSpec[]>();
    for (const s of SHORTCUTS) {
      const arr = m.get(s.group) ?? [];
      arr.push(s);
      m.set(s.group, arr);
    }
    return Array.from(m.entries());
  }, []);

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay
          className={cn(
            "fixed inset-0 z-50 bg-background/70 backdrop-blur-sm",
            "data-[state=open]:animate-in data-[state=closed]:animate-out",
            "data-[state=open]:fade-in-0 data-[state=closed]:fade-out-0",
          )}
        />
        <Dialog.Content
          aria-describedby={undefined}
          className={cn(
            "fixed left-1/2 top-1/2 z-50 w-full max-w-2xl -translate-x-1/2 -translate-y-1/2",
            "rounded-2xl border border-border bg-card text-card-foreground shadow-elevated",
            "data-[state=open]:animate-in data-[state=closed]:animate-out",
            "data-[state=open]:fade-in-0 data-[state=closed]:fade-out-0",
            "data-[state=open]:zoom-in-95 data-[state=closed]:zoom-out-95",
          )}
        >
          <div className="flex items-center justify-between border-b border-border px-5 py-3">
            <div className="flex items-center gap-2">
              <span className="flex size-7 items-center justify-center rounded-lg bg-secondary text-foreground">
                <Keyboard className="size-3.5" />
              </span>
              <Dialog.Title className="text-sm font-semibold text-foreground">
                Keyboard shortcuts
              </Dialog.Title>
            </div>
            <Dialog.Close asChild>
              <Button variant="ghost" size="icon-sm" aria-label="Close">
                <X className="size-4" />
              </Button>
            </Dialog.Close>
          </div>

          <div className="grid max-h-[70vh] grid-cols-1 gap-x-8 gap-y-6 overflow-y-auto px-5 py-5 sm:grid-cols-2">
            {groups.map(([group, items]) => (
              <section key={group}>
                <h3 className="mb-2 text-2xs font-semibold uppercase tracking-widest text-muted-foreground">
                  {group}
                </h3>
                <ul className="flex flex-col gap-1.5">
                  {items.map((s) => (
                    <li
                      key={s.id}
                      className="flex items-center justify-between gap-3"
                    >
                      <span className="text-sm text-foreground">{s.label}</span>
                      <span className="flex shrink-0 items-center gap-1">
                        {renderChord(s.chord, isMac).map((token, i) => (
                          <span key={i} className="flex items-center gap-1">
                            {i > 0 ? (
                              <span className="text-2xs text-muted-foreground">
                                then
                              </span>
                            ) : null}
                            {token.split("+").map((k, j) => (
                              <Kbd key={j}>{k}</Kbd>
                            ))}
                          </span>
                        ))}
                      </span>
                    </li>
                  ))}
                </ul>
              </section>
            ))}
          </div>

          <div className="border-t border-border bg-muted/30 px-5 py-2 text-2xs text-muted-foreground">
            Press <Kbd>?</Kbd> from any page to reopen this list.
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
