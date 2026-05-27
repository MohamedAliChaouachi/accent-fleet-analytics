import type { HTMLAttributes, ReactNode } from "react";
import { cn } from "@/lib/cn";
import { Breadcrumbs } from "./Breadcrumbs";

interface PageContainerProps extends HTMLAttributes<HTMLDivElement> {
  title: string;
  description?: ReactNode;
  // Right-aligned slot for page-level actions: filters, export, etc.
  actions?: ReactNode;
  // Hide breadcrumbs on top-level "home" routes if desired.
  showBreadcrumbs?: boolean;
  children: ReactNode;
}

// Standard page wrapper: breadcrumbs, header with title/description/actions,
// then content. Use inside the DashboardShell's <Outlet>.
export function PageContainer({
  title,
  description,
  actions,
  showBreadcrumbs = true,
  className,
  children,
  ...props
}: PageContainerProps) {
  return (
    <div
      className={cn(
        "mx-auto w-full max-w-[1600px] px-6 py-6 animate-fade-in",
        className,
      )}
      {...props}
    >
      {showBreadcrumbs ? <Breadcrumbs className="mb-3" /> : null}
      <header className="mb-6 flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight text-foreground">
            {title}
          </h1>
          {description ? (
            <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
              {description}
            </p>
          ) : null}
        </div>
        {actions ? (
          <div className="flex flex-wrap items-center gap-2">{actions}</div>
        ) : null}
      </header>
      {children}
    </div>
  );
}
