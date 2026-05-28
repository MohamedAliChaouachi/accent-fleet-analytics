import type { HTMLAttributes } from "react";
import { cn } from "@/lib/cn";

// Shimmer placeholder. Compose for whole panels:
//   <Skeleton className="h-4 w-32" />
//   <Skeleton className="h-36 w-full" />
export function Skeleton({ className, ...props }: HTMLAttributes<HTMLDivElement>) {
  return <div className={cn("skeleton", className)} {...props} />;
}
