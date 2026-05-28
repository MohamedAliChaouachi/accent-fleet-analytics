import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

// Standard shadcn-style class merger. Use this anywhere a component
// accepts `className` so callers can override Tailwind classes without
// losing the component's defaults to specificity collisions.
export function cn(...inputs: ClassValue[]): string {
  return twMerge(clsx(inputs));
}
