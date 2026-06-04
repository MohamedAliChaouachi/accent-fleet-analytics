import { useEffect } from "react";
import { useLocation } from "react-router-dom";

/**
 * Scrolls to top of the main content area on route change.
 * The scroll container is the <main> element with overflow-y-auto,
 * not the window itself.
 */
export function ScrollToTop() {
  const { pathname } = useLocation();

  useEffect(() => {
    // Find the main scroll container (main element with overflow-y-auto)
    const mainContent = document.querySelector("main.overflow-y-auto");
    if (mainContent) {
      mainContent.scrollTop = 0;
    } else {
      // Fallback to window scroll
      window.scrollTo(0, 0);
    }
  }, [pathname]);

  return null;
}
