import { flatNav } from "../../config/site";

/** The main-menu entry for this path: the longest internal href the path starts with. */
export function currentNavHref(pathname: string): string | undefined {
  return flatNav
    .map((item) => item.href)
    .filter((href) => href.startsWith("/") && !href.startsWith("/#") && href !== "/" && pathname.startsWith(href))
    .sort((a, b) => b.length - a.length)[0];
}
