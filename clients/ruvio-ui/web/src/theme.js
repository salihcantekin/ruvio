// Light/dark theme: "auto" follows the system, "light" and "dark" are explicit.
// The choice is stored under ruvio.theme; index.html applies it before first paint.
import { preference, savePreference } from "./preferences.js";

export const modes = ["auto", "light", "dark"];

export function storedMode() {
  const value = preference("theme", "auto");
  return modes.includes(value) ? value : "auto";
}

export function resolve(mode, prefersLight) {
  return mode === "light" || mode === "dark" ? mode : prefersLight ? "light" : "dark";
}

export function applyTheme(mode) {
  const light = typeof matchMedia === "function" && matchMedia("(prefers-color-scheme: light)").matches;
  document.documentElement.dataset.theme = resolve(mode, light);
}

export function nextMode(mode) {
  return modes[(modes.indexOf(mode) + 1) % modes.length];
}

export function setMode(mode) {
  savePreference("theme", mode);
  applyTheme(mode);
}
