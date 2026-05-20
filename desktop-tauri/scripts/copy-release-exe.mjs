import { copyFileSync, existsSync, mkdirSync, statSync } from "node:fs";
import { basename, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const desktopRoot = resolve(scriptDir, "..");
const repoRoot = resolve(desktopRoot, "..");
const releaseExe = resolve(desktopRoot, "src-tauri", "target", "release", "theriac-lore-tauri.exe");
const rootExe = resolve(repoRoot, basename(releaseExe));

if (!existsSync(releaseExe)) {
  console.error(`Release executable not found: ${releaseExe}`);
  process.exit(1);
}

mkdirSync(repoRoot, { recursive: true });
copyFileSync(releaseExe, rootExe);

const size = statSync(rootExe).size;
console.log(`Copied ${releaseExe} -> ${rootExe} (${size} bytes)`);
