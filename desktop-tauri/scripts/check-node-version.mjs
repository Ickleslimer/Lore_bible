const supportedMajors = new Set([22, 23, 24]);
const [major] = process.versions.node.split(".").map((part) => Number.parseInt(part, 10));

if (!supportedMajors.has(major)) {
  console.error(
    [
      `Unsupported Node.js ${process.version}.`,
      "Use Node.js 24 LTS for this desktop app; Node.js 22 LTS is also supported.",
      "Node.js 23 is end-of-life and can make npm emit experimental CommonJS/ESM warnings.",
    ].join(" ")
  );
  process.exit(1);
}
