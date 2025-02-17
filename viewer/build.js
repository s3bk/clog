const { build } = require("esbuild");
const { solidPlugin } = require("esbuild-plugin-solid");

async function run() {
  await build({
    entryPoints: [`main.tsx`],
    bundle: true,
    outfile: `pkg/main.js`,
    minify: false,
    sourcemap: true,
    logLevel: "debug",
    plugins: [
      solidPlugin()
    ],
    treeShaking: false,
    format: "esm",
    target: ["esnext", "safari15"],
    treeShaking: true,
  });
}

run();
