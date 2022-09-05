require("esbuild").serve(
  {
    servedir: "public",
  },
  {
    entryPoints: ["index.tsx"],
    outdir: "public",
    bundle: true,
    define: {
      global: "window",
    },
  }
);
