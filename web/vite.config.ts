import { defineConfig } from "vite";

export default defineConfig({
  // Served from a repository subpath on GitHub Pages, so every asset URL has to
  // be relative to it. Overridden in dev, where the app is at the root.
  base: process.env.BASE_PATH ?? "/",
  build: {
    target: "es2022",
    outDir: "dist",
    rollupOptions: {
      input: {
        main: "index.html",
        results: "results.html",
      },
    },
  },
  // 5173 is commonly taken by another project; pick a lane of our own.
  server: { port: 5199, strictPort: true },
});
