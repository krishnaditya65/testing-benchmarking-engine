/**
 * vitest.config.ts
 *
 * Aliases `@shared-libraries/dag-domain-model` straight to that package's `src/index.ts` so unit
 * tests resolve the real domain types without depending on the root `pnpm-workspace.yaml`
 * having been reconciled to include this new service group yet (see this package's README).
 * Once the workspace entry lands and `pnpm install` links `node_modules/@shared-libraries/dag-domain-model`
 * for real, this alias is redundant but harmless — it points at the same source either way.
 */
import { defineConfig } from "vitest/config";
import path from "node:path";

export default defineConfig({
  resolve: {
    alias: {
      "@shared-libraries/dag-domain-model": path.resolve(
        __dirname,
        "../../shared-libraries/dag-domain-model/src/index.ts",
      ),
    },
  },
});
