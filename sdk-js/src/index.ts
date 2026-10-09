/**
 * VidgeDB JavaScript/TypeScript SDK — pilot the `vidgedb --service`
 * JSON-RPC subprocess from Node (>= 18).
 *
 * Zero runtime dependencies: node:child_process + node:readline only.
 *
 *     import { VidgeDB, VidgeDBError } from "@vidge-ai/vidgedb";
 *
 *     await VidgeDB.with("/data/machine.vdg", { agentId: "my-agent" }, async (db) => {
 *       const report = await db.check("Motor42", "current", 1760000000, 1760025600);
 *       console.log(report.status, report.deviation);
 *     });
 */

export * from "./client.js";
export * from "./types.js";

import { VidgeDB, VidgeDBError } from "./client.js";
import type { CheckResult } from "./types.js";

/** SDK version, mirroring the Python SDK (package.json is the source of truth). */
export const VERSION: string = "0.1.0";

export default VidgeDB;