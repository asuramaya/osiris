/**
 * @deepseek-ai/dsh-osiris-lifecycle — in-process Osiris lifecycle integration.
 *
 * Replaces Claude Code's subprocess hook scripts (statusline, stop, whisper, etc.)
 * with DSH-native lifecycle listeners — zero forks, zero cold connections,
 * everything through the MCP client's existing pool.
 *
 * Hooks mapped:
 *   Session start (first turn) → mount(cwd)
 *   Session end (turn boundary) → settle()
 *   Compaction → settle()
 *   Periodic → get_status() (poll for mail count, pulse)
 *
 * @module
 */
import type { Context } from '@deepseek-ai/cordis';
import { Schema } from '@deepseek-ai/schemastery';
/** Osiris lifecycle plugin config. */
export interface Config {
    /** Osiris MCP server name (as registered in the MCP client plugin). Default: 'osiris'. */
    serverName?: string;
    /** Mount automatically on first turn. Default: true. */
    autoMount?: boolean;
    /** Settle on session end. Default: true. */
    autoSettle?: boolean;
    /** Poll interval for status updates (seconds). 0 = disabled. Default: 30. */
    statusIntervalSecs?: number;
    /** Cwd to mount with. Defaults to the session's cwd. */
    cwd?: string;
}
export declare const name: "@deepseek-ai/dsh-osiris-lifecycle";
export declare const using: readonly ["mcp"];
export declare const Config: Schema<Config>;
export declare function apply(ctx: Context, config: Config): void;
//# sourceMappingURL=index.d.ts.map