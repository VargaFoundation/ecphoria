import type { EcphoriaApiError } from "./types.js";
/** Error thrown when the Ecphoria API returns an error response. */
export declare class EcphoriaError extends Error {
    readonly code: string;
    readonly requestId?: string;
    readonly status: number;
    constructor(message: string, code: string, status: number, requestId?: string);
    static fromApiError(err: EcphoriaApiError, status: number): EcphoriaError;
}
//# sourceMappingURL=errors.d.ts.map