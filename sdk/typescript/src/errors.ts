import type { StrataApiError } from "./types.js";

/** Error thrown when the Strata API returns an error response. */
export class StrataError extends Error {
  readonly code: string;
  readonly requestId?: string;
  readonly status: number;

  constructor(message: string, code: string, status: number, requestId?: string) {
    super(message);
    this.name = "StrataError";
    this.code = code;
    this.status = status;
    this.requestId = requestId;
  }

  static fromApiError(err: StrataApiError, status: number): StrataError {
    return new StrataError(err.message, err.code, status, err.request_id);
  }
}
