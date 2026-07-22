/** Error thrown when the Ecphoria API returns an error response. */
export class EcphoriaError extends Error {
    code;
    requestId;
    status;
    constructor(message, code, status, requestId) {
        super(message);
        this.name = "EcphoriaError";
        this.code = code;
        this.status = status;
        this.requestId = requestId;
    }
    static fromApiError(err, status) {
        return new EcphoriaError(err.message, err.code, status, err.request_id);
    }
}
//# sourceMappingURL=errors.js.map