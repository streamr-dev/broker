export class GenericError extends Error {

    errorCode: string

    constructor(message: string, errorCode: string) {
        super(message)
        this.errorCode = errorCode
    }
}