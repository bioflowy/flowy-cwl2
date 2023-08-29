export class ValidationException extends Error {
    constructor(message:string){
        super(message)
    }
}
export class WorkflowException extends Error {
    constructor(message:string){
        super(message)
    }
}
export class UnsupportedRequirement extends Error {
    constructor(message:string){
        super(message)
    }
}
export class ValueError extends Error {
    constructor(message:string){
        super(message)
    }
}