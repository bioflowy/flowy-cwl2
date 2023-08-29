import { CWLObjectType, CWLOutputType, isString } from "./utils"

export function do_eval(
    ex1:CWLObjectType | undefined,
    jobinput:CWLObjectType,
    requirements:CWLObjectType[],
    outdir:string,
    tmpdir:string,
    resources:{[key:string]:number},
    context?: CWLOutputType,
    timeout?: number,
    strip_whitespace:boolean = true,
    cwlVersion:string = "",
    kwargs?:CWLObjectType
)
    {
        console.log(jobinput)
        return "TODO"
    }

export function needs_parsing(snippet: any) :boolean{
    return isString(snippet) &&  (snippet.includes("$(") || snippet.includes("${"))
}