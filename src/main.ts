import * as os from 'os'
import * as cwlTsAuto from 'cwl-ts-auto'
import { error } from 'console'
import { LoadingContext } from './context'
import { CommandLineTool } from './command_line_tool'

async function main():Promise<number>{
    const doc = await cwlTsAuto.loadDocument(process.argv[2])
    console.log(doc)
    if(!(doc instanceof cwlTsAuto.CommandLineTool)){
        return  1
    }
    const loadingContext = new LoadingContext({})
    const tool = new CommandLineTool(doc,loadingContext)
    return 0
}

main().then((n)=>{
    console.log(n)
})
