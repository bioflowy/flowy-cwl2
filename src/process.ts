/**
 * Classes and methods relevant for all CWL Process types.
 */
import * as fs from 'fs'
import * as path from 'path'
import {createHash} from 'crypto';
import fsExtra from 'fs-extra';
import {
    Builder
} from './builder';
import * as copy from './copy'
import {
    LoadingContext,
    RuntimeContext,
    getDefault,
} from './context';

import {
    UnsupportedRequirement,
    ValidationException,
    WorkflowException
} from './errors';

import {
    _logger
} from './loghandler';

import {
    MapperEnt,
    PathMapper
} from './pathmapper';

import {
    StdFsAccess
} from './stdfsaccess';

import { v4 as uuidv4 } from 'uuid';
import {
    CWLObjectType,
    CWLOutputAtomType,
    CWLOutputType,
    HasReqsHints,
    JobsGeneratorType,
    LoadListingType,
    MutableSequence,
    OutputCallbackType,
    aslist,
    ensureWritable,
    fileUri,
    get_listing,
    normalizeFilesDirs,
    random_outdir,
    uriFilePath,
    urldefrag,
    visit_class
} from './utils';

import urljoin from 'url-join';
import { needs_parsing } from './expression';


let _logger_validation_warnings =  _logger;

let supportedProcessRequirements = [
    "DockerRequirement",
    "SchemaDefRequirement",
    "EnvVarRequirement",
    "ScatterFeatureRequirement",
    "SubworkflowFeatureRequirement",
    "MultipleInputFeatureRequirement",
    "InlineJavascriptRequirement",
    "ShellCommandRequirement",
    "StepInputExpressionRequirement",
    "ResourceRequirement",
    "InitialWorkDirRequirement",
    "ToolTimeLimit",
    "WorkReuse",
    "NetworkAccess",
    "InplaceUpdateRequirement",
    "LoadListingRequirement",
    "http://commonwl.org/cwltool#TimeLimit",
    "http://commonwl.org/cwltool#WorkReuse",
    "http://commonwl.org/cwltool#NetworkAccess",
    "http://commonwl.org/cwltool#LoadListingRequirement",
    "http://commonwl.org/cwltool#InplaceUpdateRequirement",
    "http://commonwl.org/cwltool#CUDARequirement",
];

let cwl_files = [
    "Workflow.yml",
    "CommandLineTool.yml",
    "CommonWorkflowLanguage.yml",
    "Process.yml",
    "Operation.yml",
    "concepts.md",
    "contrib.md",
    "intro.md",
    "invocation.md",
];

let salad_files = [
    "metaschema.yml",
    "metaschema_base.yml",
    "salad.md",
    "field_name.yml",
    "import_include.md",
    "link_res.yml",
    "ident_res.yml",
    "vocab_res.yml",
    "vocab_res.yml",
    "field_name_schema.yml",
    "field_name_src.yml",
    "field_name_proc.yml",
    "ident_res_schema.yml",
    "ident_res_src.yml",
    "ident_res_proc.yml",
    "link_res_schema.yml",
    "link_res_src.yml",
    "link_res_proc.yml",
    "vocab_res_schema.yml",
    "vocab_res_src.yml",
    "vocab_res_proc.yml",
];

let SCHEMA_CACHE = {};
let SCHEMA_FILE;
let SCHEMA_DIR;
let SCHEMA_ANY;

// let custom_schemas = {};

// function use_standard_schema(version) {
//     if (version in custom_schemas) {
//         delete custom_schemas[version];
//     }
//     if (version in SCHEMA_CACHE) {
//         delete SCHEMA_CACHE[version];
//     }
// }

// function use_custom_schema(version, name, text) {
//     custom_schemas[version] = (name, text);
//     if (version in SCHEMA_CACHE) {
//         delete SCHEMA_CACHE[version];
//     }
// }


export function shortname(inputid: string): string {
    const parsedUrl = new URL(inputid);

    if (parsedUrl.hash) {
        return parsedUrl.hash.split('/').pop() || '';
    }
    return parsedUrl.pathname.split('/').pop() || '';

}
// function relocateOutputs(
//     outputObj: CWLObjectType,
//     destination_path: string,
//     source_directories: Set<string>,
//     action: string,
//     fs_access: StdFsAccess,
//     compute_checksum: boolean = true,
//     path_mapper: PathMapper
// ): CWLObjectType {
//     adjustDirObjs(outputObj, ((get_listing) => fs_access.get_listing(recursive=true)));

//     if (!["move", "copy"].includes(action)) {
//         return outputObj;
//     }

//     function *_collectDirEntries(
//         obj: CWLObjectType | Array<CWLObjectType> | null
//     ): Generator<CWLObjectType> {
//         if (obj instanceof Object) {
//             if (["File", "Directory"].includes(obj["class"])) {
//                 yield obj as CWLObjectType;
//             } else {
//                 for (let sub_obj of Object.values(obj)) {
//                     if(sub_obj){
//                         yield* _collectDirEntries(sub_obj as any );
//                     }
//                 }
//             }
//         } else if (Array.isArray(obj)) {
//             for (let sub_obj of obj as any[]) {
//                 yield* _collectDirEntries(sub_obj);
//             }
//         }
//     }

//     function _relocate(src: string, dst: string): void {
//         src = fs_access.realpath(src);
//         dst = fs_access.realpath(dst);

//         if (src === dst) {
//             return;
//         }

//         let src_can_deleted = source_directories.some(p => os.path.commonprefix([p, src]) === p);

//         let _action = action === "move" && src_can_deleted ? "move" : "copy";

//         if (_action === "move") {
//             _logger.debug(`Moving ${src} to ${dst}`);
//             if (fs_access.isdir(src) && fs_access.isdir(dst)) {

//                 for (let dir_entry of scandir(src)) {
//                     _relocate(dir_entry.path, fs_access.join(dst, dir_entry.name));
//                 }
//             } else {
//                 shutil.move(src, dst);
//             }

//         } else if (_action === "copy") {
//             _logger.debug(`Copying ${src} to ${dst}`);
//             if (fs_access.isdir(src)) {
//                 if (os.path.isdir(dst)) {
//                     shutil.rmtree(dst);
//                 } else if (os.path.isfile(dst)) {
//                     os.unlink(dst);
//                 }
//                 shutil.copytree(src, dst);
//             } else {
//                 shutil.copy2(src, dst);
//             }
//         }
//     }

//     function _realpath(ob: CWLObjectType): void {
//         const location = ob["location"] as string;
//         if (location.startsWith("file:")) {
//             ob["location"] = fileUri(path.resolve(uriFilePath(location)));
//         } else if (location.startsWith("/")) {
//             ob["location"] = path.resolve(location);
//         } else if (!location.startsWith("_:") && location.includes(":")) {
//             ob["location"] = fileUri(fs_access.realpath(location));
//         }
//     }

//     let outfiles = Array.from(_collectDirEntries(outputObj));
//     visit_class(outfiles, ["File", "Directory"], _realpath);
//     let pm = path_mapper(outfiles, "", destination_path, {separateDirs: false});
//     stage_files(pm, {stage_func: _relocate, symlink: false, fix_conflicts: true});

//     function _check_adjust(a_file: CWLObjectType): CWLObjectType {
//         a_file["location"] = fileUri(pm.mapper(a_file["location"])[1]);
//         if (a_file["contents"]) {
//             delete a_file["contents"];
//         }
//         return a_file;
//     }

//     visit_class(outputObj, ["File", "Directory"], _check_adjust);

//     if (compute_checksum) {
//         visit_class(outputObj, ["File"], (compute_checksums) => fs_access.compute_checksums);
//     }
//     return outputObj;
// }
function cleanIntermediate(output_dirs: Iterable<string>): void {
    for (let a of output_dirs) {
        if (fs.existsSync(a)) {
            _logger.debug(`Removing intermediate output directory ${a}`);
            fsExtra.removeSync(a);
        }
    }
}

function add_sizes(fsaccess: StdFsAccess, obj: CWLObjectType): void {
    if ("location" in obj) {
        try {
            if (!("size" in obj)) {
                obj["size"] = fsaccess.size(String(obj["location"]));
            }
        } catch (OSError) {
            return;
        }
    } else if ("contents" in obj) {
        obj["size"] = (obj["contents"] as any).length;
    }
    return;  // best effort
}

function fill_in_defaults(
    inputs: Array<CWLObjectType>,
    job: CWLObjectType,
    fsaccess: StdFsAccess,
): void {
    let debug = _logger.isDebugEnabled()
    for (let e = 0; e < inputs.length; e++) {
        let inp = inputs[e];
        const fieldname = shortname(String(inp["id"]));
        if (job[fieldname] != null) {
            continue;
        } else if (job[fieldname] == null && "default" in inp) {
            job[fieldname] = JSON.parse(JSON.stringify(inp["default"]));
        } else if (job[fieldname] == null && "null" in aslist(inp["type"])) {
            job[fieldname] = undefined;
        } else {
            throw new WorkflowException(
                `Missing required input parameter '${shortname(String(inp["id"]))}'`
            );
        }
    }
}
function avroizeType(
    fieldType: CWLObjectType | MutableSequence<any> | CWLOutputType | null,
    namePrefix: string = "",
): CWLObjectType | MutableSequence<any> | CWLOutputType | null {
    if (Array.isArray(fieldType)) {
        for (let i = 0; i < fieldType.length; i++) {
            fieldType[i] = avroizeType(fieldType[i], namePrefix);
        }
    } else if (fieldType instanceof Map) {
        if (fieldType["type"] === "enum" || fieldType["type"] === "record") {
            if (!("name" in fieldType)) {
                fieldType["name"] = namePrefix + uuidv4();
            }
        }
        if (fieldType["type"] === "record") {
            fieldType["fields"] = avroizeType(fieldType["fields"], namePrefix);
        } else if (fieldType["type"] === "array") {
            fieldType["items"] = avroizeType(fieldType["items"], namePrefix);
        } else {
            fieldType["type"] = avroizeType(fieldType["type"], namePrefix);
        }
    } else if (fieldType === "File") {
        return "org.w3id.cwl.cwl.File";
    } else if (fieldType === "Directory") {
        return "org.w3id.cwl.cwl.Directory";
    }
    return fieldType;
}

function getOverrides(overrides: MutableSequence<CWLObjectType>, toolId: string): CWLObjectType {
    let req: CWLObjectType = {};
    if (!Array.isArray(overrides)) {
        throw new Error(`Expected overrides to be a list, but was ${typeof overrides}`);
    }
    for (let ov of overrides) {
        if (ov["overrideTarget"] === toolId) {
            Object.assign(req, ov);
        }
    }
    return req;
}
const _VAR_SPOOL_ERROR = `
    Non-portable reference to /var/spool/cwl detected: '{}'.
    To fix, replace /var/spool/cwl with $(runtime.outdir) or add
    DockerRequirement to the 'requirements' section and declare
    'dockerOutputDirectory: /var/spool/cwl'.
`;

function var_spool_cwl_detector(
    obj: CWLOutputType,
    item: any = null,
    obj_key: any = null,
): boolean {
    let r = false;
    if (typeof obj === 'string') {
        if (obj.includes("var/spool/cwl") && obj_key != "dockerOutputDirectory") {
            _logger.warning(
                new Error(
                    _VAR_SPOOL_ERROR.replace('{}', obj)
                )
            );
            r = true;
        }
    } else if (obj instanceof Map) {
        for (let [mkey, mvalue] of obj.entries()) {
            r = var_spool_cwl_detector(mvalue as CWLOutputType, obj, mkey) || r;
        }
    } else if (obj instanceof Array) {
        for (let [lkey, lvalue] of obj.entries()) {
            r = var_spool_cwl_detector(lvalue as CWLOutputType, obj, lkey) || r;
        }
    }
    return r;
}

function eval_resource(
    builder: Builder, resource_req: string | number
): (string | number | null) {
    if (typeof resource_req === 'string' && needs_parsing(resource_req)) {
        let result = builder.do_eval(resource_req);
        if (typeof result === 'number') {
            throw new WorkflowException(
                `Floats are not valid in resource requirement expressions prior 
                 to CWL v1.2: ${resource_req} returned ${result}.`
            );
        }
        if (typeof result === 'string' || typeof result === 'number' || result === null) {
            return result as string | number | null;
        }
        throw new WorkflowException(
            `Got incorrect return type ${typeof result} from resource expression evaluation of ${resource_req}.`
        );
    }
    return resource_req;
}

// Threshold where the "too many files" warning kicks in
const FILE_COUNT_WARNING = 5000;
export abstract class Process extends HasReqsHints {
    metadata: CWLObjectType;
    provenance_object: any | null;
    parent_wf: any | null;
    names: any;
    tool: Object;
    requirements: any[];
    hints: any[];
    original_requirements: any[];
    original_hints: any[];
    doc_loader: any;
    doc_schema: any;
    formatgraph: any | null;
    schemaDefs: {[key: string]: CWLObjectType};
    inputs_record_schema: CWLObjectType;
    outputs_record_schema: CWLObjectType;
    container_engine: "docker" | "podman" |"singularity";
    constructor(toolpath_object: Object, loadingContext: LoadingContext) {
        super();

        this.metadata = getDefault(loadingContext.metadata, {});
        this.provenance_object = null;
        this.parent_wf = null;

        // if (SCHEMA_FILE === null || SCHEMA_ANY === null || SCHEMA_DIR === null) {
        //     get_schema("v1.0");
        //     SCHEMA_ANY = SCHEMA_CACHE["v1.0"][3].idx["https://w3id.org/cwl/salad#Any"];
        //     SCHEMA_FILE = SCHEMA_CACHE["v1.0"][3].idx["https://w3id.org/cwl/cwl#File"];
        //     SCHEMA_DIR = SCHEMA_CACHE["v1.0"][3].idx["https://w3id.org/cwl/cwl#Directory"];
        // }

        // this.names = make_avro_schema([SCHEMA_FILE, SCHEMA_DIR, SCHEMA_ANY], new Loader({}));
        this.tool = toolpath_object;
        let debug = loadingContext.debug;
        this.requirements = copy.deepcopy(getDefault(loadingContext.requirements, []));
        let tool_requirements = this.tool["requirements"] || [];

        if (tool_requirements === null) {
            throw new ValidationException(
                "If 'requirements' is present then it must be a list or map/dictionary, not empty."
            );
        }

        this.requirements = this.requirements.concat(tool_requirements);

        if (!this.tool.hasOwnProperty("id")) {
            this.tool["id"] = "_:" + uuidv4();
        }
        // overrides not supported
        // this.requirements = this.requirements.concat(
        //     getOverrides(getDefault(loadingContext.overrides_list, []), this.tool["id"]).get(
        //         "requirements", []
        //     )
        // );

        this.hints = copy.deepcopy(getDefault(loadingContext.hints, []));
        let tool_hints = this.tool["hints"] || [];

        if (tool_hints === null) {
            throw new ValidationException(
                "If 'hints' is present then it must be a list or map/dictionary, not empty."
            );
        }

        this.hints = this.hints.concat(tool_hints);
        this.original_requirements = copy.deepcopy(this.requirements);
        this.original_hints = copy.deepcopy(this.hints);
        // this.doc_loader = loadingContext.loader;
        // this.doc_schema = loadingContext.avsc_names;
        this.formatgraph = null;

        if (this.doc_loader !== null) {
            this.formatgraph = this.doc_loader.graph;
        }

        this.checkRequirements(this.tool as any, supportedProcessRequirements);
        this.validate_hints(
            undefined,
            this.tool["hints"] || [],
            getDefault(loadingContext.strict, false)
        );

        this.schemaDefs = {};

        let sd = this.get_requirement("SchemaDefRequirement")[0];

        if (sd !== undefined) {
            let sdtypes = copy.deepcopy(sd["types"]);
            // TODO
            // avroizeType(sdtypes);
            // let av = make_valid_avro(
            //     sdtypes,
            //     {t["name"]: t for t in sdtypes},
            //     new Set(),
            //     INPUT_OBJ_VOCAB,
            // );
            // for (let i of av) {
            //     this.schemaDefs[i["name"]] = i;
            // }

            // make_avsc_object(convert_to_dict(av), this.names);
        }
        // Build record schema from inputs
        this.inputs_record_schema = {
            "name": "input_record_schema",
            "type": "record",
            "fields": [],
        };

        this.outputs_record_schema = {
            "name": "outputs_record_schema",
            "type": "record",
            "fields": [],
        };

        for (let key of ["inputs", "outputs"]) {
            for (let i of this.tool[key]) {
                let c = JSON.parse(JSON.stringify(i));
                c["name"] = shortname(c["id"]);
                delete c["id"];

                if (!c.hasOwnProperty('type')) {
                    throw new Error(`Missing 'type' in parameter '${c["name"]}'`);
                }

                if (c.hasOwnProperty('default') && !aslist(c['type']).includes('null')) {
                    let nullable = ["null"];
                    nullable.push(...aslist(c["type"]));
                    c["type"] = nullable;
                } else {
                    c["type"] = c["type"];
                }

                c["type"] = avroizeType(c["type"], c["name"]);

                if (key === "inputs") {
                    (this.inputs_record_schema["fields"] as any).push(c);
                } else if (key === "outputs") {
                    (this.outputs_record_schema["fields"] as any).push(c);
                }
            }
        }
        
        this.container_engine = "docker";
        if (loadingContext.podman) {
            this.container_engine = "podman";
        } else if (loadingContext.singularity) {
            this.container_engine = "singularity";
        }

        if (toolpath_object["class"] && !getDefault(loadingContext.disable_js_validation, false)) {
            let validate_js_options: { [key: string]: Array<string> | string | number } | null = null;
            if (loadingContext.js_hint_options_file) {
                try {
                    // Note: Reading files in TypeScript/JavaScript, especially in browsers, doesn't use 'open'. You'd typically use something like the File API, or fs in Node.js.
                    const options_file = fs.readFileSync(loadingContext.js_hint_options_file, 'utf8');
                    validate_js_options = JSON.parse(options_file);
                } catch (e) {
                    _logger.error(`Failed to read options file ${loadingContext.js_hint_options_file}`);
                    throw e;
                }
            }
            if (this.doc_schema) {
                const classname = toolpath_object["class"];
                let avroname = classname;
                // if (this.doc_loader && this.doc_loader.vocab[classname]) {
                //     avroname = avro_type_name(this.doc_loader.vocab[classname]);
                // }
                // validate_js_expressions(
                //     toolpath_object,
                //     this.doc_schema.names[avroname],
                //     validate_js_options,
                //     this.container_engine,
                //     loadingContext.eval_timeout
                // );
            }
        }

        const [dockerReq, is_req] = this.get_requirement("DockerRequirement");

        if (
            dockerReq &&
            "dockerOutputDirectory" in dockerReq &&
            is_req !== undefined &&
            !is_req
        ) {
            _logger.warning(
                "When 'dockerOutputDirectory' is declared, DockerRequirement should go in the 'requirements' section, not 'hints'."
            );
        }

        if (
            dockerReq &&
            is_req !== undefined &&
            dockerReq["dockerOutputDirectory"] === "/var/spool/cwl"
        ) {
            if (is_req) {
                // In this specific case, it is legal to have /var/spool/cwl, so skip the check.
            } else {
                // Must be a requirement
                var_spool_cwl_detector(this.tool as CWLObjectType);
            }
        } else {
            var_spool_cwl_detector(this.tool as CWLObjectType);
        }
    }

// Remaining code skipped as it involves missing functions or types. The conversion follows the same pattern.
_init_job(joborder: CWLObjectType, runtime_context: RuntimeContext): Builder {

    let job = Object.assign({}, joborder);

    let fs_access = new StdFsAccess(runtime_context.basedir);

    let [load_listing_req] = this.get_requirement("LoadListingRequirement");

    let load_listing = 
        load_listing_req != null 
        ? load_listing_req["loadListing"] as LoadListingType
        : "no_listing"
    ;

    // Validate job order
    try {
        fill_in_defaults(this.tool["inputs"], job, fs_access);

        normalizeFilesDirs(job);
        let schema = this.names.get_name("input_record_schema", null);
        if (schema == null) {
            throw new WorkflowException("Missing input record schema: " + this.names);
        }
        // validate_ex(
        //     schema,
        //     job,
        //     false,
        //     _logger_validation_warnings,
        //     INPUT_OBJ_VOCAB,
        // );

        if (load_listing != "no_listing") {
            get_listing(fs_access, job, load_listing == "deep_listing");
        }

        visit_class(job, ["File"], (x: any): void => add_sizes(fs_access,x));

        if (load_listing == "deep_listing") {
            this.tool["inputs"].forEach((inparm: any, i: number) => {
                let k = shortname(inparm["id"]);
                if (!(k in job)) {
                    return;
                }
                let v = job[k];
                let dircount = [0];

                let inc = function (d: Array<number>): void {
                    d[0]++;
                }

                visit_class(v, ["Directory"], (x: any): void => inc(dircount));
                if (dircount[0] == 0) {
                    return;
                }
                let filecount = [0];
                visit_class(v, ["File"], (x: any): void => inc(filecount));
                if (filecount[0] > FILE_COUNT_WARNING) {
                    _logger.warning(`Recursive directory listing has resulted in a large number of File objects (${filecount[0]}) passed to the input parameter '${k}'.  This may negatively affect workflow performance and memory use.

If this is a problem, use the hint 'cwltool:LoadListingRequirement' with "shallow_listing" or "no_listing" to change the directory listing behavior:

$namespaces:
  cwltool: "http://commonwl.org/cwltool#"
hints:
  cwltool:LoadListingRequirement:
    loadListing: shallow_listing
`)
                }
            });
        }
    }
    catch (err) {
        if (err instanceof ValidationException || err instanceof WorkflowException) {
            throw new WorkflowException("Invalid job input record:\n" + err);
        } else {
            throw err;
        }
    }

    let files: Array<CWLObjectType> = [];
    let bindings:any[] = [];
    let outdir = "";
    let tmpdir = "";
    let stagedir = "";

    let [docker_req] = this.get_requirement("DockerRequirement");
    let default_docker:string | undefined = undefined;

    if (docker_req == null && runtime_context.default_container) {
        default_docker = runtime_context.default_container;
    }

    if ((docker_req || default_docker) && runtime_context.use_container) {
        if (docker_req != null) {
            const dockerOutputDirectory = docker_req["dockerOutputDirectory"]
            if (dockerOutputDirectory && typeof dockerOutputDirectory === "string" && dockerOutputDirectory.startsWith("/")) {
                outdir = dockerOutputDirectory;
            } else {
                outdir = String(
                    dockerOutputDirectory
                    || runtime_context.docker_outdir
                    || random_outdir()
                );
            }
        } else if (default_docker != null) {
            outdir = runtime_context.docker_outdir || random_outdir();
        }
        tmpdir = runtime_context.docker_tmpdir || "/tmp";
        stagedir = runtime_context.docker_stagedir || "/var/lib/cwl";
    } else {
        if (this.tool["class"] == "CommandLineTool") {
            outdir = fs_access.realpath(runtime_context.getOutdir());
            tmpdir = fs_access.realpath(runtime_context.getTmpdir());
            stagedir = fs_access.realpath(runtime_context.getStagedir());
        }
    }
// let cwl_version: string = <string> this.metadata[];
let builder: Builder = new Builder(
    job,
    files,
    bindings,
    this.schemaDefs,
    this.names,
    this.requirements,
    this.hints,
    {},
    runtime_context.mutation_manager,
    this.formatgraph,
    StdFsAccess,
    fs_access,
    runtime_context.job_script_provider,
    runtime_context.eval_timeout,
    runtime_context.debug,
    runtime_context.js_console,
    runtime_context.force_docker_pull,
    load_listing,
    outdir,
    tmpdir,
    stagedir,
    "unknown",
    this.container_engine,
);

bindings.push(...builder.bind_input(
    this.inputs_record_schema,
    job,
    runtime_context.toplevel ?? false
));

if (this.tool["baseCommand"]) {
    this.tool["baseCommand"].forEach((command, index) => {
        bindings.push( { "position": [-1000000, index], "datum": command } );
    });
}

if (this.tool["arguments"]) {
    this.tool["arguments"].forEach((arg, i) => {
        if ((arg as any) instanceof Object) {
            arg = Object.assign({}, arg);
            if (arg["position"]) {
                let position = arg.get("position");
                if (typeof position === "string") {
                    position = builder.do_eval(position);
                    if (!position) {
                        position = 0;
                    }
                }
                arg["position"] = [position, i];
            }
            else {
                arg["position"] = [0, i];
            }
            bindings.push(arg);
        }
        else if (arg.includes("$(") || arg.includes("${")) {
            let cm = {
                "position": [0, i],
                 "valueFrom": arg
            }
            bindings.push(cm);
        }
        else {
            let cm = {
                "position": [0, i],
                "datum": arg
            };
            bindings.push(cm);
        }
    });
}

let bindings_copy = [...bindings];
bindings.length = 0;
bindings = bindings_copy.sort();

if (this.tool["class"] !== "Workflow") {
    // builder.resources = eval_resources(builder, runtime_context);
}
return builder;
}
checkRequirements(
  rec: MutableSequence<CWLObjectType> | CWLObjectType | CWLOutputType | null,
  supported_process_requirements: Iterable<string>,
): void {
    // TODO
//   if (rec instanceof MutableMapping) {
//     if ("requirements" in rec) {
//       const debug = _logger.isDebugEnabled()
//       for (let i = 0 ; i < rec["requirements"].length ; i++ ) {
//         const entry = rec["requirements"][i] as CWLObjectType;
//         const sl = new SourceLine(rec["requirements"], i, UnsupportedRequirement, debug);
//         if ((entry["class"] as string) not in supported_process_requirements) {
//           throw new UnsupportedRequirement(
//             `Unsupported requirement ${entry['class']}.`
//           );
//         }
//       }
//     }
//   }
}

validate_hints(avsc_names: any, hints: Array<CWLObjectType>, strict: boolean): void {
    // TODO
//   if (this.doc_loader === null) {
//     return;
//   }
//   const debug = _logger.isDebugEnabled()
//   for (let i = 0; i < hints.length; i++) {
//     const r = hints[i];
//     const sl = new SourceLine(hints, i, ValidationException, debug);
//     const classname = r["class"] as string
//     if (classname === "http://commonwl.org/cwltool#Loop") {
//       throw new ValidationException(
//         "http://commonwl.org/cwltool#Loop is valid only under requirements."
//       );
//     }
//     let avroname = classname;
//     if (classname in this.doc_loader.vocab) {
//       avroname = avro_type_name(this.doc_loader.vocab[classname]);
//     }
//     if (avsc_names.get_name(avroname, null) !== null) {
//       const plain_hint = {
//         ...r,
//         ...{[key]: r[key] for (let key in r) if (key not in this.doc_loader.identifiers} // strip identifiers
//       };
//       validate_ex(
//         avsc_names.get_name(avroname, null) as Schema,
//         plain_hint,
//         strict,
//         this.doc_loader.vocab,
//       );
//     } else if ((r["class"] as string) in ["NetworkAccess", "LoadListingRequirement"]) {
//       continue;
//     } else {
//       _logger.info(sl.makeError(`Unknown hint ${r["class"]}`));
//     }
//   }
}

visit(op: (map: any) => void): void {
  op(this.tool);
}

abstract job(
  job_order: CWLObjectType,
  output_callbacks: OutputCallbackType | null,
  runtimeContext: RuntimeContext,
): JobsGeneratorType;

toString(): string {
  return `${this.constructor.name}: ${this.tool['id']}`;
}
}
const _names: Set<string> = new Set();
export function uniquename(stem: string, names?: Set<string>): string {
    if (!names) {
        names = _names;
    }
    let c = 1;
    let u = stem;
    while (names.has(u)) {
        c += 1;
        u = `${stem}_${c}`;
    }
    names.add(u);
    return u;
}


function nestdir(base: string, deps: CWLObjectType): CWLObjectType{
    let dirname = path.dirname(base) + "/";
    let subid = <string>deps["location"];
    if (subid.startsWith(dirname)){
        let s2 = subid.slice(dirname.length);
        let sp = s2.split("/");
        sp.pop();
        while (sp.length > 0){
            let loc =dirname + sp.join("/");
            let nx = sp.pop();
            deps = {
                "class": "Directory",
                "basename": nx,
                "listing": [deps],
                "location": loc,
            }
        }
    }
    return deps;
}


function mergedirs(listing: CWLObjectType[]): CWLObjectType[] {
    let r: CWLObjectType[] = [];
    let ents: {[key:string]:CWLObjectType} = {};
    for (let e of listing) {
        let basename = <string>e["basename"];
        if (basename in ents == false) {
            ents[basename] = e;
        } else if (e["location"] != ents[basename]["location"]) {
            throw new ValidationException(`Conflicting basename in listing or secondaryFiles, ${basename} used by both ${e["location"]} and ${ents[basename]["location"]}`);
        } else if (e["class"] == "Directory") {
            if (e["listing"]){
                (<CWLObjectType[]> ents[basename]["listing"]).push(... <CWLObjectType[]> e["listing"]);
            }
        }
    }

    for (let e of Object.values(ents)) {
        if (e["class"] == "Directory" && "listing" in e){
            e["listing"] = mergedirs(e["listing"] as CWLObjectType[]);
        }
    }

    r.push(...Object.values(ents));
    return r; 
}

const CWL_IANA = "https://www.iana.org/assignments/media-types/application/cwl";
function scandeps_file_dir(
    base: string,
    doc: CWLObjectType,
    reffields: Set<string>,
    urlfields: Set<string>,
    loadref: (arg1: string, arg2: string) => Object | Array<any> | string | null,
    urljoin: (arg1: string, arg2: string) => string,
    nestdirs: boolean
  ): Array<CWLObjectType> {
    let r: Array<CWLObjectType> = [];
    let u = (doc["location"] || doc["path"]) as string ;
    if (u && !u.startsWith("_:")) {
      let deps: CWLObjectType = {
        "class": doc["class"],
        "location": urljoin(base, u)
      }
      if (doc["basename"]) {
        deps["basename"] = doc["basename"];
      }
      if (doc["class"] == "Directory" && doc["listing"]) {
        deps["listing"] = doc["listing"];
      }
      if (doc["class"] == "File" && doc["secondaryFiles"]) {
        deps["secondaryFiles"] = scandeps(
          base,
          doc["secondaryFiles"] as (CWLObjectType | Array<CWLObjectType>),
          reffields,
          urlfields,
          loadref,
          urljoin,
          nestdirs
        ) as CWLOutputAtomType;
      }
      if (nestdirs) {
        deps = nestdir(base, deps);
      }
      r.push(deps);
    } else {
      if (doc["class"] == "Directory" && doc["listing"]) {
        r = r.concat(scandeps(
          base,
          doc["listing"] as Array<CWLObjectType>,
          reffields,
          urlfields,
          loadref,
          urljoin,
          nestdirs
        ));
      } else if (doc["class"] == "File" && doc["secondaryFiles"]) {
        r = r.concat(scandeps(
          base,
          doc["secondaryFiles"] as Array<CWLObjectType>,
          reffields,
          urlfields,
          loadref,
          urljoin,
          nestdirs
        ));
      }
    }
    return r;
  }
  function scandeps_item(base: string,
                         doc: CWLObjectType,
                         reffields: Set<string>,
                         urlfields: Set<string>,
                         loadref: (param1: string, param2: string) => (Object | Array<any> | string | null),
                         urljoin: (param1: string, param2: string) => string,
                         nestdirs: boolean,
                         key: string,
                         v: CWLOutputType): Array<CWLObjectType> {
      let r: Array<CWLObjectType> = [];
      if (reffields.has(key)) {
          for (let u2 of aslist(v)) {
              if (u2 instanceof Map) {
                  r.push(
                      ...scandeps(
                          base,
                          u2 as unknown as CWLObjectType,
                          reffields,
                          urlfields,
                          loadref,
                          urljoin,
                          nestdirs,
                      )
                  );
              } else {
                  let subid = urljoin(base, u2);
                  let basedf = new URL(base).hash;
                  let subiddf = new URL(subid).hash;
                  if (basedf == subiddf) {
                      continue;
                  }
                  let sub = loadref(base, u2);
                  let deps2: CWLObjectType = {
                      "class": "File",
                      "location": subid,
                      "format": CWL_IANA,
                  };
                  let sf = scandeps(
                      subid,
                      sub as CWLObjectType | CWLObjectType[],
                      reffields,
                      urlfields,
                      loadref,
                      urljoin,
                      nestdirs,
                  );
                  if (sf.length > 0) {
                      deps2["secondaryFiles"] = mergedirs(sf)
                  }
                  if (nestdirs) {
                      deps2 = nestdir(base, deps2);
                  }
                  r.push(deps2);
              }
          }
      } else if (urlfields.has(key) && key != "location") {
          for (let u3 of aslist(v)) {
              let deps:CWLObjectType = { "class": "File", "location": urljoin(base, u3) };
              if (nestdirs) {
                  deps = nestdir(base, deps)
              }
              r.push(deps);
          }
      } else if ((doc["class"] == "File" || doc["class"] == "Directory") && (key == "listing" || key == "secondaryFiles")) {
          // should be handled earlier.
      } else {
          r.push(
              ...scandeps( base,
                v as CWLObjectType | CWLObjectType[], 
                reffields, 
                urlfields, 
                loadref, 
                urljoin, 
                nestdirs)
          );
      }
      return r;
  }
export function scandeps(
      base: string,
      doc: CWLObjectType | CWLObjectType[],
      reffields: Set<string>,
      urlfields: Set<string>,
      loadref: (a: string, b: string) => Object | Array<any> | string | null,
      urljoin2: (a: string, b: string) => string = urljoin,
      nestdirs: boolean = true,
  ): CWLObjectType[] {
      let r: CWLObjectType[] = [];
      if (typeof doc === "object" && doc !== null && !Array.isArray(doc)) {
          if ("id" in doc) {
              if (typeof doc["id"] === 'string' && doc["id"].startsWith("file://")) {
                  let df, _ = urldefrag(doc["id"]);
                  if (base !== df) {
                      r.push({ "class": "File", "location": df, "format": CWL_IANA });
                      base = df;
                  }
              }
          }
  
          if (doc["class"] as string in ["File", "Directory"] && "location" in urlfields) {
              r = r.concat(scandeps_file_dir(base, doc, reffields, urlfields, loadref, urljoin2, nestdirs));
          }
  
          for (let k in doc) {
              if (doc.hasOwnProperty(k)) {
                  let v = doc[k];
                  if(v){
                    r = r.concat(scandeps_item(base, doc, reffields, urlfields, loadref, urljoin2, nestdirs, k, v));
                  }
              }
          }
      } else if (Array.isArray(doc)) {
          for (let d of doc) {
              r = r.concat(
                  scandeps(
                      base,
                      d,
                      reffields,
                      urlfields,
                      loadref,
                      urljoin2,
                      nestdirs,
                  )
              );
          }
      }
  
      if (r.length) {
          normalizeFilesDirs(r);
      }
  
      return r;
  }
  function calculateSHA1(filePath: string): Promise<string> {
    return new Promise((resolve, reject) => {
        const hash = createHash('sha1');
        const stream = fs.createReadStream(filePath);

        stream.on('data', chunk => {
            hash.update(chunk);
        });

        stream.on('end', () => {
            resolve(hash.digest('hex'));
        });

        stream.on('error', err => {
            reject(err);
        });
    });
}
export async function compute_checksums(fs_access: StdFsAccess, fileobj: CWLObjectType): Promise<void>{
    // TODO
    if (!("checksum" in fileobj)) {
        const location = fileobj["location"] as string;
        const hash = await createHash(location)
        fileobj["checksum"] = `sha1$${hash}`;
        fileobj["size"] = fs_access.size(location);
    }
}
