import { Builder, contentLimitRespectedRead, substitute } from "./builder";
import * as path from 'path'
import * as fs from 'fs'
import { LoadingContext, RuntimeContext, getDefault } from "./context";
import { UnsupportedRequirement, ValidationException, WorkflowException } from "./errors";
import { _logger } from "./loghandler";
import { PathMapper } from "./pathmapper";
import { Process, compute_checksums, shortname, uniquename } from "./process";
import { StdFsAccess } from "./stdfsaccess";
import { CWLObjectType, CWLOutputType, JobsGeneratorType, MutableMapping, OutputCallbackType, adjustDirObjs, adjustFileObjs, aslist, fileUri, get_listing, isString, normalizeFilesDirs, quote, splitext, uriFilePath, visit_class } from "./utils";
import { CommandLineJob, JobBase } from "./job";
import * as cwlTsAuto from 'cwl-ts-auto'
import { DockerCommandLineJob, PodmanCommandLineJob } from "./docker";
class PathCheckingMode {
    
    static STRICT: RegExp = new RegExp("^[\w.+\,\-:@\]^\u2600-\u26FF\U0001f600-\U0001f64f]+$");
    static RELAXED: RegExp = new RegExp(".*");
    
}
export class ExpressionJob {
    builder: Builder;
    script: string;
    output_callback: OutputCallbackType | null;
    requirements: Array<CWLObjectType>;
    hints: Array<CWLObjectType>;
    outdir: string | null;
    tmpdir: string | null;
    prov_obj: any | null;
    
    constructor(
        builder: Builder,
        script: string,
        output_callback: OutputCallbackType | null,
        requirements: Array<CWLObjectType>,
        hints: Array<CWLObjectType>,
        outdir: string | null = null,
        tmpdir: string | null = null
    ) {
        this.builder = builder;
        this.requirements = requirements;
        this.hints = hints;
        this.output_callback = output_callback;
        this.outdir = outdir;
        this.tmpdir = tmpdir;
        this.script = script;
        this.prov_obj = null;
    }

    run(
        runtimeContext: RuntimeContext,
        tmpdir_lock: any | null = null
    ) {
        try {
            normalizeFilesDirs(this.builder.job);
            let ev = this.builder.do_eval(this.script);
            normalizeFilesDirs(ev as any);
            
            if (this.output_callback) {
                this.output_callback(ev as CWLObjectType, "success");
            }
        } catch (err: any) {
            _logger.warning(
                "Failed to evaluate expression:\n%s",
                err.toString(),
                {exc_info: runtimeContext.debug}
            );
            
            if (this.output_callback) {
                this.output_callback({}, "permanentFail");
            }
        }
    }
}
export class ExpressionTool extends Process {
    tool:cwlTsAuto.ExpressionTool
    constructor(tool:cwlTsAuto.ExpressionTool,loadingContext:LoadingContext){
        super(tool,loadingContext)
        this.tool = tool
    }
    *job(job_order: CWLObjectType, output_callbacks: OutputCallbackType | null, runtimeContext: RuntimeContext):
        JobsGeneratorType {
        const builder = this._init_job(job_order, runtimeContext);
        const job = new ExpressionJob(
            builder,
            this.tool["expression"],
            output_callbacks,
            this.requirements,
            this.hints,
        );
        job.prov_obj = runtimeContext.prov_obj;
        yield job;
    }
}

class AbstractOperation extends Process {
    *job(job_order: CWLObjectType, output_callbacks: OutputCallbackType | null, runtimeContext: RuntimeContext):
        JobsGeneratorType {
        throw new Error("Abstract operation cannot be executed.");
    }
}

function remove_path(f: CWLObjectType): void {
    if ("path" in f) {
        delete f["path"];
    }
}
function revmap_file(builder: Builder, outdir: string, f: CWLObjectType): CWLObjectType | null {
    if(outdir.startsWith("/")){
        outdir = fileUri(outdir);
    }    
    if (f.hasOwnProperty('location') && !f.hasOwnProperty('path')){
        let location: string = f["location"] as string;
        if (location.startsWith("file://")){
            f["path"] = uriFilePath(location)
        }
        else {
            f["location"] = builder.fs_access.join(outdir, f["location"] as string);
            return f;
        }
    }
    if (f.hasOwnProperty('dirname')) {
        delete f['dirname'];
    }
    if (f.hasOwnProperty('path')){
        let path = builder.fs_access.join(builder.outdir, f["path"] as string);
        let uripath = fileUri(path);
        delete f["path"];
        if (!(f.hasOwnProperty('basename'))){
            f["basename"] = require('path').basename(path)
        }
        if (!builder.pathmapper) {
            throw new Error("Do not call revmap_file using a builder that doesn't have a pathmapper.");
        }
        let revmap_f = builder.pathmapper.reversemap(path);
        if (revmap_f && !builder.pathmapper.mapper(revmap_f[0]).type.startsWith("Writable")) {
            f["location"] = revmap_f[1];
        }
        else if (uripath == outdir || uripath.startsWith(outdir + require('path').sep) || uripath.startsWith(outdir + "/")) {
             f["location"] = uripath;
        }
        else if (path == builder.outdir || path.startsWith(builder.outdir + require('path').sep) ||
                 path.startsWith(builder.outdir + "/")) {
            let joined_path = builder.fs_access.join(outdir, encodeURIComponent(path.substring(builder.outdir.length + 1)));
            f["location"] = joined_path;
        }
        else {
            throw new WorkflowException(`Output file path ${path} must be within designated output directory ${builder.outdir} or an input file pass through.`);
        }
        return f;
    }
    throw new WorkflowException(`Output File object is missing both 'location' and 'path' fields: ${f}`);
}
export class CallbackJob {
    job: any;
    output_callback: any | null;
    cachebuilder: any;
    outdir: string;
    prov_obj: any | null;

    constructor(job: any, output_callback: any | null, cachebuilder: any, jobcache: string) {
        this.job = job;
        this.output_callback = output_callback;
        this.cachebuilder = cachebuilder;
        this.outdir = jobcache;
        this.prov_obj = null;
    }

    run(runtimeContext: any, tmpdir_lock: any | null = null): void {
        if (this.output_callback) {
            this.output_callback(
                this.job.collect_output_ports(
                    this.job.tool["outputs"],
                    this.cachebuilder,
                    this.outdir,
                    (runtimeContext.compute_checksum != null) ? runtimeContext.compute_checksum : true
                ),
                "success"
            );
        }
    }
}
function checkAdjust(acceptRe: any, builder: Builder, fileO: CWLObjectType): CWLObjectType {
    if (!builder.pathmapper) {
        throw new Error("Do not call check_adjust using a builder that doesn't have a pathmapper.");
    }
    const path1 = builder.pathmapper.mapper(fileO["location"] as string).target;
    fileO["path"] = path1
    let basename = fileO["basename"];
    let dn = path.dirname(path1)
    const bn = path.basename(path1);
    if (fileO["dirname"] !== dn) {
        fileO["dirname"] = dn.toString();
    }
    if (basename !== bn) {
        fileO["basename"] = basename = bn.toString();
    }
    if (fileO["class"] === "File") {
        const ne = path.extname(basename);
        let nr =  path.basename(basename, ne);
        if (fileO["nameroot"] !== nr) {
            fileO["nameroot"] = nr.toString();
        }
        if (fileO["nameext"] !== ne) {
            fileO["nameext"] = ne.toString();
        }
    }
    // TODO 
    // if (!acceptRe.test(basename)) {
    //     throw new Error(
    //         `Invalid filename: ${fileO['basename']} contains illegal characters`
    //     );
    // }
    return fileO;
}

function checkValidLocations(fsAccess: StdFsAccess, ob: CWLObjectType): void {
    let location = ob["location"] as string;
    if (location.startsWith("_:")) {
        return;
    }
    if (ob["class"] === "File" && !fsAccess.isfile(location)) {
        throw new Error(`Does not exist or is not a File: '${location}'`);
    }
    if (ob["class"] === "Directory" && !fsAccess.isdir(location)) {
        throw new Error(`Does not exist or is not a Directory: '${location}'`);
    }
}

interface OutputPortsType {
    [key: string]: CWLOutputType | undefined;
}

class ParameterOutputWorkflowException extends WorkflowException {
    constructor(msg: string, port: CWLObjectType, kwargs: any) {
        super(
            `Error collecting output for parameter '${port["id"]}': ${msg}`,
        );
    }
}
const MPIRequirementName = "http://commonwl.org/cwltool#MPIRequirement"
export class CommandLineTool extends Process {
    tool: cwlTsAuto.CommandLineTool
    prov_obj: any; // placeholder type
    path_check_mode: any; // placeholder type

    constructor(toolpath_object: cwlTsAuto.CommandLineTool, loadingContext: any) { // placeholder types
        super(toolpath_object, loadingContext)
        this.tool = toolpath_object
        this.prov_obj = loadingContext.prov_obj;
        this.path_check_mode = 
            loadingContext.relax_path_checks 
            ? PathCheckingMode.RELAXED
            : PathCheckingMode.STRICT
    } 

    make_job_runner(runtimeContext: any): any { // placeholder types
        let [dockerReq, dockerRequired] = this.get_requirement("DockerRequirement");
        let [mpiReq, mpiRequired] = this.get_requirement(MPIRequirementName);

        if (!dockerReq && runtimeContext.use_container) {
            if (runtimeContext.find_default_container) {
                const default_container = runtimeContext.find_default_container(this);
                if (default_container) {
                    dockerReq = {
                        "class": "DockerRequirement",
                        "dockerPull": default_container
                    };
                    if (mpiRequired) {
                        this.hints.unshift(dockerReq);
                        dockerRequired = false;
                    } else {
                        this.requirements.unshift(dockerReq);
                        dockerRequired = true;
                    }
                }
            }
        }

        if (dockerReq && runtimeContext.use_container) {
            if (mpiReq)
                _logger.warning("MPIRequirement with containers is a beta feature");
            if(mpiReq) {
                if(mpiRequired) {
                    if(dockerRequired) 
                        throw new UnsupportedRequirement("No support for Docker and MPIRequirement both being required");
                    else {
                        _logger.warning("MPI has been required while Docker is hinted, discarding Docker hint(s)");
                        this.hints = this.hints.filter(h => h["class"] !== "DockerRequirement");
                        return CommandLineJob;
                    }
                } else {
                    if(dockerRequired) {
                        _logger.warning("Docker has been required while MPI is hinted, discarding MPI hint(s)");
                        this.hints = this.hints.filter(h => h["class"] !== MPIRequirementName);
                    } else {
                        throw new UnsupportedRequirement("Both Docker and MPI have been hinted - don't know what to do");
                    }
                }
            }
            if (runtimeContext.podman) return PodmanCommandLineJob;
            return DockerCommandLineJob;
        }

        if (dockerRequired)
                throw new UnsupportedRequirement("--no-container, but this CommandLineTool has " +
                "DockerRequirement under 'requirements'.");
        return CommandLineJob;
    }
    static make_path_mapper(
        reffiles: CWLObjectType[],
        stagedir: string,
        runtimeContext: RuntimeContext,
        separateDirs: boolean
    ): PathMapper {
        return new PathMapper(reffiles, runtimeContext.basedir, stagedir, separateDirs);
    }

    updatePathmap(outdir: string, pathmap: PathMapper, fn: CWLObjectType): void {
        if (!(fn instanceof Object)) {
            throw new WorkflowException(`Expected File or Directory object, was ${typeof fn}`);
        }
        let basename = fn["basename"] as string;
        if ("location" in fn) {
            let location = fn["location"] as string;
            if (pathmap.contains(location)) {
                pathmap.update(
                    location,
                    pathmap.mapper(location).resolved,
                    path.join(outdir, basename),
                    `${fn["writable"]? "Writable" : ""}${fn["class"]}` as string,
                    false
                );
            }
        }
        for (let sf of (fn["secondaryFiles"] || []) as CWLObjectType[]) {
            this.updatePathmap(outdir, pathmap, sf);
        }
        for (let ls of (fn["listing"] || []) as CWLObjectType[]) {
            this.updatePathmap(path.join(outdir, fn["basename"] as string), pathmap, ls);
        }
    }
    evaluate_listing_string(initialWorkdir: any, builder: Builder, classic_dirent: any, classic_listing: any, debug: any): Array<CWLObjectType> {
        let ls_evaluated = builder.do_eval(initialWorkdir["listing"]);
        let fail: any = false;
        let fail_suffix: string = "";
        if (!(ls_evaluated instanceof Array)) {
            fail = ls_evaluated;
        } else {
            let ls_evaluated2 = ls_evaluated as Array<any>;
            for (let entry of ls_evaluated2) {
                if (entry == null) {
                    if (classic_dirent) {
                        fail = entry;
                        fail_suffix = " Dirent.entry cannot return 'null' before CWL v1.2. Please consider using 'cwl-upgrader' to upgrade your document to CWL version v1.2.";
                    }
                } else if (entry instanceof Array) {
                    if (classic_listing) {
                        throw new WorkflowException("InitialWorkDirRequirement.listing expressions cannot return arrays of Files or Directories before CWL v1.1. Please considering using 'cwl-upgrader' to upgrade your document to CWL v1.1' or later.");
                    } else {
                        for (let entry2 of entry) {
                            if (!(entry2 instanceof Object && ("class" in entry2 && entry2["class"] == "File" || "Directory"))) {
                                fail = `an array with an item ('${entry2}') that is not a File nor a Directory object.`;
                            }
                        }
                    }
                } else if (!(entry instanceof Object && ("class" in entry && (entry["class"] == "File" || "Directory") || "entry" in entry))) {
                    fail = entry;
                }
            }
        }
        if (fail !== false) {
            let message = "Expression in a 'InitialWorkdirRequirement.listing' field must return a list containing zero or more of: File or Directory objects; Dirent objects";
            if (classic_dirent) {
                message += ". ";
            } else {
                message += "; null; or arrays of File or Directory objects. ";
            }
            message += `Got ${fail} among the results from `;
            message += `${initialWorkdir['listing'].trim()}.` + fail_suffix;
            throw new WorkflowException(message);
        }
        return ls_evaluated as Array<CWLObjectType>;
    }
    evaluate_listing_expr_or_dirent(initialWorkdir: any, builder: Builder, classic_dirent: any, classic_listing: any, debug: any): Array<CWLObjectType> {
        let ls: Array<CWLObjectType> = [];
        
        for (let t of initialWorkdir["listing"] as Array<string | CWLObjectType>) {
            if (t instanceof Object && "entry" in t) {
                let entry_field: string = t["entry"] as string;
                let entry = builder.do_eval(entry_field, false);
                if (entry === null) {
                    continue;
                }
                if (entry instanceof Array) {
                    if (classic_listing) {
                        throw new WorkflowException(
                            "'entry' expressions are not allowed to evaluate " +
                            "to an array of Files or Directories until CWL " +
                            "v1.2. Consider using 'cwl-upgrader' to upgrade " +
                            "your document to CWL version 1.2."
                        );
                    }
                    let filelist = true;
                    for (let e of entry) {
                        if (!(e instanceof Object) || !["File", "Directory"].includes(e["class"])) {
                            filelist = false;
                            break;
                        }
                    }
                    if (filelist) {
                        if ("entryname" in t) {
                            throw new WorkflowException(
                                "'entryname' is invalid when 'entry' returns list of File or Directory"
                            );
                        }
                        for (let e of entry) {
                            let ec: CWLObjectType = e as CWLObjectType;
                            ec["writable"] = t["writable"] || false;
                        }
                        ls.push(...entry as Array<CWLObjectType>);
                        continue;
                    }
                }
                let et: CWLObjectType = {} as CWLObjectType;
                
                if (entry instanceof Object && ["File", "Directory"].includes(entry["class"])) {
                    et["entry"] = entry as CWLOutputType;
                } else {
                    if (typeof entry === "string") {
                        et["entry"] = entry;
                    } else {
                        if (classic_dirent) {
                            throw new WorkflowException(
                                `'entry' expression resulted in something other than number, object or array besides a single File or Dirent object. In CWL v1.2+ this would be serialized to a JSON object. However this is a document. If that is the desired result then please consider using 'cwl-upgrader' to upgrade your document to CWL version 1.2. Result of ${entry_field} was ${entry}.`
                            );
                        }
                        et["entry"] = JSON.stringify(entry, null, "");
                    }
                }
                
                if (t.hasOwnProperty("entryname")) {
                    
                    let entryname_field = t["entryname"] as string;
                    if (entryname_field.includes('${') || entryname_field.includes('$(')) {
                        let en = builder.do_eval(t["entryname"] as string);
                        if (typeof en !== "string") {
                            throw new WorkflowException(
                                `'entryname' expression must result a string. Got ${en} from ${entryname_field}`
                            );
                        }
                        et["entryname"] = en;
                    } else {
                        et["entryname"] = entryname_field;
                    }
                } else {
                    et["entryname"] = undefined;
                }
                et["writable"] = t["writable"] || false;
                ls.push(et);
            } else {
                let initwd_item = builder.do_eval(t);
                if (!initwd_item) {
                    continue;
                }
                if (initwd_item instanceof Array) {
                    ls.push(...initwd_item as Array<CWLObjectType>);
                } else {
                    ls.push(initwd_item as CWLObjectType);
                }
            }
        }
        return ls;
    }
    check_output_items(initialWorkdir: CWLObjectType, ls: CWLObjectType[], debug: boolean): void {
        for (let i = 0; i < ls.length; i++) {
            const t2 = ls[i];
            if (!(t2 instanceof Map)) {
                throw new WorkflowException(
                    `Entry at index ${i} of listing is not a record, was ${typeof t2}`
                );
            }

            if (!("entry" in t2)) {
                continue;
            }

            // Dirent
            if (typeof t2["entry"] === "string") {
                if (!t2["entryname"]) {
                    throw new WorkflowException(
                        `Entry at index ${i} of listing missing entryname`
                    );
                }
                ls[i] = {
                    "class": "File",
                    "basename": t2["entryname"],
                    "contents": t2["entry"],
                    "writable": t2["writable"],
                };
                continue;
            }

            if (!(t2["entry"] instanceof Map)) {
                throw new WorkflowException(
                    `Entry at index ${i} of listing is not a record, was ${typeof t2["entry"]}`
                );
            }

            if (!["File", "Directory"].includes(t2["entry"].get("class"))) {
                throw new WorkflowException(
                    `Entry at index ${i} of listing is not a File or Directory object, was ${t2}`
                );
            }

            if (t2.get("entryname") || t2.get("writable")) {
                const t3 = JSON.parse(JSON.stringify(t2));
                let t2entry = t3["entry"] as CWLObjectType;
                if (t3.get("entryname")) {
                    t2entry["basename"] = t3["entryname"];
                }
                t2entry["writable"] = t3.get("writable");
            }

            ls[i] = t2["entry"] as CWLObjectType;
        }
    }
    check_output_items2(initialWorkdir:CWLObjectType, ls: Array<CWLObjectType>, cwl_version:string, debug:boolean) {
        for (let i = 0; i < ls.length; i++) {
            const t3 = ls[i];
            if (!["File", "Directory"].includes(t3["class"] as string)) {
                throw new WorkflowException(
                    `Entry at index ${i} of listing is not a Dirent, File or ` +
                    `Directory object, was ${t3}.`);
            }
            if (!("basename" in t3)) continue;
            const basename = path.normalize(t3["basename"] as string);
            t3["basename"] = basename;
            if (basename.startsWith("../")) {
                throw new WorkflowException(
                    `Name ${basename} at index ${i} of listing is invalid, `+
                    `cannot start with '../'`);
            }
            // if (basename.startsWith("/")) {
            // if (cwl_version && ORDERED_VERSIONS.indexOf(cwl_version) < ORDERED_VERSIONS.indexOf("v1.2.0-dev4")) {
            // throw new WorkflowException(
            //     `Name ${basename!r} at index ${i} of listing is invalid, `+
            //     `paths starting with '/' are only permitted in CWL 1.2 ` +
            //     `and later. Consider changing the absolute path to a relative ` +
            //     `path, or upgrade the CWL description to CWL v1.2 using https://pypi.org/project/cwl-upgrader/`);
            //     }
            //     const [req, is_req] = this.get_requirement("DockerRequirement");
            //     if (is_req !== true) {
            //         throw new WorkflowException(
            //             `Name ${basename!r} at index ${i} of listing is invalid, ` +
            //             `name can only start with '/' when DockerRequirement is in 'requirements'.`);
            //     }
            // }
        }
    }
    setup_output_items(initialWorkdir: CWLObjectType, builder: Builder, ls: CWLObjectType[], debug: boolean): void {
        for (let entry of ls) {
            if ("basename" in entry) {
                let basename = <string>entry["basename"];
                entry["dirname"] = path.join(builder.outdir, path.dirname(basename));
                entry["basename"] = path.basename(basename);
            }
            normalizeFilesDirs(entry);
            this.updatePathmap(
                (entry["dirname"] || builder.outdir) as string,
                <PathMapper>builder.pathmapper,
                entry,
            );
            if ("listing" in entry) {

                function remove_dirname(d: CWLObjectType): void {
                    if ("dirname" in d) {
                        delete d["dirname"];
                    }
                }

                visit_class(
                    entry["listing"],
                    ["File", "Directory"],
                    remove_dirname,
                );
            }

            visit_class(
                [builder.files, builder.bindings],
                ["File", "Directory"],
                (val)=>(checkAdjust(this.path_check_mode.value, builder,val)
            ))
        }
    }

    _initialworkdir(j: JobBase, builder: Builder): void {
        let [initialWorkdir,_] = this.get_requirement("InitialWorkDirRequirement");
        if (initialWorkdir == undefined) {
            return;
        }
        let debug = _logger.isDebugEnabled()
        let classic_dirent = false
        let classic_listing = false

        let ls: CWLObjectType[] = [];
        if (typeof initialWorkdir["listing"] == 'string') {
            ls = this.evaluate_listing_string(initialWorkdir, builder, classic_dirent, classic_listing, debug);
        } else {
            ls = this.evaluate_listing_expr_or_dirent(initialWorkdir, builder, classic_dirent, classic_listing, debug);
        }

        this.check_output_items(initialWorkdir, ls, debug);

        this.check_output_items2(initialWorkdir, ls, "cwl_version", debug);

        j.generatefiles["listing"] = ls;
        this.setup_output_items(initialWorkdir,builder,  ls, debug);
    }
    cache_context(runtimeContext:RuntimeContext) {
        // TODO cache not implemented
        // let cachecontext = runtimeContext.copy();
        // cachecontext.outdir = "/out";
        // cachecontext.tmpdir = "/tmp"; // nosec
        // cachecontext.stagedir = "/stage";
        // let cachebuilder = this._init_job(job_order, cachecontext);
        // cachebuilder.pathmapper = new PathMapper(
        //     cachebuilder.files,
        //     runtimeContext.basedir,
        //     cachebuilder.stagedir,
        //     false 
        // );
        // let _check_adjust = partial(check_adjust, this.path_check_mode.value, cachebuilder);
        // let _checksum = partial(
        //     compute_checksums,
        //     runtimeContext.make_fs_access(runtimeContext.basedir),
        // );
        // visit_class(
        //     [cachebuilder.files, cachebuilder.bindings],
        //     ["File", "Directory"],
        //     _check_adjust,
        // );
        // visit_class([cachebuilder.files, cachebuilder.bindings], ["File"], _checksum);

        // let cmdline = flatten(list(map(cachebuilder.generate_arg, cachebuilder.bindings)));
        // let [docker_req, ] = this.get_requirement("DockerRequirement");
        // let dockerimg;
        // if (docker_req !== undefined && runtimeContext.use_container) {
        //     dockerimg = docker_req["dockerImageId"] || docker_req["dockerPull"];
        // } else if (runtimeContext.default_container !== null && runtimeContext.use_container) {
        //     dockerimg = runtimeContext.default_container;
        // } else {
        //     dockerimg = null;
        // }

        // if (dockerimg !== null) {
        //     cmdline = ["docker", "run", dockerimg].concat(cmdline);
        //     // not really run using docker, just for hashing purposes
        // }

        // let keydict: { [key: string]: any } = {
        //     "cmdline": cmdline
        // };

        // for (let shortcut of ["stdin", "stdout", "stderr"]) {
        //     if (shortcut in this.tool) {
        //         keydict[shortcut] = this.tool[shortcut];
        //     }
        // }

        // let calc_checksum = (location: string): string | undefined => {
        //     for (let e of cachebuilder.files) {
        //         if (
        //             "location" in e &&
        //             e["location"] === location &&
        //             "checksum" in e &&
        //             e["checksum"] != "sha1$hash"
        //         ) {
        //             return e["checksum"] as string;
        //         }
        //     }
        //     return undefined;
        // }

        // let remove_prefix = (s: string, prefix: string): string => {
        //     return s.startsWith(prefix) ? s.slice(prefix.length) : s;
        // }

        // for (let [location, fobj] of Object.entries(cachebuilder.pathmapper)) {
        //     if (fobj.type === "File") {
        //         let checksum = calc_checksum(location);
        //         let fobj_stat = fs.statSync(fobj.resolved);
        //         let path = remove_prefix(fobj.resolved, runtimeContext.basedir + "/");
        //         if (checksum !== null) {
        //             keydict[path] = [fobj_stat.size, checksum];
        //         } else {
        //             keydict[path] = [
        //                 fobj_stat.size,
        //                 Math.round(fobj_stat.mtimeMs),
        //             ];
        //         }
        //     }
        // }

        // let interesting = new Set([
        //     "DockerRequirement",
        //     "EnvVarRequirement",
        //     "InitialWorkDirRequirement",
        //     "ShellCommandRequirement",
        //     "NetworkAccess",
        // ]);
        // for (let rh of [this.original_requirements, this.original_hints]) {
        //     for (let r of rh.slice().reverse()) {
        //         let cls = r["class"] as string
        //         if (interesting.has(cls) && !(cls in keydict)) {
        //             keydict[cls] = r;
        //         }
        //     }
        // }

        // let keydictstr = json_dumps(keydict, { separators: (",", ":"), sort_keys: true });
        // let cachekey = hashlib.md5(keydictstr.encode("utf-8")).hexdigest(); // nosec

        // _logger.debug(`[job ${jobname}] keydictstr is ${keydictstr} -> ${cachekey}`);

        // let jobcache = path.join(runtimeContext.cachedir, cachekey);

        // // Create a lockfile to manage cache status.
        // let jobcachepending = `${jobcache}.status`;
        // let jobcachelock = null;
        // let jobstatus = null;

        // // Opens the file for read/write, or creates an empty file.
        // jobcachelock = open(jobcachepending, "a+");

        // // get the shared lock to ensure no other process is trying
        // // to write to this cache
        // shared_file_lock(jobcachelock);
        // jobcachelock.seek(0);
        // jobstatus = jobcachelock.read();

        // if (os.path.isdir(jobcache) && jobstatus === "success") {
        //     if (docker_req && runtimeContext.use_container) {
        //         cachebuilder.outdir = runtimeContext.docker_outdir || random_outdir();
        //     } else {
        //         cachebuilder.outdir = jobcache;
        //     }

        //     _logger.info(`[job ${jobname}] Using cached output in ${jobcache}`);
        //     yield new CallbackJob(this, output_callbacks, cachebuilder, jobcache);
        //     // we're done with the cache so release lock
        //     jobcachelock.close();
        //     return null;
        // } else {
        //     _logger.info(`[job ${jobname}] Output of job will be cached in ${jobcache}`);

        //     // turn shared lock into an exclusive lock since we'll
        //     // be writing the cache directory
        //     upgrade_lock(jobcachelock);

        //     shutil.rmtree(jobcache, true);
        //     fs.makedir(jobcache);
        //     runtimeContext = runtimeContext.copy();
        //     runtimeContext.outdir = jobcache;

        //     let update_status_output_callback = (
        //         output_callbacks: OutputCallbackType,
        //         jobcachelock: TextIO,
        //         outputs: Optional[CWLObjectType],
        //         processStatus: string,
        //     ) => {
        //         // save status to the lockfile then release the lock
        //         jobcachelock.seek(0);
        //         jobcachelock.truncate();
        //         jobcachelock.write(processStatus);
        //         jobcachelock.close();
        //         output_callbacks(outputs, processStatus);
        //     }

        //     output_callbacks = partial(
        //         update_status_output_callback, output_callbacks, jobcachelock
        //     );
        //     return output_callbacks;
        // }
    }
handle_tool_time_limit(builder: Builder, j: JobBase, debug: boolean): void {
    const [timelimit, _] = this.get_requirement("ToolTimeLimit");
    if (timelimit == null) {
        return;
    }

    let limit_field = (<{ [key: string]: string | number }>timelimit)["timelimit"];
    if (typeof limit_field === "string") {
        let timelimit_eval = builder.do_eval(limit_field);
        if (timelimit_eval && typeof timelimit_eval !== "number") {
            throw new WorkflowException(
                `'timelimit' expression must evaluate to a long/int. Got 
                ${timelimit_eval} for expression ${limit_field}.`
            )
        } else {
            let timelimit_eval = limit_field;
        }
        if (typeof timelimit_eval !== "number" || timelimit_eval < 0) {
            throw new WorkflowException(
                `timelimit must be an integer >= 0, got: ${timelimit_eval}`
            )
        }
        j.timelimit = timelimit_eval;
    }
}

handle_network_access(builder: Builder, j: JobBase, debug: boolean): void {
    const [networkaccess, _] = this.get_requirement("NetworkAccess");
    if (networkaccess == null) {
        return;
    }
    let networkaccess_field = networkaccess["networkAccess"];
    if (typeof networkaccess_field === "string") {
        let networkaccess_eval = builder.do_eval(networkaccess_field);
        if (typeof networkaccess_eval !== "boolean") {
            throw new WorkflowException(
                `'networkAccess' expression must evaluate to a bool. 
                Got ${networkaccess_eval} for expression ${networkaccess_field}.`
            )
        } else {
            let networkaccess_eval = networkaccess_field;
        }
        if (typeof networkaccess_eval !== "boolean") {
            throw new WorkflowException(
                `networkAccess must be a boolean, got: ${networkaccess_eval}.`
            )
        }
        j.networkaccess = networkaccess_eval;
    }
}
handle_env_var(builder: Builder, debug: boolean): any {
    let [evr, _] = this.get_requirement("EnvVarRequirement");
    if (evr === undefined) {
        return {};
    }
    let required_env = {};
    const envDef = evr["envDef"] as CWLObjectType[]
    for (let eindex = 0; eindex < envDef.length; eindex++) {
        let t3 = envDef[eindex];
        let env_value_field = t3["envValue"];
        let env_value : string | undefined = undefined
        if (isString(env_value_field) &&( env_value_field.includes("${") || env_value_field.includes("$("))) {
            let env_value_eval = builder.do_eval(env_value_field);
            if (typeof env_value_eval !== 'string') {
                throw new WorkflowException(
                    "'envValue expression must evaluate to a str. " + 
                    `Got ${env_value_eval} for expression ${env_value_field}.`
                );
            }
            env_value = env_value_eval;
        } else {
            env_value = env_value_field as string;
        }
        required_env[t3["envName"] as string] = env_value;
    }
    return required_env;
    }
    setup_command_line(builder: Builder, j: JobBase, debug: boolean) {
        const [shellcmd, _] = this.get_requirement("ShellCommandRequirement");
        if (shellcmd !== null) {
            let cmd:string[] = [];  // type: List[str]
            for (let b of builder.bindings) {
                let arg = builder.generate_arg(b);
                if (b.get("shellQuote", true)) {
                    arg = aslist(arg).map(a=>quote(a));
                }
                cmd = [...cmd, ...aslist(arg)];
            }
            j.command_line = ["/bin/sh", "-c", cmd.join(" ")];
        } else {
            const cmd = builder.bindings.map(binding => builder.generate_arg(binding)).flat()
            j.command_line = cmd;
        }
    }

    handle_mpi_require(runtimeContext: RuntimeContext, builder: Builder, j: JobBase, debug: boolean) {
        // TODO mpi not implemented
        // let mpi;
        // [mpi, _] = this.get_requirement(MPIRequirementName);

        // if (mpi === null) {
        //     return;
        // }

        // let np = mpi.get("processes", runtimeContext.mpi_config.default_nproc);
        // if (typeof np === 'string') {
        //     let np_eval = builder.do_eval(np);
        //     if (typeof np_eval !== 'number') {
        //         throw new SourceLine(mpi, "processes", WorkflowException, debug).makeError(
        //         `${MPIRequirementName} needs 'processes' expression to ` +
        //         `evaluate to an int, got ${np_eval} for expression ${np}.`
        //         );
        //     }
        //     np = np_eval;
        // }
        // j.mpi_procs = np;
    }
    setup_std_io(builder: any, j: any, reffiles: any[], debug: boolean): void {
        if(this.tool.stdin) {
            const stdin_eval = builder.do_eval(this.tool["stdin"]);
            if(!(typeof stdin_eval === 'string' || stdin_eval === null)) {
                throw new Error(
                    `'stdin' expression must return a string or null. Got ${stdin_eval} for ${this.tool['stdin']}.`
                );
            }
            j.stdin = stdin_eval;
            if(j.stdin) {
                reffiles.push({"class": "File", "path": j.stdin});
            }
        }

        if(this.tool.stderr) {
            const stderr_eval = builder.do_eval(this.tool.stderr);
            if(typeof stderr_eval !== 'string') {
                throw new Error(
                    `'stderr' expression must return a string. Got ${stderr_eval} for ${this.tool['stderr']}.`
                );
            }
            j.stderr = stderr_eval;
            if(j.stderr) {
                if(require('path').isAbsolute(j.stderr) || j.stderr.includes("..")) {
                    throw new Error(
                        `stderr must be a relative path, got '${j.stderr}'`
                    );
                }
            }
        }

        if(this.tool.stdout) {
            const stdout_eval = builder.do_eval(this.tool.stdout);
            if(typeof stdout_eval !== 'string') {
                throw new Error(
                    `'stdout' expression must return a string. Got ${stdout_eval} for ${this.tool['stdout']}.`
                );
            }
            j.stdout = stdout_eval;
            if(j.stdout) {
                if(require('path').isAbsolute(j.stdout) || j.stdout.includes("..") || !j.stdout) {
                    throw new Error(
                        `stdout must be a relative path, got '${j.stdout}'`
                    );
                }
            }
        }
    }
    handleMutationManager(builder: Builder, j: JobBase): Record<string, CWLObjectType> | undefined {
        let readers: Record<string, CWLObjectType> = {};
        let muts: Set<string> = new Set();
        // TODO MutationManager is not implemented
        // if (builder.mutationManager === null) {
        //     return readers;
        // }

        // let registerMut = (f: CWLObjectType) => {
        //     let mm = builder.mutationManager as MutationManager;
        //     muts.add(f['location'] as string);
        //     mm.registerMutation(j.name, f);
        // }

        // let registerReader = (f: CWLObjectType) => {
        //     let mm = builder.mutationManager as MutationManager;
        //     if (!muts.has(f['location'] as string)) {
        //         mm.registerReader(j.name, f);
        //         readers[f['location'] as string] = JSON.parse(JSON.stringify(f));
        //     }
        // }

        // for (let li of j.generatefiles['listing']) {
        //     if (li.get('writable') && j.inplaceUpdate) {
        //         adjustFileObjs(li, registerMut)
        //         adjustDirObjs(li, registerMut)
        //     } else {
        //         adjustFileObjs(li, registerReader)
        //         adjustDirObjs(li, registerReader)
        //     }
        // }
        

        // adjustFileObjs(builder.files, registerReader);
        // adjustFileObjs(builder.bindings, registerReader);
        // adjustDirObjs(builder.files, registerReader);
        // adjustDirObjs(builder.bindings, registerReader);
        
        return readers;
    }
    *job(
        this: any,
        job_order: CWLObjectType,
        output_callbacks: OutputCallbackType | null,
        runtimeContext: RuntimeContext
    ): Generator<JobBase | CallbackJob, void, unknown> {
        const [workReuse, ] = this.get_requirement("WorkReuse");
        const enableReuse = workReuse ? workReuse.get("enableReuse", true) : true;

        const jobname = uniquename(runtimeContext.name || shortname(this.tool.get("id", "job")));
        if (runtimeContext.cachedir && enableReuse) {
            output_callbacks = this.cache_context();
            if (output_callbacks) {
                return;
            }
        }
        const builder = this._init_job(job_order, runtimeContext);

        const reffiles = JSON.parse(JSON.stringify(builder.files));

        const j = this.make_job_runner(runtimeContext)(
            builder,
            builder.job,
            this.make_path_mapper,
            this.requirements,
            this.hints,
            jobname
        );

        j.prov_obj = this.prov_obj;

        j.successCodes = this.tool.get("successCodes", []);
        j.temporaryFailCodes = this.tool.get("temporaryFailCodes", []);
        j.permanentFailCodes = this.tool.get("permanentFailCodes", []);

        const debug = _logger.isDebugEnabled()

        if (debug) {
            _logger.debug(
                `[job ${j.name}] initializing from ${this.tool.get("id", "")}${
                    runtimeContext.part_of ? ` as part of ${runtimeContext.part_of}` : ""
                }`
            );
            _logger.debug(`[job ${j.name}] ${JSON.stringify(builder.job, null, 4)}`);
        }

        builder.pathmapper = this.make_path_mapper(reffiles, builder.stagedir, runtimeContext, true);
        builder.requirements = j.requirements;

        const _check_adjust = (val) =>checkAdjust(this.path_check_mode.value, builder,val);

        visit_class([builder.files, builder.bindings], ["File", "Directory"], _check_adjust);

        this._initialworkdir(j, builder);

        if (debug) {
            _logger.debug(
                `[job ${j.name}] path mappings is ${JSON.stringify(
                    builder.pathmapper.files().reduce((obj: any, p: string) => {
                        obj[p] = builder.pathmapper.mapper(p);
                        return obj;
                    }, {}),
                    null,
                    4
                )}`
            );
        }

        this.setup_std_io(builder,j,reffiles, debug);

        if (debug) {
            _logger.debug(
                `[job ${j.name}] command line bindings is ${JSON.stringify(builder.bindings, null, 4)}`
            );
        }

        const [dockerReq, ] = this.get_requirement("DockerRequirement");
        if (dockerReq && runtimeContext.use_container) {
            j.outdir = runtimeContext.getOutdir();
            j.tmpdir = runtimeContext.getTmpdir();
            j.stagedir = runtimeContext.createTmpdir();
        } else {
            j.outdir = builder.outdir;
            j.tmpdir = builder.tmpdir;
            j.stagedir = builder.stagedir;
        }

        const [inplaceUpdateReq, _] = this.get_requirement("InplaceUpdateRequirement");
        if (inplaceUpdateReq) {
            j.inplace_update = inplaceUpdateReq["inplaceUpdate"];
        }
        normalizeFilesDirs(j.generatefiles);

        const readers = this.handle_mutation_manager(builder, j);
        this.handle_tool_time_limit(builder,j,debug);

        this.handle_network_access(builder,j,debug);

        const required_env = this.handle_env_var(builder,debug);
        j.prepare_environment(runtimeContext, required_env);

        this.setup_command_line(builder, j, debug);

        j.pathmapper = builder.pathmapper;
        j.collect_outputs =(val)=>
            this.collect_output_ports(
            this.tool["outputs"],
            builder,
            getDefault(runtimeContext.compute_checksum, true),
            jobname,
            readers,
            val)
        j.output_callback = output_callbacks;

        this.handle_mpi_require(runtimeContext,builder,j ,debug);
        yield j;
    }
    collect_output_ports(
        ports: {} | Set<CWLObjectType>,
        builder: Builder,
        outdir: string,
        rcode: number,
        compute_checksum: boolean = true,
        jobname: string = "",
        readers: MutableMapping<CWLObjectType> | null = null,
    ): OutputPortsType {
        let ret: OutputPortsType = {};
        const debug = _logger.isDebugEnabled()
        const cwl_version:string = "v1.2"
        if (cwl_version != "v1.0") {
            builder.resources["exitCode"] = rcode;
        }
        try {
            const fs_access = builder.make_fs_access(outdir);
            const custom_output = fs_access.join(outdir, "cwl.output.json");
            if (fs_access.exists(custom_output)) {
                const f = fs_access.open(custom_output, "r");
                ret = JSON.parse(f as any);
                if (debug) {
                    _logger.debug(
                        "Raw output from " + custom_output + ": " + JSON.stringify(ret, null, 4),
                    );
                }
            } else {
                // for (let i = 0; i < ports.length; i++) {
                //     let port = ports[i];
                //     const fragment = shortname(port["id"]);
                //     ret[fragment] = this.collect_output(
                //         port,
                //         builder,
                //         outdir,
                //         fs_access,
                //         compute_checksum,
                //     );
                // }
            }
            if (ret) {
                const revmap = (val)=>revmap_file(builder, outdir,val); 
                // adjustDirObjs(ret, trim_listing);
                visit_class(ret, ["File", "Directory"], revmap);
                visit_class(ret, ["File", "Directory"], remove_path);
                normalizeFilesDirs(ret);
                visit_class(
                    ret,
                    ["File", "Directory"],
                    (val)=>checkValidLocations(fs_access,val),
                );

                if (compute_checksum) {
                    adjustFileObjs(ret, (val)=>compute_checksums(fs_access,val));
                }
                // const expected_schema = ((this.names.get_name("outputs_record_schema", null)) as Schema);
                // validate_ex(
                //     expected_schema,
                //     ret,
                //     false,
                //     _logger_validation_warnings,
                //     INPUT_OBJ_VOCAB,
                // );
                if (ret && builder.mutation_manager) {
                    adjustFileObjs(ret, builder.mutation_manager.set_generation);
                }
                return ret || {};
            }
        }
        catch (e) {
            if (e instanceof ValidationException) {
                throw new WorkflowException(
                    "Error validating output record. " + e + "\n in " + JSON.stringify(ret, null, 4)
                );
            }
            throw e;
        }
        finally {
            if (builder.mutation_manager && readers) {
                for (let r of Object.values(readers)) {
                    builder.mutation_manager.release_reader(jobname, r);
                }
            }
        }
        return ret
    }
    async glob_output(builder: Builder, binding, debug: boolean, outdir: string,
    fs_access: StdFsAccess, compute_checksum: boolean, revmap): Promise<[CWLOutputType[], string[]]> {
        let r: Array<CWLOutputType> = [];
        let globpatterns: Array<string> = [];

        if (!binding["glob"]) {
            return [r, globpatterns];
        }

        try {
            for (let gb of aslist(binding["glob"])) {
                gb = builder.do_eval(gb);
                if (gb) {
                    let gb_eval_fail = false;
                    if (typeof gb !== "string") {
                        if (Array.isArray(gb)) {
                            for (let entry of gb) {
                                if (typeof entry !== "string") {
                                    gb_eval_fail = true;
                                }
                            }
                        } else {
                            gb_eval_fail = true;
                        }
                    }

                    if (gb_eval_fail) {
                        throw new WorkflowException(
                            "Resolved glob patterns must be strings or list of strings, not "+
                            `${gb!} from ${binding['glob']!}`
                        );
                    }

                    globpatterns.push(...aslist(gb));
                }
            }

            for (let gb of globpatterns) {
                if (gb.startsWith(builder.outdir)) {
                    gb = gb.substring(builder.outdir.length + 1);
                } else if (gb === ".") {
                    gb = outdir;
                } else if (gb.startsWith("/")) {
                    throw new WorkflowException("glob patterns must not start with '/'");
                }

                try {
                    let prefix = fs_access.glob(outdir);
                    let sorted_glob_result = fs_access.glob(fs_access.join(outdir, gb)).sort();

                    r.push(
                        ...sorted_glob_result.map((g) => ({
                            "location": g,
                            "path": fs_access.join(
                                builder.outdir,
                                decodeURIComponent(g.substring(prefix[0].length + 1)),
                            ),
                            "basename": g,
                            "nameroot": splitext(g)[0],
                            "nameext": splitext(g)[1],
                            "class": fs_access.isfile(g) ? "File" : "Directory"
                        })
                    ));
                } catch (e) {
                    console.error("Unexpected error from fs_access");
                    throw e;
                }
            }

            for (let files of r) {
                let rfile = { ...files as any };
                revmap(rfile);
                if (files["class"] === "Directory") {
                    let ll = binding["loadListing"] || builder.loadListing;
                    if (ll && ll !== "no_listing") {
                        get_listing(fs_access, files, (ll === "deep_listing"));
                    }
                } else {
                    if (binding["loadContents"]) {
                        let f:any = undefined
                        try {
                            f = fs_access.open(rfile["location"] as string, "rb");
                            files["contents"] = contentLimitRespectedRead(f)
                        } finally {
                            if(f){
                                f.close();
                            }
                        }
                    }

                    if (compute_checksum) {
                        await compute_checksums(fs_access,rfile)
                    }

                    files["size"] = fs_access.size(rfile["location"] as string);
                }
            }

            return [r, globpatterns];
        } catch (e) {
            throw e;
        }
        throw new Error("unexpected path")
    }
    collect_secondary_files(schema: CWLObjectType,builder:Builder,result: CWLOutputType | null,debug:boolean,
        fs_access: StdFsAccess,revmap: any): void {
            for (let primary of aslist(result)) {
                if (primary instanceof Object) {
                    if (!primary["secondaryFiles"]) {
                        primary["secondaryFiles"] = [];
                    }
                    let pathprefix = primary["path"].substring(0, primary["path"].lastIndexOf(path.sep) + 1);
                    for (let sf of aslist(schema["secondaryFiles"])) {
                        let sf_required: boolean;
                        if ("required" in sf) {
                            let sf_required_eval = builder.do_eval(
                                sf["required"], primary
                            );
                            if (!(typeof sf_required_eval === 'boolean' ||
                                sf_required_eval === null)) {
                                throw new WorkflowException(
                                    `Expressions in the field 'required' must evaluate to a Boolean (true or false) or None. Got ${sf_required_eval} for ${sf['required']}.`
                                );
                            }
                            sf_required = sf_required_eval as boolean || false;
                        } else {
                            sf_required = false;
                        }

                        let sfpath;
                        if (sf["pattern"].includes("$(") || sf["pattern"].includes("${")) {
                            sfpath = builder.do_eval(sf["pattern"], primary);
                        } else {
                            sfpath = substitute(primary["basename"], sf["pattern"]);
                        }

                        for (let sfitem of aslist(sfpath)) {
                            if (!sfitem) {
                                continue;
                            }
                            if (typeof sfitem === 'string') {
                                sfitem = {"path": pathprefix + sfitem};
                            }
                            let original_sfitem = JSON.parse(JSON.stringify(sfitem));
                            if (!fs_access.exists(revmap(sfitem)["location"])&& sf_required) {
                                throw new WorkflowException(
                                    `Missing required secondary file '${original_sfitem["path"]}'`
                                );
                            }
                            if ("path" in sfitem && !("location" in sfitem)) {
                                revmap(sfitem);
                            }
                            if (fs_access.isfile(sfitem["location"])) {
                                sfitem["class"] = "File";
                                primary["secondaryFiles"].push(sfitem);
                            } else if (fs_access.isdir(sfitem["location"])) {
                                sfitem["class"] = "Directory";
                                primary["secondaryFiles"].push(sfitem);
                            }
                        }
                    }
                }
            }
        }
    handle_output_format(schema: CWLObjectType, builder: Builder, result: CWLObjectType, debug: boolean): void {
        if ("format" in schema) {
            const format_field: string = <string>schema["format"];
            if (format_field.includes("$(") || format_field.includes("${")) {
                aslist(result).forEach((primary, index) => {
                    const format_eval: any = builder.do_eval(format_field, primary);
                    if (typeof format_eval !== 'string') {
                        let message: string = `'format' expression must evaluate to a string. Got ${format_eval} from ${format_field}.`;
                        if (Array.isArray(result)) {
                            message += ` 'self' had the value of the index ${index} result: ${primary}.`;
                        }
                        throw new WorkflowException(message);
                    }
                    primary["format"] = format_eval;
                });
            } else {
                aslist(result).forEach((primary) => {
                    primary["format"] = format_field;
                });
            }
        }
    }
    async collect_output(
    schema: CWLObjectType,
    builder: Builder,
    outdir: string,
    fs_access: StdFsAccess,
    compute_checksum: boolean = true,
    ): Promise<CWLOutputType | undefined> {
    let empty_and_optional: boolean = false;
    let debug = _logger.isDebugEnabled()
    let result: CWLOutputType | undefined = undefined;
    if (schema.hasOwnProperty("outputBinding")) {
        let binding = schema["outputBinding"] as Object;
        let revmap = revmap_file.bind(null, builder, outdir);
        let [r, globpatterns] = await this.glob_output(builder, binding, debug, outdir, fs_access, compute_checksum, revmap);
        let optional: boolean = false;
        let single: boolean = false;
        if (Array.isArray(schema["type"])) {
        if (schema["type"].includes("null")) {
            optional = true;
        }
        if (schema["type"].includes("File") || schema["type"].includes("Directory")) {
            single = true;
        }
        } else if (schema["type"] === "File" || schema["type"] === "Directory") {
        single = true;
        }
        let result 
        if (binding.hasOwnProperty("outputEval")) {
            try {
                result = builder.do_eval(<CWLOutputType>binding["outputEval"], { context: r });
            } 
            catch (error) {
                // Log the error here
            }
        } else {
            result = <CWLOutputType>r;
        }
        if (single) {
        try {
            if (!result && !optional) {
            throw new WorkflowException(`Did not find output file with glob pattern: ${globpatterns}.`);
            } else if (!result && optional) {
            // Do nothing
            } else if (Array.isArray(result)) {
            if (result.length > 1) {
                throw new WorkflowException("Multiple matches for output item that is a single file.");
            } else {
                result = <CWLOutputType>result[0];
            }
            }
        }
        catch (error) {
            // Log the error here
        };
        }
        if (schema.hasOwnProperty("secondaryFiles")) {
        this.collect_secondary_files(schema, builder, result, debug, fs_access, revmap);
        }
        this.handle_output_format(schema, builder, result, debug);
        adjustFileObjs(result, revmap);
        if (!result && optional && ![0, ""].includes(result)) {
        return undefined;
        }
    }
    if (!result && !empty_and_optional && typeof schema["type"] === 'object' && schema["type"]["type"] === "record") {
        let out = {};
        for (let field of <CWLObjectType[]>schema["type"]["fields"]) {
        out[shortname(<string>field["name"])] = this.collect_output(field, builder, outdir, fs_access, compute_checksum);
        }
        return out;
    }
    return result;
    }
}