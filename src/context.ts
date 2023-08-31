
import {PathMapper} from './pathmapper'
import {StdFsAccess} from './stdfsaccess';
import {DEFAULT_TMP_PREFIX, CWLObjectType, HasReqsHints,  mkdtemp, CommentedMap} from './utils';

import { Builder } from './builder'
import { Process } from './process'

class ContextBase {
    constructor(kwargs: { [key: string]: any } | null = null) {
        if (kwargs) {
            for (let [k, v] of Object.entries(kwargs)) {
                if (this.hasOwnProperty(k)) {
                    (this as any)[k] = v;
                }
            }
        }
    }
}

function make_tool_notimpl(toolpath_object: CommentedMap, loadingContext: LoadingContext): Process {
    throw new Error("Not implemented");
}

let default_make_tool = make_tool_notimpl;

function log_handler(outdir: string, base_path_logs: string, stdout_path: string | null, stderr_path: string | null): void {
    if (outdir != base_path_logs) {
        if (stdout_path) {
            let new_stdout_path = stdout_path.replace(base_path_logs, outdir);
//            shutil.copy2(stdout_path, new_stdout_path);
        }
        if (stderr_path) {
            let new_stderr_path = stderr_path.replace(base_path_logs, outdir);
  //          shutil.copy2(stderr_path, new_stderr_path);
        }
    }
}

function set_log_dir(outdir: string, log_dir: string, subdir_name: string): string {
    if (log_dir === "") {
        return outdir;
    }
    else {
        return log_dir + "/" + subdir_name;
    }
}
export class LoadingContext extends ContextBase {
    debug: boolean = false;
    metadata: CWLObjectType = {};
    requirements: Array<CWLObjectType> | null = null;
    hints: Array<CWLObjectType> | null = null;
    disable_js_validation: boolean = false;
    js_hint_options_file: string | null = null;
    do_validate: boolean = true;
    enable_dev: boolean = false;
    strict: boolean = true;
    construct_tool_object = default_make_tool;
    orcid: string = "";
    cwl_full_name: string = "";
    host_provenance: boolean = false;
    user_provenance: boolean = false;
    prov_obj: any | null = null;
    do_update: boolean | null = null;
    jobdefaults: CommentedMap | null = null;
    doc_cache: boolean = true;
    relax_path_checks: boolean = false;
    singularity: boolean = false;
    podman: boolean = false;
    eval_timeout: number = 60;
    fast_parser: boolean = false;
    skip_resolve_all: boolean = false;
    skip_schemas: boolean = false;

    constructor(kwargs?: { [key: string]: any } | null) {
        super(kwargs);
    }

    copy(): LoadingContext {
        return Object.assign(Object.create(Object.getPrototypeOf(this)), this);
    }
}
export class RuntimeContext extends ContextBase {
    outdir?: string = undefined;
    tmpdir: string = "";
    tmpdir_prefix: string = DEFAULT_TMP_PREFIX;
    tmp_outdir_prefix: string = "";
    stagedir: string = "";

    user_space_docker_cmd?: string = undefined;
    secret_store?: any = undefined;
    no_read_only: boolean = false;
    custom_net?: string = undefined;
    no_match_user: boolean = false;
    preserve_environment?: string[] = undefined;
    preserve_entire_environment: boolean = false;
    use_container: boolean = true;
    force_docker_pull: boolean = false;

    rm_tmpdir: boolean = true;
    pull_image: boolean = true;
    rm_container: boolean = true;
    move_outputs: "move" | "leave" | "copy" = "move";
    log_dir: string = "";
    set_log_dir = set_log_dir;
    log_dir_handler = log_handler;
    streaming_allowed: boolean = false;

    singularity: boolean = false;
    podman: boolean = false;
    debug: boolean = false;
    compute_checksum: boolean = true;
    name: string = "";
    default_container?: string = undefined;
    find_default_container?: any = undefined;
    cachedir?: string = undefined;
    part_of: string = "";
    basedir: string = "";
    toplevel: boolean = false;
    mutation_manager?: any = undefined;
    path_mapper = PathMapper;
    builder?: any = undefined;
    docker_outdir: string = "";
    docker_tmpdir: string = "";
    docker_stagedir: string = "";
    js_console: boolean = false;
    job_script_provider?: any = undefined;
    select_resources?: any = undefined;
    eval_timeout: number = 60;
    postScatterEval?: any = undefined;
    on_error: "stop" | "continue" = "stop";
    strict_memory_limit: boolean = false;
    strict_cpu_limit: boolean = false;
    cidfile_dir?: string = undefined;
    cidfile_prefix?: string = undefined;

    workflow_eval_lock?: any = undefined;
    research_obj?: any = undefined;
    orcid: string = "";
    cwl_full_name: string = "";
    process_run_id?: string = undefined;
    prov_obj?: any = undefined;
    default_stdout?: any = undefined;
    default_stderr?: any = undefined;

    constructor(kwargs?: any){
        super(kwargs);
        if (this.tmp_outdir_prefix == "") {
            this.tmp_outdir_prefix = this.tmpdir_prefix;
        }
    }
getOutdir(): string {
    if (this.outdir) {
        return this.outdir;
    }
    return this.createOutdir();
}

getTmpdir(): string {
    if (this.tmpdir) {
        return this.tmpdir;
    }
    return this.createTmpdir();
}

getStagedir(): string {
    if (this.stagedir) {
        return this.stagedir;
    }
    let [tmpDir, tmpPrefix] = this.tmpdir_prefix.split('/');
    return mkdtemp(tmpPrefix, tmpDir);
}

createTmpdir(): string {
    let [tmpDir, tmpPrefix] = this.tmpdir_prefix.split('/');
    return mkdtemp(tmpPrefix, tmpDir);
}

createOutdir(): string {
    let [outDir, outPrefix] = this.tmp_outdir_prefix.split('/');
    return mkdtemp(outPrefix, outDir);
}

copy(): RuntimeContext {
    return Object.assign({}, this);
}
}
export function getDefault(val: any, def: any): any {
    if (val === null) {
        return def;
    } else {
        return val;
    }
}
