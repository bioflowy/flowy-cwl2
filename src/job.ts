import * as cp from "child_process";
import * as os from "os"
import fsExtra from 'fs-extra';
import * as fs from "fs";
import * as path from "path";
import { CWLObjectType, DirectoryType, HasReqsHints, OutputCallbackType, createTmpDir, ensureWritable, ensure_non_writable, stage_files } from "./utils";
import { MapperEnt, PathMapper } from "./pathmapper";
import { RuntimeContext } from "./context";
import { _logger } from "./loghandler";
import { UnsupportedRequirement, ValueError, WorkflowException } from "./errors";
import { Builder } from "./builder";
import { v4 as uuidv4 } from 'uuid';
// ... and so on for other modules
const needsShellQuotingRe = /(^$|[\s|&;()<>\'"$@])/;

function relink_initialworkdir(
    pathmapper: PathMapper,
    host_outdir: string,
    container_outdir: string,
    inplace_update: boolean = false,
): void {
    for (let [key, vol] of pathmapper.items_exclude_children()) {
        if (!vol.staged) {
            continue;
        }
        if (vol.type in ["File", "Directory"] || (
            inplace_update && vol.type in ["WritableFile", "WritableDirectory"]
        )) {
            if (!vol.target.startsWith(container_outdir)) {
                continue;
            }
            let host_outdir_tgt = path.join(host_outdir, vol.target.substr(container_outdir.length + 1));
            if (fs.lstatSync(host_outdir_tgt).isSymbolicLink() || fs.lstatSync(host_outdir_tgt).isFile()) {
                try {
                    fs.unlinkSync(host_outdir_tgt);
                } catch (e) {
                    throw e
                    // if (e.code !== 'EPERM') throw e;
                }
            } else if (fs.lstatSync(host_outdir_tgt).isDirectory() && !vol.resolved.startsWith("_:")) {
                fs.rmdirSync(host_outdir_tgt, { recursive: true });
            }
            if (!vol.resolved.startsWith("_:")) {
                try {
                    fs.symlinkSync(vol.resolved, host_outdir_tgt);
                } catch (e) {
                    throw e
                    //if (e.code !== 'EEXIST') throw e;
                }
            }
        }
    }
}

let neverquote = (string: string, pos: number = 0, endpos: number = 0): any => {
    return null;
}
export async function _job_popen(
    commands: string[],
    stdin_path: string | undefined,
    stdout_path: string | undefined,
    stderr_path: string | undefined,
    env: { [key: string]: string },
    cwd: string,
    make_job_dir: () => string,
    job_script_contents: string | null = null,
    timelimit: number | undefined = undefined,
    name: string | undefined = undefined,
    monitor_function: ((sproc: any) => void) | null = null,
    default_stdout: any = undefined,
    default_stderr: any = undefined
):Promise<number> {
    let stdin: any = 'pipe';
    let stdout: any = default_stdout ? default_stdout : process.stderr;
    let stderr: any = default_stderr ? default_stderr : process.stderr;
    let rcode: any;
    let tm = null;

    if (stdin_path !== undefined) {
        stdin = fs.openSync(stdin_path, 'r');
    }
    if (stdout_path !== undefined) {
        stdout = fs.openSync(stdout_path, 'w');
    }
    if (stderr_path !== undefined) {
        stderr = fs.openSync(stderr_path, 'w');
    }
    const [cmd,...args] = commands
    return new Promise((resolve, reject) => {
        const child = cp.spawn(cmd,args,{
            cwd:cwd,
            env:env,
            stdio: [stdin,stdout,stderr],
            timeout:timelimit!==null?timelimit:undefined})
        if(monitor_function){
            monitor_function(child)
        }
        child.on('close', (code) => {
           resolve(code??-1);
        });
    
        child.on('error', (error) => {
          reject(error);
        });
    });
}

type CollectOutputsType = ((str: string, int: number) => CWLObjectType) | any; // Assuming functools.partial as any
export abstract class JobBase extends HasReqsHints {
    builder: Builder;
    base_path_logs:string;
    joborder: CWLObjectType;
    make_path_mapper: (param1: CWLObjectType[], param2: string, param3: RuntimeContext, param4: boolean) => PathMapper;
    requirements: CWLObjectType[];
    hints: CWLObjectType[];
    name: string;
    stdin?: string;
    stderr?: string;
    stdout?: string;
    successCodes: number[];
    temporaryFailCodes: number[];
    permanentFailCodes: number[];
    command_line: string[];
    pathmapper: PathMapper;
    generatemapper?: PathMapper;
    collect_outputs?: CollectOutputsType;
    output_callback?: OutputCallbackType;
    outdir: string;
    tmpdir: string;
    environment: {[key: string]: string};
    generatefiles: DirectoryType = { "class": "Directory", "listing": [], "basename": "" };
    stagedir?: string;
    inplace_update: boolean;
    prov_obj?: any; // ProvenanceProfile;
    parent_wf?: any; // ProvenanceProfile;
    timelimit?: number;
    networkaccess: boolean;
    mpi_procs?: number;

    constructor(
        builder: Builder,
        joborder: CWLObjectType,
        make_path_mapper: (param1: CWLObjectType[], param2: string, param3: RuntimeContext, param4: boolean) => PathMapper,
        requirements: CWLObjectType[],
        hints: CWLObjectType[],
        name: string
    ) {
        super();
        this.builder = builder;
        this.joborder = joborder;
        // TODO
        this.base_path_logs = "/tmp"
        this.stdin = undefined;
        this.stderr = undefined;
        this.stdout = undefined;
        this.successCodes = [];
        this.temporaryFailCodes = [];
        this.permanentFailCodes = [];
        this.requirements = requirements;
        this.hints = hints;
        this.name = name;
        this.command_line = [];
        this.pathmapper = new PathMapper([], "", "");
        this.make_path_mapper = make_path_mapper;
        this.generatemapper = undefined;
        this.collect_outputs = undefined;
        this.output_callback = undefined;
        this.outdir = "";
        this.tmpdir = "";
        this.environment = {};
        this.inplace_update = false;
        this.prov_obj = undefined;
        this.parent_wf = undefined;
        this.timelimit = undefined;
        this.networkaccess = false;
        this.mpi_procs = undefined;
    }

    toString(): string {
        return `CommandLineJob(${this.name})`;
    }
    abstract run(runtimeContext: RuntimeContext, tmpdir_lock?: Lock): void;

    _setup(runtimeContext: RuntimeContext): void {
    // cuda not supported now
    // let cuda_req;
    // [cuda_req, _] = this.builder.get_requirement("http://commonwl.org/cwltool#CUDARequirement");
    // if (cuda_req) {
    //     let count = cuda_check(cuda_req, Math.ceil(this.builder.resources["cudaDeviceCount"]));
    //     if (count === 0) throw new WorkflowException("Could not satisfy CUDARequirement");
    // }
    if (!fs.existsSync(this.outdir)) fs.mkdirSync(this.outdir, { recursive: true });

    const is_streamable = (file: string): boolean => {
        if (!runtimeContext.streaming_allowed) return false;
        for (const inp of (this.joborder as {[key:string]:any}).values()) {
            if (typeof inp === 'object' && inp["location"] == file) return inp["streamable"];
        }
        return false;
    }

    for (let knownfile of this.pathmapper.files()) {
        let p = this.pathmapper.mapper(knownfile);
        if (p.type == "File" && !fs.existsSync(p.resolved) && p.staged) {
            if (!(is_streamable(knownfile) && fs.statSync(p.resolved).isFIFO())) {
                throw new WorkflowException(`Input file ${knownfile} (at ${this.pathmapper.mapper(knownfile).resolved}) not found or is not a regular file.`);
            }
        }
    }

    if ("listing" in this.generatefiles) {
        runtimeContext.outdir = this.outdir;
        this.generatemapper = this.make_path_mapper(
            this.generatefiles["listing"],
            this.builder.outdir,
            runtimeContext,
            false,
        );
        // if (_logger.isEnabledFor(logging.DEBUG)) {
        //     _logger.debug(
        //         "[job %s] initial work dir %s",
        //         this.name,
        //         JSON.stringify({ p: this.generatemapper.mapper(p) for(p of this.generatemapper.files()) }, null, 4),
        //     );
        // }
    }
    this.base_path_logs = runtimeContext.set_log_dir(this.outdir, runtimeContext.log_dir, this.name)
    }
    async _execute(
        runtime: string[],
        env: { [id: string] : string },
        runtimeContext: any,
        monitor_function: ((popen: any) => void) | null = null,
    )  {
        let scr = this.get_requirement("ShellCommandRequirement")[0];

    let shouldquote = neverquote
    // needsShellQuotingRe.search;
    // if (scr !== null) {
    //     shouldquote = neverquote;
    // }

    if (this.mpi_procs) {
        let menv = runtimeContext.mpi_config;
        let mpi_runtime = [
            menv.runner,
            menv.nproc_flag,
            this.mpi_procs.toString(),
            ...menv.extra_flags
        ];
        runtime = [...mpi_runtime, ...runtime];
        menv.pass_through_env_vars(env);
        menv.set_env_vars(env);
    }

    _logger.info(
        "[job %s] %s$ %s%s%s%s",
        this.name,
        this.outdir,
        " \\\n    " + 
        runtime.concat(this.command_line)
            .map(arg => shouldquote(arg.toString()) ? arg.toString() : arg.toString()) // TODO
            .join(' '),
        this.stdin ? " < " + this.stdin : "",
        this.stdout ? " > " + path.join(this.base_path_logs, this.stdout) : "",
        this.stderr ? " 2> " + path.join(this.base_path_logs, this.stderr) : "",
    );
    if (this.joborder !== null && runtimeContext.research_obj !== null) {
        let job_order = this.joborder;
        if (
            runtimeContext.process_run_id !== null
            && runtimeContext.prov_obj !== null
            && (job_order instanceof Array || job_order instanceof Object)
        ) {
            runtimeContext.prov_obj.used_artefacts(
                job_order, runtimeContext.process_run_id, this.name.toString()
            );
        } else {
            _logger.warning(
                "research_obj set but one of process_run_id "
                + "or prov_obj is missing from runtimeContext: "
                + runtimeContext.toString()
            );
        }
    }
    let outputs: any = {};
    let processStatus = ""
    try {
        let stdin_path: string | undefined;
        if (this.stdin !== undefined) {
            let rmap = this.pathmapper.reversemap(this.stdin);
            if (rmap === undefined) {
                throw new WorkflowException(`${this.stdin} missing from pathmapper`);
            } else {
                stdin_path = rmap[1];
            }
        }

        let stderr_stdout_log_path = (base_path_logs: string, stderr_or_stdout: string | undefined) : string | undefined => {
            if (stderr_or_stdout !== undefined) {
                let abserr = path.join(base_path_logs, stderr_or_stdout);
                let dnerr = path.dirname(abserr);
                if (dnerr && !fs.existsSync(dnerr)) {
                    fs.mkdirSync(dnerr, { recursive: true });
                }
                return abserr;
            }
            return undefined;
        };

        let stderr_path = stderr_stdout_log_path(this.base_path_logs, this.stderr);
        let stdout_path = stderr_stdout_log_path(this.base_path_logs, this.stdout);
        let commands = runtime.concat(this.command_line).map(x => x.toString());
        if (runtimeContext.secret_store !== null) {
            commands = <string[]> runtimeContext.secret_store.retrieve(<any>commands);
            env = <{ [id: string] : string }> runtimeContext.secret_store.retrieve(<any>env);
        }

        let job_script_contents: string | null = null;
        let builder: any = this.builder ? this.builder : null;
        if (builder !== null) {
            job_script_contents = builder.build_job_script(commands);
        }
        let rcode = await _job_popen(
            commands,
            stdin_path,
            stdout_path,
            stderr_path,
            env,
            this.outdir,
            () => runtimeContext.create_outdir(),
            job_script_contents,
            this.timelimit,
            this.name,
            monitor_function,
            runtimeContext.default_stdout,
            runtimeContext.default_stderr,
        );
        if (this.successCodes.includes(rcode)) {
            processStatus = "success";
        } else if (this.temporaryFailCodes.includes(rcode)) {
            processStatus = "temporaryFail";
        } else if (this.permanentFailCodes.includes(rcode)) {
            processStatus = "permanentFail";
        } else if (rcode === 0) {
            processStatus = "success";
        } else {
            processStatus = "permanentFail";
        }

        if (processStatus !== "success") {
            if (rcode < 0) {
                _logger.warning(
                    `[job ${this.name}] was terminated by signal:`,
                );
            } else {
                _logger.warning(`[job ${this.name}] exited with status: ${rcode}`);
            }
        }

        if (this.generatefiles.listing) {
            if (this.generatemapper) {
                relink_initialworkdir(
                    this.generatemapper,
                    this.outdir,
                    this.builder.outdir,
                     this.inplace_update 
                );
            } else {
                throw new ValueError(
                    `'listing' in self.generatefiles but no generatemapper was setup.`
                );
            }
        }
        runtimeContext.log_dir_handler(
            this.outdir, this.base_path_logs, stdout_path, stderr_path
        );
        let outputs = this.collect_outputs(this.outdir, rcode);
        //outputs = bytes2str_in_dicts(outputs);
    // } catch (e) {
    //     if (e.errno == 2) {
    //         if (runtime) {
    //             _logger.error(`'${runtime[0]}' not found: ${e}`);
    //         } else {
    //             _logger.error(`'${this.command_line[0]}' not found: ${e}`);
    //         }
    //     } else {
    //         new Error("Exception while running job");
            
    //     }
    //     processStatus = "permanentFail";
    } catch (err) {
        _logger.error(`[job ${this.name}] Job error:\n${err}`);
        processStatus = "permanentFail";
    }
    //  catch {
    //     _logger.exception("Exception while running job");
    //     processStatus = "permanentFail";
    // }
    if (
        runtimeContext.research_obj !== null
        && this.prov_obj !== null
        && runtimeContext.process_run_id !== null
    ) {
        // creating entities for the outputs produced by each step (in the provenance document)
        this.prov_obj.record_process_end(
            String(this.name),
            runtimeContext.process_run_id,
            outputs,
            new Date(),
        )
    }
    if (processStatus !== "success") {
        _logger.warning(`[job ${this.name}] completed ${processStatus}`);
    } else {
        _logger.info(`[job ${this.name}] completed ${processStatus}`);
    }

    if (_logger.isDebugEnabled()) {
        _logger.debug(`[job ${this.name}] outputs ${JSON.stringify(outputs, null, 4)}`);
    }

    if (this.generatemapper !== null && runtimeContext.secret_store !== null) {
        // TODO
        // Delete any runtime-generated files containing secrets.
        // for (let _, p of Object.entries(this.generatemapper)) {
        //     if (p.type === "CreateFile") {
        //         if (runtimeContext.secret_store.has_secret(p.resolved)) {
        //             let host_outdir = this.outdir;
        //             let container_outdir = this.builder.outdir;
        //             let host_outdir_tgt = p.target;
        //             if (p.target.startsWith(container_outdir + "/")) {
        //                 host_outdir_tgt = path.join(
        //                     host_outdir, p.target.slice(container_outdir.length + 1)
        //                 );
        //             }
        //             fs.unlinkSync(host_outdir_tgt);
        //         }
        //     }
        // }
    }

    if (runtimeContext.workflow_eval_lock === null) {
        throw new Error("runtimeContext.workflow_eval_lock must not be None");
    }

    if (this.output_callback) {
        this.output_callback(outputs, processStatus);
    }

    if (runtimeContext.rm_tmpdir && this.stagedir !== undefined && fs.existsSync(this.stagedir)) {
        _logger.debug(
            `[job ${this.name}] Removing input staging directory ${this.stagedir}`
        );
        await fsExtra.remove(this.stagedir);
    }

    if (runtimeContext.rm_tmpdir) {
        _logger.debug(`[job ${this.name}] Removing temporary directory ${this.tmpdir}`);
        await fsExtra.remove(this.tmpdir);
    }
}
abstract _required_env(): Record<string, string>;

_preserve_environment_on_containers_warning(varname?: Iterable<string>): void {
    // By default, don't do anything; ContainerCommandLineJob below
    // will issue a warning.
}

prepare_environment(runtimeContext: any, envVarReq: Record<string, string>): void {
    // Start empty
    let env: Record<string, string> = {};

    // Preserve any env vars
    if (runtimeContext.preserve_entire_environment) {
        this._preserve_environment_on_containers_warning();
        Object.assign(env, process.env);
    } else if (runtimeContext.preserve_environment) {
        this._preserve_environment_on_containers_warning(runtimeContext.preserve_environment);
        for (let key of runtimeContext.preserve_environment) {
            if (process.env[key]) {
                env[key] = process.env[key] as string;
            } else {
                console.warn(`Attempting to preserve environment variable ${key} which is not present`);
            }
        }
    }

    // Set required env vars
    Object.assign(env, this._required_env());

    // Apply EnvVarRequirement
    Object.assign(env, envVarReq);

    // Set on ourselves
    this.environment = env;
}
process_monitor(sproc: any): void {
    // TODO
    // let monitor = psutil.Process(sproc.pid);
    // let memory_usage: (number | null)[] = [null];

    // let get_tree_mem_usage = function(memory_usage: (number | null)[]) {
    //     let children = monitor.children();
    //     try {
    //         let rss = monitor.memory_info().rss;
    //         while (children.length) {
    //             rss += children.reduce((sum, process) => sum + process.memory_info().rss, 0);
    //             children = [].concat(...children.map(process => process.children()));
    //         }

    //         if (memory_usage[0] === null || rss > memory_usage[0]) {
    //             memory_usage[0] = rss;
    //         }
    //     } catch (e) {
    //         if (e instanceof psutil.NoSuchProcess) {
    //             mem_tm.cancel();
    //         }
    //     }
    // };

    // let mem_tm = new Timer(1, get_tree_mem_usage, memory_usage);
    // mem_tm.daemon = true;
    // mem_tm.start();
    // sproc.wait();
    // mem_tm.cancel();

    // if (memory_usage[0] !== null) {
    //     _logger.info("[job ${this.name}] Max memory used: ${Math.round(memory_usage[0] / (2**20))}MiB");
    // } else {
    //     _logger.debug('Could not collect memory usage, job ended before monitoring began.');
    // }
}
}
export  class CommandLineJob extends JobBase {
  run(
    runtimeContext: RuntimeContext,
    tmpdir_lock?: any,
  ): void {
    if (tmpdir_lock) {
      // assuming tmpdir_lock has a context equivalent
      tmpdir_lock.run(() => {
        if (!fs.existsSync(this.tmpdir)) {
          fs.mkdirSync(this.tmpdir, { recursive: true });
        }
      });
    } else {
      if (!fs.existsSync(this.tmpdir)) {
        fs.mkdirSync(this.tmpdir, { recursive: true });
      }
    }

    this._setup(runtimeContext);

    stage_files(
      this.pathmapper,
      null,
      true,
      true,
      runtimeContext.secret_store,
    );
    if (this.generatemapper) {
      stage_files(
        this.generatemapper,
        null,
        this.inplace_update,
        true,
        runtimeContext.secret_store,
      );
      relink_initialworkdir(
        this.generatemapper,
        this.outdir,
        this.builder.outdir,
        this.inplace_update,
      );
    }

    const monitor_function = this.process_monitor.bind(this)

    this._execute([], this.environment, runtimeContext, monitor_function)
  }

  _required_env(): {[key: string]: string} {
    let env:{[key: string]: string} = {};
    env["HOME"] = this.outdir;
    env["TMPDIR"] = this.tmpdir;
    env["PATH"] = process.env["PATH"] as string;
    for (let extra of ["SYSTEMROOT", "QEMU_LD_PREFIX"]) {
      if (extra in process.env) {
        env[extra] = process.env[extra] as string;
      }
    }
    return env;
  }
}

const CONTROL_CODE_RE: string = "\\x1b\\[[0-9;]*[a-zA-Z]";
export abstract class ContainerCommandLineJob extends JobBase {
    static readonly CONTAINER_TMPDIR: string = "/tmp";

    abstract get_from_requirements(r: any, pull_image: boolean, force_pull: boolean, tmp_outdir_prefix: string): any;

    abstract create_runtime(env: { [key: string]: string }, runtime_context: any): [Array<string>, any];

    abstract append_volume(runtime: Array<string>, source: string, target: string, writable: boolean): void;

    abstract add_file_or_directory_volume(runtime: Array<string>, volume: any, host_outdir_tgt: any): void;

    abstract add_writable_file_volume(runtime: Array<string>, volume: any, host_outdir_tgt: any, tmpdir_prefix: string): void;

    abstract add_writable_directory_volume(runtime: Array<string>, volume: any, host_outdir_tgt: any, tmpdir_prefix: string): void;

    _preserve_environment_on_containers_warning(varnames: Array<string> = []) {
        let flags: string;
        if (varnames.length === 0) {
            flags = "--preserve-entire-environment";
        } else {
            flags = "--preserve-environment={" + varnames.join(", ") + "}";
        }

        console.warn(
            `You have specified ${flags} while running a container which will override variables set in the container. This may break the container, be non-portable, and/or affect reproducibility.`
        );
    }
    create_file_and_add_volume(
    runtime: Array<string>,
    volume: MapperEnt,
    host_outdir_tgt: string,
    secret_store: any,
    tmpdir_prefix: string,
    ): string {
    let new_file: string = "";
    if (!host_outdir_tgt) {
        host_outdir_tgt = path.join(
        createTmpDir(tmpdir_prefix),
        path.basename(volume.target),
        );
    }
    let writable = volume.type === "CreateWritableFile";
    let contents = volume.resolved;
    if (secret_store) {
        contents = secret_store.retrieve(volume.resolved) as string;
    }
    let dirname = path.dirname(host_outdir_tgt || new_file);
    if (!fs.existsSync(dirname)) {
        fs.mkdirSync(dirname, { recursive: true });
    }
    fs.writeFileSync(host_outdir_tgt || new_file, contents);
    if (!host_outdir_tgt) {
        this.append_volume(runtime, new_file, volume.target, writable);
    }
    if (writable) {
        ensureWritable(host_outdir_tgt || new_file);
    } else {
        ensure_non_writable(host_outdir_tgt || new_file);
    }
    return host_outdir_tgt || new_file;
    }
    add_volumes(
        pathmapper: PathMapper,
        runtime: string[],
        tmpdir_prefix: string,
        secret_store: any, // TODO SecretStore | null = null,
        any_path_okay: boolean = false
    ): void {
        let container_outdir = this.builder.outdir;
        for (let [key, vol] of [...(pathmapper.items()).filter((itm: any) => itm[1].staged)]) {
            let host_outdir_tgt: string | undefined = undefined;
            if (vol.target.startsWith(container_outdir + "/")) {
                host_outdir_tgt = path.join(this.outdir, vol.target.slice(container_outdir.length + 1));
            }
            if (!host_outdir_tgt && !any_path_okay) {
                throw new WorkflowException(
                    `No mandatory DockerRequirement, yet path is outside ` +
                    `the designated output directory, also know as ` +
                    `$(runtime.outdir): ${vol}`
                );
            }
            if (vol.type === "File" || vol.type === "Directory") {
                this.add_file_or_directory_volume(runtime, vol, host_outdir_tgt);
            } else if (vol.type === "WritableFile") {
                this.add_writable_file_volume(runtime, vol, host_outdir_tgt, tmpdir_prefix);
            } else if (vol.type === "WritableDirectory") {
                this.add_writable_directory_volume(runtime, vol, host_outdir_tgt, tmpdir_prefix);
            } else if (["CreateFile", "CreateWritableFile"].includes(vol.type)) {
                let new_path = this.create_file_and_add_volume(
                    runtime, vol, host_outdir_tgt as string, secret_store, tmpdir_prefix
                );
                pathmapper.update(key, new_path, vol.target, vol.type, vol.staged);
            }
        }
    }
    run(runtimeContext: any, tmpdir_lock?: any): void {
        const debug = runtimeContext.debug;
        if (tmpdir_lock) {
            tmpdir_lock(() => {
                if (!fs.existsSync(this.tmpdir)) {
                    fs.mkdirSync(this.tmpdir);
                }
            });
        } else {
            if (!fs.existsSync(this.tmpdir)) {
                fs.mkdirSync(this.tmpdir);
            }
        }

        const [docker_req, docker_is_req] = this.get_requirement("DockerRequirement");
        this.prov_obj = runtimeContext.prov_obj;
        let img_id: any = null;
        const user_space_docker_cmd = runtimeContext.user_space_docker_cmd;
        if (docker_req !== undefined && user_space_docker_cmd) {

            if (docker_req.hasOwnProperty("dockerImageId")) {
                img_id = String(docker_req["dockerImageId"]);
            } 
            else if (docker_req.hasOwnProperty("dockerPull")) {
                img_id = String(docker_req["dockerPull"]);
                const cmd = [user_space_docker_cmd, "pull", img_id];
                _logger.info(String(cmd));
                // TODO
                // try {
                //     process.check_call(cmd, sys.stderr)
                // } catch (exc: any) {
                //     throw new WorkflowException(
                //         `Either Docker container ${img_id} is not available with  user space docker implementation ${user_space_docker_cmd}  or ${user_space_docker_cmd} is missing or broken.`
                //     );
                // }
            } 
            else {
                throw new WorkflowException(
                    "Docker image must be specified as 'dockerImageId' or 'dockerPull' when using user space implementations of Docker"
                );
            }
        } 
        else {
            try {
                if (docker_req !== null && runtimeContext.use_container) {
                    img_id = String(this.get_from_requirements(
                        docker_req,
                        runtimeContext.pull_image,
                        runtimeContext.force_docker_pull,
                        runtimeContext.tmp_outdir_prefix,
                    ));
                }
                if (img_id === null) {
                    if (this.builder.find_default_container) {
                        const default_container = this.builder.find_default_container();
                        if (default_container) {
                            img_id = String(default_container);
                        }
                    }
                }
                if (docker_req !== null && img_id === null && runtimeContext.use_container) {
                    throw new Error("Docker image not available");
                }
                if (
                    this.prov_obj !== null
                    && img_id !== null
                    && runtimeContext.process_run_id !== null
                ) {
                    const container_agent = this.prov_obj.document.agent(
                        uuidv4,
                        {
                            "prov:type": "SoftwareAgent",
                            "cwlprov:image": img_id,
                            "prov:label": `Container execution of image ${img_id}`,
                        },
                    );
                    this.prov_obj.document.wasAssociatedWith(
                        runtimeContext.process_run_id, container_agent
                    );
                }
            } catch (err: any) {
                const container = runtimeContext.singularity ? "Singularity" : "Docker";
                _logger.debug(`${container} error`, err);
                if (docker_is_req) {
                    throw new UnsupportedRequirement(
                        `${container} is required to run this tool: ${String(err)}`
                    );
                } else {
                    throw new WorkflowException(
                        `${container} is not available for this tool, try --no-container to disable ${container}, or install a user space Docker replacement like uDocker with --user-space-docker-cmd.: ${err}`
                    );
                }
            }
        }

        this._setup(runtimeContext);

        const env = {...process.env};
        const [runtime, cidfile] = this.create_runtime(env as {[key:string]:string}, runtimeContext);

        runtime.push(String(img_id));
        let monitor_function: Function | null = null;
        if (cidfile) {
            monitor_function = (process:any) =>
                this.docker_monitor(
                    cidfile,
                    runtimeContext.tmpdir_prefix,
                    !Boolean(runtimeContext.cidfile_dir),
                    runtimeContext.podman ? "podman" : "docker",
                    process
                )
        } else if (runtimeContext.user_space_docker_cmd) {
            monitor_function = this.process_monitor;
        }
        this._execute(runtime, env as {[key:string]:string}, runtimeContext, monitor_function as any);
    }
    docker_monitor(
    cidfile: string,
    tmpdir_prefix: string,
    cleanup_cidfile: boolean,
    docker_exe: string,
    process: any
    ): void {
    let cid: string | null = null;
    while (!cid) {
        // sleep(1);
        if (process.returncode !== null) {
        if (cleanup_cidfile) {
            try {
            fs.unlinkSync(cidfile);
            } catch (exc) {
            _logger.warn(`Ignored error cleaning up ${docker_exe} cidfile: ${exc}`);
            }
            return;
        }
        }
        try {
        cid = fs.readFileSync(cidfile, 'utf8').trim();
        } catch (err) {
        cid = null;
        }
    }
    const max_mem = os.totalmem();
    // const [tmp_dir, tmp_prefix] = path.parse(tmpdir_prefix);
    // const stats_file = tmp.fileSync({ prefix: tmp_prefix, dir: tmp_dir });
    const stats_file_name = "stats_file.name";
    try {
        const stats_file_handle = fs.createWriteStream(stats_file_name, { flags: 'w' });
        let cmds = [docker_exe, "stats"];
        if (!docker_exe.includes("podman")) {
        cmds.push("--no-trunc");
        }
        cmds.push("--format", "{{.MemPerc}}", cid);
        const stats_proc = cp.spawn(cmds[0], cmds.slice(1), {
        stdio: [
            'ignore', // Use parent's stdin for child
            stats_file_handle, // Pipe child's stdout to file
            'ignore', // Pipe child's stderr to null
        ],
        });
        process.wait();
        stats_proc.kill();
    } catch (exc) {
        _logger.warn("Ignored error with %s stats: %s", docker_exe, exc);
        return;
    }
    let max_mem_percent: number = 0;
    let mem_percent: number = 0;
    const stats = fs.readFileSync(stats_file_name).toString().split("\n");
    for (let line of stats) {
        if (!line) {
        break;
        }
        try {
        mem_percent = parseFloat(line.replace(CONTROL_CODE_RE, "").replace("%", ""));
        if (mem_percent > max_mem_percent) {
            max_mem_percent = mem_percent;
        }
        } catch (exc) {
        _logger.debug("%s stats parsing error in line %s: %s", docker_exe, line, exc);
        }
    }
    _logger.info(
        `[job ${this.name}] Max memory used: ${Math.floor((max_mem_percent / 100 * max_mem) / (2**20))}MiB`
    );
    if (cleanup_cidfile) {
        fs.unlinkSync(cidfile);
    }
    }
}