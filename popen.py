def _job_popen(
    commands: List[str],
    stdin_path: Optional[str],
    stdout_path: Optional[str],
    stderr_path: Optional[str],
    env: Mapping[str, str],
    cwd: str,
    make_job_dir: Callable[[], str],
    job_script_contents: Optional[str] = None,
    timelimit: Optional[int] = None,
    name: Optional[str] = None,
    monitor_function: Optional[Callable[["subprocess.Popen[str]"], None]] = None,
    default_stdout: Optional[Union[IO[bytes], TextIO]] = None,
    default_stderr: Optional[Union[IO[bytes], TextIO]] = None,
) -> int:
    if job_script_contents is None and not FORCE_SHELLED_POPEN:
        stdin: Union[IO[bytes], int] = subprocess.PIPE
        if stdin_path is not None:
            stdin = open(stdin_path, "rb")

        stdout = (
            default_stdout if default_stdout is not None else sys.stderr
        )  # type: Union[IO[bytes], TextIO]
        if stdout_path is not None:
            stdout = open(stdout_path, "wb")

        stderr = (
            default_stderr if default_stderr is not None else sys.stderr
        )  # type: Union[IO[bytes], TextIO]
        if stderr_path is not None:
            stderr = open(stderr_path, "wb")

        sproc = subprocess.Popen(
            commands,
            shell=False,  # nosec
            close_fds=True,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            env=env,
            cwd=cwd,
            universal_newlines=True,
        )
        processes_to_kill.append(sproc)

        if sproc.stdin is not None:
            sproc.stdin.close()

        tm = None
        if timelimit is not None and timelimit > 0:

            def terminate():  # type: () -> None
                try:
                    _logger.warning(
                        "[job %s] exceeded time limit of %d seconds and will be terminated",
                        name,
                        timelimit,
                    )
                    sproc.terminate()
                except OSError:
                    pass

            tm = Timer(timelimit, terminate)
            tm.daemon = True
            tm.start()
        if monitor_function:
            monitor_function(sproc)
        rcode = sproc.wait()

        if tm is not None:
            tm.cancel()

        if isinstance(stdin, IO) and hasattr(stdin, "close"):
            stdin.close()

        if stdout is not sys.stderr and hasattr(stdout, "close"):
            stdout.close()

        if stderr is not sys.stderr and hasattr(stderr, "close"):
            stderr.close()

        return rcode
    else:
        if job_script_contents is None:
            job_script_contents = SHELL_COMMAND_TEMPLATE

        job_description = {
            "commands": commands,
            "cwd": cwd,
            "env": env,
            "stdout_path": stdout_path,
            "stderr_path": stderr_path,
            "stdin_path": stdin_path,
        }

        job_dir = make_job_dir()
        try:
            with open(os.path.join(job_dir, "job.json"), mode="w", encoding="utf-8") as job_file:
                json_dump(job_description, job_file, ensure_ascii=False)
            job_script = os.path.join(job_dir, "run_job.bash")
            with open(job_script, "w") as _:
                _.write(job_script_contents)

            job_run = os.path.join(job_dir, "run_job.py")
            shutil.copyfile(run_job.__file__, job_run)

            env_getter = os.path.join(job_dir, "env_to_stdout.py")
            shutil.copyfile(env_to_stdout.__file__, env_getter)

            sproc = subprocess.Popen(  # nosec
                ["bash", job_script],
                shell=False,  # nosec
                cwd=job_dir,
                # The nested script will output the paths to the correct files if they need
                # to be captured. Else just write everything to stderr (same as above).
                stdout=sys.stderr,
                stderr=sys.stderr,
                stdin=subprocess.PIPE,
                universal_newlines=True,
            )
            processes_to_kill.append(sproc)
            if sproc.stdin is not None:
                sproc.stdin.close()

            rcode = sproc.wait()

            return rcode
        finally:
            shutil.rmtree(job_dir)
