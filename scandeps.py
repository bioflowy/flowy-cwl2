CWL_IANA = "https://www.iana.org/assignments/media-types/application/cwl"

def scandeps_file_dir(base:str,
                      doc: CWLObjectType,
                      reffields: Set[str],
                      urlfields: Set[str],
                      loadref: Callable[[str, str], Union[CommentedMap, CommentedSeq, str, None]],
                      urljoin: Callable[[str, str], str],
                      nestdirs: bool) -> MutableSequence[CWLObjectType]:
    r: MutableSequence[CWLObjectType] = []
    u = cast(Optional[str], doc.get("location", doc.get("path")))
    if u and not u.startswith("_:"):
        deps: CWLObjectType = {
            "class": doc["class"],
            "location": urljoin(base, u),
        }
        if "basename" in doc:
            deps["basename"] = doc["basename"]
        if doc["class"] == "Directory" and "listing" in doc:
            deps["listing"] = doc["listing"]
        if doc["class"] == "File" and "secondaryFiles" in doc:
            deps["secondaryFiles"] = cast(
                CWLOutputAtomType,
                scandeps(
                    base,
                    cast(
                        Union[CWLObjectType, MutableSequence[CWLObjectType]],
                        doc["secondaryFiles"],
                    ),
                    reffields,
                    urlfields,
                    loadref,
                    urljoin=urljoin,
                    nestdirs=nestdirs,
                ),
            )
        if nestdirs:
            deps = nestdir(base, deps)
        r.append(deps)
    else:
        if doc["class"] == "Directory" and "listing" in doc:
            r.extend(
                scandeps(
                    base,
                    cast(MutableSequence[CWLObjectType], doc["listing"]),
                    reffields,
                    urlfields,
                    loadref,
                    urljoin=urljoin,
                    nestdirs=nestdirs,
                )
            )
        elif doc["class"] == "File" and "secondaryFiles" in doc:
            r.extend(
                scandeps(
                    base,
                    cast(MutableSequence[CWLObjectType], doc["secondaryFiles"]),
                    reffields,
                    urlfields,
                    loadref,
                    urljoin=urljoin,
                    nestdirs=nestdirs,
                )
            )
    return r
#------
def scandeps_item(base:str,
                      doc: CWLObjectType,
                      reffields: Set[str],
                      urlfields: Set[str],
                      loadref: Callable[[str, str], Union[CommentedMap, CommentedSeq, str, None]],
                      urljoin: Callable[[str, str], str],
                      nestdirs: bool,
                      key:str,
                      v:any) -> MutableSequence[CWLObjectType]:
    r: MutableSequence[CWLObjectType] = []
    if key in reffields:
        for u2 in aslist(v):
            if isinstance(u2, MutableMapping):
                r.extend(
                    scandeps(
                        base,
                        u2,
                        reffields,
                        urlfields,
                        loadref,
                        urljoin=urljoin,
                        nestdirs=nestdirs,
                    )
                )
            else:
                subid = urljoin(base, u2)
                basedf, _ = urllib.parse.urldefrag(base)
                subiddf, _ = urllib.parse.urldefrag(subid)
                if basedf == subiddf:
                    continue
                sub = cast(
                    Union[MutableSequence[CWLObjectType], CWLObjectType],
                    loadref(base, u2),
                )
                deps2: CWLObjectType = {
                    "class": "File",
                    "location": subid,
                    "format": CWL_IANA,
                }
                sf = scandeps(
                    subid,
                    sub,
                    reffields,
                    urlfields,
                    loadref,
                    urljoin=urljoin,
                    nestdirs=nestdirs,
                )
                if sf:
                    deps2["secondaryFiles"] = cast(
                        MutableSequence[CWLOutputAtomType], mergedirs(sf)
                    )
                if nestdirs:
                    deps2 = nestdir(base, deps2)
                r.append(deps2)
    elif key in urlfields and key != "location":
        for u3 in aslist(v):
            deps = {"class": "File", "location": urljoin(base, u3)}
            if nestdirs:
                deps = nestdir(base, deps)
            r.append(deps)
    elif doc.get("class") in ("File", "Directory") and key in (
        "listing",
        "secondaryFiles",
    ):
        # should be handled earlier.
        pass
    else:
        r.extend(
            scandeps(
                base,
                cast(Union[MutableSequence[CWLObjectType], CWLObjectType], v),
                reffields,
                urlfields,
                loadref,
                urljoin=urljoin,
                nestdirs=nestdirs,
            )
        )
    return r
#------
def scandeps(
    base: str,
    doc: Union[CWLObjectType, MutableSequence[CWLObjectType]],
    reffields: Set[str],
    urlfields: Set[str],
    loadref: Callable[[str, str], Union[CommentedMap, CommentedSeq, str, None]],
    urljoin: Callable[[str, str], str] = urllib.parse.urljoin,
    nestdirs: bool = True,
) -> MutableSequence[CWLObjectType]:
    """
    Search for external files references in a CWL document or input object.

    Looks for objects with 'class: File' or 'class: Directory' and
    adds them to the list of dependencies.

    :param base: the base URL for relative references.
    :param doc: a CWL document or input object
    :param urlfields: added as a File dependency
    :param reffields: field name like a workflow step 'run'; will be
      added as a dependency and also loaded (using the 'loadref'
      function) and recursively scanned for dependencies.  Those
      dependencies will be added as secondary files to the primary file.
    :param nestdirs: if true, create intermediate directory objects when
      a file is located in a subdirectory under the starting directory.
      This is so that if the dependencies are materialized, they will
      produce the same relative file system locations.
    :returns: A list of File or Directory dependencies
    """
    r: MutableSequence[CWLObjectType] = []
    if isinstance(doc, MutableMapping):
        if "id" in doc:
            if cast(str, doc["id"]).startswith("file://"):
                df, _ = urllib.parse.urldefrag(cast(str, doc["id"]))
                if base != df:
                    r.append({"class": "File", "location": df, "format": CWL_IANA})
                    base = df

        if doc.get("class") in ("File", "Directory") and "location" in urlfields:
            r.extend(scandeps_file_dir(base,doc,reffields,urlfields,loadref,urljoin,nestdirs))

        for k, v in doc.items():
            r.extend(scandeps_item(base,doc,reffields,urlfields,loadref,urljoin,nestdirs,k,v))
    elif isinstance(doc, MutableSequence):
        for d in doc:
            r.extend(
                scandeps(
                    base,
                    d,
                    reffields,
                    urlfields,
                    loadref,
                    urljoin=urljoin,
                    nestdirs=nestdirs,
                )
            )

    if r:
        normalizeFilesDirs(r)

    return r
