const CWL_IANA = "https://www.iana.org/assignments/media-types/application/cwl";

function scandeps_file_dir(
  base: string,
  doc: CWLObjectType,
  reffields: Set<string>,
  urlfields: Set<string>,
  loadref: (arg1: string, arg2: string) => CommentedMap | CommentedSeq | string | null,
  urljoin: (arg1: string, arg2: string) => string,
  nestdirs: boolean
): Array<CWLObjectType> {
  let r: Array<CWLObjectType> = [];
  let u = doc.get("location") || doc.get("path");
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
                       loadref: (param1: string, param2: string) => (CommentedMap | CommentedSeq | string | null),
                       urljoin: (param1: string, param2: string) => string,
                       nestdirs: boolean,
                       key: string,
                       v: any): Array<CWLObjectType> {
    let r: Array<CWLObjectType> = [];
    if (reffields.has(key)) {
        for (let u2 of aslist(v)) {
            if (u2 instanceof Map) {
                r.push(
                    ...scandeps(
                        base,
                        u2,
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
                    sub,
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
            let deps = { "class": "File", "location": urljoin(base, u3) };
            if (nestdirs) {
                deps = nestdir(base, deps)
            }
            r.push(deps);
        }
    } else if ((doc.get("class") == "File" || doc.get("class") == "Directory") && (key == "listing" || key == "secondaryFiles")) {
        // should be handled earlier.
    } else {
        r.push(
            ...scandeps(
                base,
                v,
                reffields,
                urlfields,
                loadref,
                urljoin,
                nestdirs,
            )
        );
    }
    return r;
}
function scandeps(
    base: string,
    doc: CWLObjectType | CWLObjectType[],
    reffields: Set<string>,
    urlfields: Set<string>,
    loadref: (a: string, b: string) => CommentedMap | CommentedSeq | string | null,
    urljoin: (a: string, b: string) => string = urllib.parse.urljoin,
    nestdirs: boolean = true,
): CWLObjectType[] {
    let r: CWLObjectType[] = [];
    if (typeof doc === "object" && doc !== null && !Array.isArray(doc)) {
        if ("id" in doc) {
            if (typeof doc["id"] === 'string' && doc["id"].startsWith("file://")) {
                let df, _ = urllib.parse.urldefrag(doc["id"]);
                if (base !== df) {
                    r.push({ "class": "File", "location": df, "format": CWL_IANA });
                    base = df;
                }
            }
        }

        if (doc["class"] in ["File", "Directory"] && "location" in urlfields) {
            r = r.concat(scandeps_file_dir(base, doc, reffields, urlfields, loadref, urljoin, nestdirs));
        }

        for (let k in doc) {
            if (doc.hasOwnProperty(k)) {
                let v = doc[k];
                r = r.concat(scandeps_item(base, doc, reffields, urlfields, loadref, urljoin, nestdirs, k, v));
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
                    urljoin,
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
