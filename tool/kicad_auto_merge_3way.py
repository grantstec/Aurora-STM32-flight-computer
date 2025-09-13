#!/usr/bin/env python3
"""
3-way KiCad-aware auto-merge for PRs.

- Uses BASE (current main), HEAD (PR branch tip), and MERGE-BASE (common ancestor).
- For *.kicad_sch:
    Key objects by (head_token, uuid). UUID wins; fallback to sha1(block).
    Decision per object key K in union(M, B, H):
      * NotIn(M) & In(B) & NotIn(H)  -> Added on BASE since MB  : KEEP (prevents accidental deletion)
      * NotIn(M) & NotIn(B) & In(H)  -> Added on HEAD since MB  : KEEP
      * In(M) & In(B) & NotIn(H)     -> Deleted by HEAD         : DELETE only if --allow-deletions true; else KEEP base
      * In(M) & NotIn(B) & In(H)     -> Deleted by BASE, kept by HEAD: KEEP HEAD
      * In(M) & In(B) & In(H)        -> Both have; choose --mode {prefer-head, prefer-base}
      * NotIn(M) & In(B) & In(H)     -> Both added independently: choose --mode
- For fp-info-cache / fp-info-cache.txt:
    Union by (Category, FootprintName) blocks. HEAD wins on duplicates.

Writes merged result into the working tree (PR branch) and stages files.
"""

import argparse, fnmatch, os, re, subprocess, sys, hashlib, pathlib

HEAD_TOKENS = {
    "symbol","wire","junction","no_connect","label","global_label","hierarchical_label",
    "sheet","bus","bus_entry","polyline","text","image","group","note","dimension"
}

def sh(*cmd, text=True):
    r = subprocess.run(cmd, capture_output=True, text=text)
    return r.returncode, r.stdout, r.stderr

def git_show(path, ref):
    rc, out, err = sh("git","show",f"{ref}:{path}")
    if rc != 0:
        return None
    return out

def changed_files(base_ref):
    rc, out, err = sh("git","diff","--name-only",f"origin/{base_ref}..HEAD")
    if rc != 0:
        print(err, file=sys.stderr)
        sys.exit(2)
    return [l.strip() for l in out.splitlines() if l.strip()]

def match_any(path, globs):
    return any(fnmatch.fnmatch(path, g) for g in globs)

def parse_top_blocks(text):
    if text is None:
        return []
    s = text
    n = len(s)
    out = []

    def next_paren(start):
        return s.find('(', start)

    def read_head(pos):
        j = pos + 1
        while j < n and s[j].isspace(): j += 1
        k = j
        while k < n and (s[k].isalnum() or s[k] in "_-"): k += 1
        return s[j:k], k

    def match_paren(start):
        depth = 0; j = start
        in_str = False; esc = False
        while j < n:
            c = s[j]
            if in_str:
                if esc: esc = False
                elif c == '\\': esc = True
                elif c == '"': in_str = False
            else:
                if c == '"': in_str = True
                elif c == '(':
                    depth += 1
                elif c == ')':
                    depth -= 1
                    if depth == 0:
                        return j + 1
            j += 1
        return None

    pos = 0
    while True:
        pos = next_paren(pos)
        if pos == -1: break
        head, _ = read_head(pos)
        end = match_paren(pos)
        if end is None: break
        block = s[pos:end]
        if head in HEAD_TOKENS:
            m = re.search(r"\(uuid\s+([0-9a-fA-F-]{8,})\)", block)
            if m:
                uid = m.group(1).lower()
            else:
                uid = hashlib.sha1(block.encode("utf-8")).hexdigest()
            out.append((head, uid, block))
        pos = end
    return out

def rebuild_from(base_text, keep_map, replace_map):
    if base_text is None:
        base_text = "(kicad_sch (version 20211014) (generator merged)\n)"

    s = base_text
    n = len(s)

    def find_blocks_with_pos(text):
        res = []
        s = text; n = len(s)
        def next_paren(start): return s.find('(', start)
        def read_head(pos):
            j = pos + 1
            while j < n and s[j].isspace(): j += 1
            k = j
            while k < n and (s[k].isalnum() or s[k] in "_-"): k += 1
            return s[j:k], k
        def match_paren(start):
            depth = 0; j = start
            in_str = False; esc = False
            while j < n:
                c = s[j]
                if in_str:
                    if esc: esc = False
                    elif c == '\\': esc = True
                    elif c == '"': in_str = False
                else:
                    if c == '"': in_str = True
                    elif c == '(':
                        depth += 1
                    elif c == ')':
                        depth -= 1
                        if depth == 0:
                            return j + 1
                j += 1
            return None
        pos = 0
        while True:
            pos = next_paren(pos)
            if pos == -1: break
            head, _ = read_head(pos)
            end = match_paren(pos)
            if end is None: break
            block = s[pos:end]
            if head in HEAD_TOKENS:
                m = re.search(r"\(uuid\s+([0-9a-fA-F-]{8,})\)", block)
                if m:
                    uid = m.group(1).lower()
                else:
                    uid = hashlib.sha1(block.encode("utf-8")).hexdigest()
                res.append((head, uid, pos, end, block))
            pos = end
        return res

    blocks_pos = find_blocks_with_pos(s)

    out = []
    last = 0
    present_keys = set()

    for head, uid, start, end, block in blocks_pos:
        key = (head, uid)
        if key in keep_map:
            out.append(s[last:start])
            out.append(replace_map.get(key, block))
            last = end
            present_keys.add(key)
        else:
            out.append(s[last:start])
            last = end

    out.append(s[last:])
    merged = "".join(out).rstrip()

    missing = [replace_map[k] for k in keep_map if k not in present_keys]
    if missing:
        if merged.endswith(")"):
            merged = merged[:-1]
        merged += "\n" + "\n\n".join(missing) + "\n)"

    return merged

def merge_kicad_3way(path, base_ref, mb_ref, mode, allow_deletions):
    mb_txt   = git_show(path, mb_ref)
    base_txt = git_show(path, f"origin/{base_ref}")
    head_txt = git_show(path, "HEAD")

    M = {(h,u):blk for (h,u,blk) in parse_top_blocks(mb_txt)}
    B = {(h,u):blk for (h,u,blk) in parse_top_blocks(base_txt)}
    H = {(h,u):blk for (h,u,blk) in parse_top_blocks(head_txt)}

    keys = set(M.keys()) | set(B.keys()) | set(H.keys())

    keep_keys = set()
    replace_map = {}

    def choose_overlap(h,u):
        if mode == "prefer-head":
            return H[(h,u)]
        else:
            return B[(h,u)]

    for (h,u) in keys:
        inM = (h,u) in M
        inB = (h,u) in B
        inH = (h,u) in H

        if (not inM) and inB and (not inH):
            # Added on BASE only -> keep base
            keep_keys.add((h,u))
            replace_map[(h,u)] = B[(h,u)]
        elif (not inM) and (not inB) and inH:
            # Added on HEAD only -> keep head
            keep_keys.add((h,u))
            replace_map[(h,u)] = H[(h,u)]
        elif inM and inB and (not inH):
            # Deleted by HEAD
            if allow_deletions:
                # drop
                pass
            else:
                # keep base
                keep_keys.add((h,u))
                replace_map[(h,u)] = B[(h,u)]
        elif inM and (not inB) and inH:
            # Deleted by BASE, kept by HEAD -> keep head
            keep_keys.add((h,u))
            replace_map[(h,u)] = H[(h,u)]
        elif inM and inB and inH:
            # both have -> choose policy
            keep_keys.add((h,u))
            replace_map[(h,u)] = choose_overlap(h,u)
        elif (not inM) and inB and inH:
            # both added independently since MB -> choose policy
            keep_keys.add((h,u))
            replace_map[(h,u)] = choose_overlap(h,u)
        # else: only in MB (removed by both) -> drop

    merged = rebuild_from(base_txt, keep_keys, replace_map)
    return merged

# ---- fp-info-cache union ----
def is_category_line(line: str) -> bool:
    s = line.strip()
    if not s: return False
    if " " in s: return False
    if s.lower().startswith("http"): return False
    if s.isdigit(): return False
    return re.fullmatch(r"[A-Za-z0-9_:\-\.]+", s) is not None

def parse_info_blocks(text: str):
    if text is None:
        return []
    lines = text.splitlines()
    i = 0; blocks = []
    while i < len(lines) - 1:
        if is_category_line(lines[i]) and is_category_line(lines[i+1]):
            cat = lines[i].strip()
            fpn = lines[i+1].strip()
            j = i + 2
            while j < len(lines) - 1:
                if is_category_line(lines[j]) and is_category_line(lines[j+1]):
                    break
                j += 1
            blocks.append(((cat, fpn), lines[i:j]))
            i = j
        else:
            i += 1
    return blocks

def merge_fp_info_3way(path, base_ref, mb_ref):
    mb_txt   = git_show(path, mb_ref)               or ""
    base_txt = git_show(path, f"origin/{base_ref}") or ""
    head_txt = git_show(path, "HEAD")               or ""

    out = {}
    order = []

    def add_blocks(txt):
        for key, blines in parse_info_blocks(txt):
            if key not in out:
                order.append(key)
            out[key] = blines

    add_blocks(mb_txt)
    add_blocks(base_txt)
    add_blocks(head_txt)

    merged_lines = []
    for key in order:
        merged_lines.extend(out[key])
    return "\n".join(merged_lines) + "\n"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True)        # base branch name (e.g., main)
    ap.add_argument("--head", required=True)        # head ref (unused; we use HEAD)
    ap.add_argument("--mergebase", required=True)   # merge-base SHA
    ap.add_argument("--mode", choices=["add-only","prefer-head","prefer-base"], default="prefer-head")
    ap.add_argument("--allow-deletions", choices=["true","false"], default="false")
    ap.add_argument("globs", nargs="+")
    args = ap.parse_args()

    base_ref = args.base
    mb_ref   = args.mergebase
    allow_del = args.allow_deletions.lower() == "true"

    rc, _, err = sh("git","fetch","origin",base_ref)
    if rc != 0:
        print(err, file=sys.stderr)
        sys.exit(2)

    changed = changed_files(base_ref)
    targets = [p for p in changed if match_any(p, args.globs)]
    if not targets:
        print("No matching changed files.")
        return

    any_staged = False
    for path in targets:
        if path.endswith(".kicad_sch"):
            merged = merge_kicad_3way(path, base_ref, mb_ref, args.mode, allow_del)
        elif os.path.basename(path) in ("fp-info-cache","fp-info-cache.txt"):
            merged = merge_fp_info_3way(path, base_ref, mb_ref)
        else:
            # default: keep HEAD version (we're resolving conflicts for PR content)
            txt = git_show(path, "HEAD")
            if txt is None:
                continue
            merged = txt

        pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(merged)

        rc, out, err = sh("git","add",path)
        if rc != 0:
            print(err, file=sys.stderr)
            sys.exit(3)
        any_staged = True
        print(f"Merged & staged: {path}")

    if not any_staged:
        print("Nothing to stage.")

if __name__ == "__main__":
    main()
