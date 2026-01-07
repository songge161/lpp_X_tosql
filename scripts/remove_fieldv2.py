import os
import re


def strip_ident_quotes(s: str) -> str:
    s = s.strip()
    if s.startswith('"') and s.endswith('"'):
        return s[1:-1]
    if s.startswith('`') and s.endswith('`'):
        return s[1:-1]
    if s.startswith('[') and s.endswith(']'):
        return s[1:-1]
    return s


def normalize_table_name(ident: str) -> str:
    # keep only the last part (after schema), lowercased, strip quotes
    last = ident.strip().split('.')[-1].strip()
    return strip_ident_quotes(last).lower()


def find_stmt_end(src: str, start: int) -> int:
    """Find the semicolon ending the SQL statement, ignoring quotes."""
    in_sq = False
    i = start
    n = len(src)
    while i < n:
        ch = src[i]
        if in_sq:
            if ch == "'":
                # handle escaped '' inside SQL strings
                if i + 1 < n and src[i + 1] == "'":
                    i += 2
                    continue
                in_sq = False
            i += 1
            continue
        else:
            if ch == "'":
                in_sq = True
                i += 1
                continue
            if ch == ';':
                return i + 1
            i += 1
    return -1


def split_list_ignoring_quotes(s: str) -> list:
    """Split a comma-separated list, ignoring commas inside quotes or bracket pairs."""
    out = []
    buf = []
    in_sq = False
    depth_paren = 0
    depth_brace = 0
    depth_bracket = 0
    i = 0
    n = len(s)
    while i < n:
        ch = s[i]
        if in_sq:
            buf.append(ch)
            if ch == "'":
                if i + 1 < n and s[i + 1] == "'":
                    buf.append("'")
                    i += 2
                    continue
                in_sq = False
            i += 1
            continue
        if ch == "'":
            in_sq = True
            buf.append(ch)
            i += 1
            continue
        if ch == '(':
            depth_paren += 1
            buf.append(ch)
            i += 1
            continue
        if ch == ')':
            if depth_paren > 0:
                depth_paren -= 1
            buf.append(ch)
            i += 1
            continue
        if ch == '{':
            depth_brace += 1
            buf.append(ch)
            i += 1
            continue
        if ch == '}':
            if depth_brace > 0:
                depth_brace -= 1
            buf.append(ch)
            i += 1
            continue
        if ch == '[':
            depth_bracket += 1
            buf.append(ch)
            i += 1
            continue
        if ch == ']':
            if depth_bracket > 0:
                depth_bracket -= 1
            buf.append(ch)
            i += 1
            continue
        if ch == ',' and not in_sq and depth_paren == 0 and depth_brace == 0 and depth_bracket == 0:
            out.append(''.join(buf).strip())
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    if buf:
        out.append(''.join(buf).strip())
    return out


def extract_parenthesized_segment(stmt: str, start_pos: int) -> tuple:
    """Given position at an opening '(', return (inner, end_index) for the matching ')' index.
    If no match, return ('', -1). Handles quotes.
    """
    if start_pos < 0 or start_pos >= len(stmt) or stmt[start_pos] != '(':
        return '', -1
    in_sq = False
    depth = 0
    i = start_pos
    n = len(stmt)
    i += 1
    depth = 1
    out = []
    while i < n:
        ch = stmt[i]
        if in_sq:
            out.append(ch)
            if ch == "'":
                if i + 1 < n and stmt[i + 1] == "'":
                    out.append("'")
                    i += 2
                    continue
                in_sq = False
            i += 1
            continue
        if ch == "'":
            in_sq = True
            out.append(ch)
            i += 1
            continue
        if ch == '(':
            depth += 1
            out.append(ch)
            i += 1
            continue
        if ch == ')':
            depth -= 1
            if depth == 0:
                return ''.join(out), i
            out.append(ch)
            i += 1
            continue
        out.append(ch)
        i += 1
    return '', -1


def force_cleanup(stmt: str):
    """Fallback – just rewrite table name to entity and return original segments."""
    try:
        new_head = re.sub(r"(?is)\bINSERT\s+INTO\s+[^\(]+", "INSERT INTO entity ", stmt, count=1)
        return new_head
    except Exception:
        return None


# 可以在此修改 SID 常量
# TARGET_SID moved to main()

def process_insert_statement(stmt: str, target_sid: str, mode: str = 'insert') -> str:
    """Process one INSERT statement for table 'entity':
    - Drop columns: flow_data, gen_id
    - Set sid to target_sid, privilege to NULL
    - Preserve data column exactly (no JSON repair)
    - Unify table name to entity
    - mode='update': If uuid exists, convert to UPDATE statement
    - mode='upsert': If uuid exists, convert to INSERT ... ON CONFLICT(uuid) DO UPDATE ...
    """
    if 'INSERT' not in stmt.upper():
        return stmt

    stmt_head_rewritten = re.sub(r"(?is)\bINSERT\s+INTO\s+[^\(]+", "INSERT INTO entity ", stmt, count=1)

    m = re.search(r"(?is)\bINSERT\s+INTO\s+([^\(]+)\(", stmt)
    if not m:
        return stmt_head_rewritten
    table_ident_raw = m.group(1)
    _ = normalize_table_name(table_ident_raw)

    cols_start = stmt.find('(', m.end(1))
    cols_seg, cols_end = extract_parenthesized_segment(stmt, cols_start)
    if cols_end < 0:
        fc = force_cleanup(stmt)
        return fc if fc else stmt_head_rewritten

    rest = stmt[cols_end + 1:]
    mvals = re.search(r"(?is)\bVALUES\s*\(", rest)
    if not mvals:
        fc = force_cleanup(stmt)
        return fc if fc else stmt_head_rewritten
    vals_start = rest.find('(', mvals.end(0) - 1)
    vals_seg, vals_end_rel = extract_parenthesized_segment(rest, vals_start)
    if vals_end_rel < 0:
        fc = force_cleanup(stmt)
        return fc if fc else stmt_head_rewritten
    stmt_tail = rest[vals_end_rel + 1:]

    raw_cols = [c.strip() for c in cols_seg.split(',')]
    norm_cols = [strip_ident_quotes(c.split('.')[-1].strip()).lower() for c in raw_cols]
    raw_vals = split_list_ignoring_quotes(vals_seg)

    if len(raw_cols) != len(raw_vals):
        cols_mut = list(raw_cols)
        vals_mut = list(raw_vals)
        norm_mut = [strip_ident_quotes(c.split('.')[-1].strip()).lower() for c in cols_mut]

        def remove_col_and_val(col_name: str):
            if col_name in norm_mut:
                idx = norm_mut.index(col_name)
                if idx < len(vals_mut):
                    vals_mut.pop(idx)
                cols_mut.pop(idx)
                norm_mut.pop(idx)

        remove_col_and_val('flow_data')
        remove_col_and_val('gen_id')

        if 'sid' in norm_mut:
            sid_idx = norm_mut.index('sid')
            if sid_idx < len(vals_mut):
                vals_mut[sid_idx] = target_sid
        if 'privilege' in norm_mut:
            priv_idx = norm_mut.index('privilege')
            if priv_idx < len(vals_mut):
                vals_mut[priv_idx] = 'NULL'

        pair_len = min(len(cols_mut), len(vals_mut))
        if pair_len > 0:
            out_cols = ', '.join(cols_mut[:pair_len])
            out_vals = ', '.join(vals_mut[:pair_len])
            rebuilt = f"INSERT INTO entity ({out_cols}) VALUES ({out_vals});"
            return rebuilt

        fc = force_cleanup(stmt)
        return fc if fc else stmt_head_rewritten

    pairs = list(zip(raw_cols, norm_cols, raw_vals))
    new_pairs = []
    for raw_c, n_c, raw_v in pairs:
        if n_c in ('flow_data', 'gen_id'):
            continue
        if n_c == 'sid':
            new_pairs.append((raw_c, n_c, target_sid))
            continue
        if n_c == 'privilege':
            new_pairs.append((raw_c, n_c, 'NULL'))
            continue
        # Preserve data and all other values as-is
        new_pairs.append((raw_c, n_c, raw_v))

    if mode == 'update':
        uuid_pair = next((p for p in new_pairs if p[1] == 'uuid'), None)
        if uuid_pair:
            set_clauses = []
            for col, norm, val in new_pairs:
                if norm == 'uuid':
                    continue
                set_clauses.append(f"{col} = {val}")
            where_clause = f"{uuid_pair[0]} = {uuid_pair[2]}"
            return f"UPDATE entity SET {', '.join(set_clauses)} WHERE {where_clause};"
    
    out_cols = ', '.join([p[0] for p in new_pairs])
    out_vals = ', '.join([p[2] for p in new_pairs])

    new_head = re.sub(r"(?is)\bINSERT\s+INTO\s+[^\(]+", "INSERT INTO entity ", stmt[:cols_start])
    new_stmt = f"{new_head}({out_cols}) VALUES ({out_vals})"

    if mode == 'upsert':
        uuid_pair = next((p for p in new_pairs if p[1] == 'uuid'), None)
        if uuid_pair:
            set_clauses = []
            for col, norm, val in new_pairs:
                if norm == 'uuid':
                    continue
                set_clauses.append(f"{col} = EXCLUDED.{col}")
            upsert_clause = f"ON CONFLICT({uuid_pair[0]}) DO UPDATE SET {', '.join(set_clauses)}"
            return f"{new_stmt} {upsert_clause};"

    return new_stmt + stmt_tail


def process_file(in_path: str, out_path: str, target_sid: str, mode: str = 'insert'):
    with open(in_path, 'r', encoding='utf-8') as f:
        src = f.read()

    out_parts = []
    i = 0
    n = len(src)
    while i < n:
        m = re.search(r"(?is)INSERT\s+INTO\s+", src[i:])
        if not m:
            out_parts.append(src[i:])
            break
        start = i + m.start()
        out_parts.append(src[i:start])
        end = find_stmt_end(src, start)
        if end < 0:
            stmt = src[start:]
            new_stmt = process_insert_statement(stmt, target_sid, mode)
            out_parts.append(new_stmt)
            break
        stmt = src[start:end]
        new_stmt = process_insert_statement(stmt, target_sid, mode)
        out_parts.append(new_stmt)
        i = end

    out = ''.join(out_parts)
    
    out = out.replace('\\\\', '\\')
    # Fix for standard SQL compatibility (PostgreSQL)
    # 1. Replace MySQL-style escaped single quotes (\') with standard SQL escaped single quotes ('')
    # out = out.replace(r"\'", "''")
  


    # 2. Fix for PG in escape mode (standard_conforming_strings=off)
    # MySQL export: \" -> PG parses as " -> JSON error (unexpected end of string or char)
    # We need PG to store \" so JSON sees ".
    # Input to PG: \\" -> PG parses as \" -> JSON parses as "
    # out = out.replace('\\"', '\\\\"')
    
    # Same for other control chars that JSON needs escaped
    # out = out.replace('\\n', '\\\\n')
    # out = out.replace('\\r', '\\\\r')
    # out = out.replace('\\t', '\\\\t')
    # out = out.replace('\\b', '\\\\b')
    # out = out.replace('\\f', '\\\\f')
    
    # Note: We do NOT replace \\\\ because MySQL \\\\ -> PG \\\\ -> PG stores \\ -> JSON sees \ (correct)
    
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(out)


def main():
    base = os.path.dirname(os.path.abspath(__file__))
    in_path = os.path.join(base, 'entity.sql')
    out_path = os.path.join(base, 'entity_processed.sql')
    
    # 在这里配置目标 SID
    # target_sid = "'sahgqakmzd'"
    # target_sid = "'i6qzt3nn20'"
    target_sid = "'g8o51bfn2n'"
    
    # mode: 'insert' | 'update' | 'upsert'
    # 'insert': Standard INSERT
    # 'update': UPDATE entity SET ... WHERE uuid=... (only if uuid exists)
    # 'upsert': INSERT ... ON CONFLICT(uuid) DO UPDATE SET ... (Postgres style)
    mode = 'update'
    
    if not os.path.exists(in_path):
        raise FileNotFoundError(f'Input file not found: {in_path}')
    process_file(in_path, out_path, target_sid, mode)
    print(f'Processed: {in_path} -> {out_path} (sid={target_sid}, mode={mode})')


if __name__ == '__main__':
    main()
