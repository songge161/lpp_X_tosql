import argparse, os, re, shutil
from pathlib import Path

IMG_EXTS = {'.png','.jpg','.jpeg','.gif','.svg','.webp'}

def is_remote(url: str) -> bool:
    s = url.strip()
    return s.startswith('http://') or s.startswith('https://') or s.startswith('//') or s.startswith('data:')

def find_urls(text: str):
    urls = []
    for m in re.finditer(r'!\[[^\]]*\]\(([^)]+)\)', text):
        content = m.group(1).strip()
        c = content
        if ' ' in c:
            idx = c.find(' ')
            tail = c[idx+1:].lstrip()
            if tail.startswith('"') or tail.startswith("'"):
                urls.append(c[:idx])
            else:
                urls.append(c)
        else:
            urls.append(c)
    for m in re.finditer(r'<img[^>]+src=["\']([^"\']+)["\']', text, flags=re.I):
        urls.append(m.group(1).strip())
    dedup = []
    seen = set()
    for u in urls:
        if u not in seen:
            dedup.append(u)
            seen.add(u)
    return dedup

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--root', default='.')
    p.add_argument('--static-dir', default='static')
    p.add_argument('--static-subdir', default='assets')
    p.add_argument('--base-url', default='/static')
    p.add_argument('--dry-run', action='store_true')
    args = p.parse_args()

    root = Path(args.root).resolve()
    static_root = root / args.static_dir / args.static_subdir
    os.makedirs(static_root, exist_ok=True)

    md_files = [f for f in root.rglob('*.md')]
    copied = 0
    rewritten = 0
    for md in md_files:
        text = md.read_text(encoding='utf-8', errors='ignore')
        urls = find_urls(text)
        replace_map = {}
        for url in urls:
            if is_remote(url):
                continue
            src_path = (md.parent / url).resolve() if not Path(url).is_absolute() else Path(url)
            if not src_path.exists():
                continue
            if src_path.suffix.lower() not in IMG_EXTS:
                continue
            try:
                rel = str(src_path.relative_to(root))
            except Exception:
                rel = src_path.name
            dest = static_root / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            if not args.dry_run:
                shutil.copy2(src_path, dest)
            new_url = f"{args.base_url}/{args.static_subdir}/{rel.replace(os.sep,'/')}"
            replace_map[url] = new_url
            copied += 1
        if replace_map:
            new_text = text
            for old, new in replace_map.items():
                new_text = new_text.replace(old, new)
            if new_text != text and not args.dry_run:
                md.write_text(new_text, encoding='utf-8')
            if new_text != text:
                rewritten += 1
    print(f"copied={copied} rewritten_md_files={rewritten} static_dir={static_root}")

if __name__ == '__main__':
    main()