import os
import sys
import shutil
import json
import pprint
import traceback
import pdb
from pathlib import Path

# Try importing msgpack for inline data
try:
    import msgpack
    HAS_MSGPACK = True
except ImportError:
    HAS_MSGPACK = False

def strip_minio_header(data: bytes) -> bytes:
    """
    Detects and strips the 32-byte MinIO/EC header if present.
    It checks for common file signatures at offset 32.
    """
    if len(data) <= 32:
        return data
    
    # Common signatures
    # PDF: %PDF
    # PNG: \x89PNG
    # ZIP/DOCX/XLSX: PK\x03\x04
    # JPEG: \xff\xd8\xff
    # GIF: GIF8
    sigs = [
        b'%PDF',
        b'\x89PNG',
        b'\x50\x4b\x03\x04', 
        b'\xff\xd8\xff',
        b'GIF8',
    ]
    
    # Check at offset 32 (MinIO header masking)
    for sig in sigs:
        if data[32:].startswith(sig):
            return data[32:]
            
    # Check at offset 0 (No masking)
    for sig in sigs:
        if data.startswith(sig):
            return data
            
    # If no signature matches, but we are in a context where we suspect a header...
    # For now, we only strip if we are confident (magic number match).
    # Otherwise we return as is.
    return data

def inspect_xl_meta(path):
    """
    Inspect the content of an xl.meta file.
    """
    if not HAS_MSGPACK:
        print("Error: 'msgpack' is not installed. Please install it with 'pip install msgpack'.")
        return

    print(f"Inspecting: {path}")
    try:
        with open(path, "rb") as f:
            meta_bytes = f.read()
    except Exception as e:
        print(f"Error reading file: {e}")
        traceback.print_exc()
        pdb.set_trace()
        return
    
    print(f"Total bytes: {len(meta_bytes)}")
    print(f"Header: {meta_bytes[:8]}")
    
    if not meta_bytes.startswith(b"XL2 "):
        print("Not an XL2 file")
        return

    try:
        unpacker = msgpack.Unpacker(None, max_buffer_size=10*1024*1024)
        unpacker.feed(meta_bytes[8:])
        
        for i, obj in enumerate(unpacker):
            print(f"--- Object {i} ---")
            if isinstance(obj, dict):
                # Print keys to avoid dumping huge data
                print("Keys:", list(obj.keys()))
                for k in obj.keys():
                    print(f"  Key type: {type(k)} Value type: {type(obj[k])}")
                    if isinstance(obj[k], bytes) and len(obj[k]) > 100:
                         print(f"  Value (bytes) length: {len(obj[k])}")
                         print(f"  Value head: {obj[k][:50]}")
                         print(f"  Value tail: {obj[k][-50:]}")

                # FORCE PRINT OBJECT if keys look weird
                if 'Versions' not in obj and 'MetaSys' not in obj:
                     print("Dumping dict content:")
                     pprint.pprint(obj)
                
                if 'Versions' in obj:
                    versions = obj['Versions']
                    print(f"Versions count: {len(versions)}")
                    for j, v in enumerate(versions):
                        print(f"  Version {j}:")
                        print(f"    Type: {v.get('Type')}")
                        meta_sys = v.get('MetaSys', {})
                        print(f"    MetaSys: {meta_sys}")
                        data = v.get('Data')
                        if data:
                            print(f"    Data length: {len(data)}")
                        else:
                            print(f"    Data: None")
                        
                        meta_user = v.get('MetaUsr', {})
                        print(f"    MetaUsr: {meta_user}")
            elif isinstance(obj, bytes):
                print(f"Type: bytes, Length: {len(obj)}")
                if len(obj) > 0:
                     print(f"Head: {obj[:20]}")
            else:
                print(f"Type: {type(obj)}")
                print(obj)
                
    except Exception as e:
        print(f"Error unpacking: {e}")
        traceback.print_exc()
        print("Breakpoint triggered. Type 'c' to continue, or inspect variables.")
        pdb.set_trace()

def restore_minio_files(source_dir: str, dest_dir: str):
    """
    Traverse source_dir, identify MinIO object directories, and restore files to dest_dir.
    Preserves directory structure relative to source_dir.
    """
    source_path = Path(source_dir).resolve()
    dest_path = Path(dest_dir).resolve()
    dest_path.mkdir(parents=True, exist_ok=True)
    
    print(f"Scanning {source_path}...")
    print(f"Restoring to {dest_path}...")
    
    count_restored = 0
    count_skipped = 0
    
    for root, dirs, files in os.walk(source_path):
        if "xl.meta" in files:
            # This 'root' is likely an Object Directory (e.g. .../file.pdf)
            obj_dir = Path(root)
            
            # Calculate relative path from source_dir to this object directory
            # e.g. source_dir/A/B/file.pdf -> rel_path = A/B/file.pdf
            try:
                rel_path = obj_dir.relative_to(source_path)
            except ValueError:
                continue
            
            # Destination file path
            out_file = dest_path / rel_path
            
            # 增量判断：如果目标文件已存在，则直接跳过
            if out_file.exists():
                print(f"[Skip] {rel_path} (Already exists)")
                count_skipped += 1
                continue

            # Ensure parent directory exists
            out_file.parent.mkdir(parents=True, exist_ok=True)
            
            # 1. Check for part.1 in subdirectories (Standard large file)
            parts = list(obj_dir.rglob("part.*"))
            if parts:
                # Sort parts just in case (though typically just part.1)
                parts.sort(key=lambda p: p.name)
                
                print(f"[Restore] {rel_path} (from {len(parts)} parts)")
                try:
                    with open(out_file, "wb") as f_out:
                        for i, p in enumerate(parts):
                            with open(p, "rb") as f_in:
                                if i == 0:
                                    # Check for 32-byte header in the first chunk
                                    # Read a bit more than 32 bytes to check signatures
                                    chunk_size = 1024
                                    first_chunk = f_in.read(chunk_size)
                                    
                                    stripped_chunk = strip_minio_header(first_chunk)
                                    f_out.write(stripped_chunk)
                                    
                                    # Copy the rest of the file
                                    shutil.copyfileobj(f_in, f_out)
                                else:
                                    shutil.copyfileobj(f_in, f_out)
                    count_restored += 1
                except Exception as e:
                    print(f"  [Error] Failed to join parts for {rel_path}: {e}")
                    traceback.print_exc()
                    pdb.set_trace()
            
            else:
                # 2. Inline data
                if HAS_MSGPACK:
                    try:
                        with open(obj_dir / "xl.meta", "rb") as f:
                            meta_bytes = f.read()
                            if meta_bytes.startswith(b"XL2 "):
                                try:
                                    unpacker = msgpack.Unpacker(None, max_buffer_size=10*1024*1024)
                                    unpacker.feed(meta_bytes[8:]) 
                                    xl_meta = None
                                    fallback_data = None
                                    
                                    # Iterate through all objects in the msgpack stream
                                    for i, obj in enumerate(unpacker):
                                        if isinstance(obj, dict):
                                            if 'Versions' in obj:
                                                xl_meta = obj
                                                break
                                            elif 'MetaSys' in obj:
                                                # Some versions might have a different structure
                                                xl_meta = obj # Potential candidate
                                            else:
                                                # Check for fallback data in values (e.g. {None: bytes})
                                                for k, v in obj.items():
                                                    if isinstance(v, bytes) and len(v) > 100:
                                                        # Heuristic: file content is usually large
                                                        if fallback_data is None or len(v) > len(fallback_data):
                                                            fallback_data = v
                                        elif isinstance(obj, bytes) and len(obj) > 100:
                                            # Also check top-level bytes objects
                                            if fallback_data is None or len(obj) > len(fallback_data):
                                                fallback_data = obj
                                    
                                    if xl_meta:
                                        versions = xl_meta.get('Versions', [])
                                        for v in versions:
                                            if v.get('Type') == 1: # Object
                                                meta_sys = v.get('MetaSys', {})
                                                is_inline = False
                                                for k, val in meta_sys.items():
                                                    k_str = k.decode('utf-8') if isinstance(k, bytes) else k
                                                    if k_str == 'x-minio-internal-inline-data':
                                                        if val == True or val == b'true':
                                                            is_inline = True
                                                
                                                if is_inline:
                                                    data = v.get('Data')
                                                    if data:
                                                        print(f"[Restore] {rel_path} (Inline)")
                                                        data = strip_minio_header(data)
                                                        with open(out_file, "wb") as f_out:
                                                            f_out.write(data)
                                                        count_restored += 1
                                                    else:
                                                        print(f"  [Warn] {rel_path} marked inline but no Data found.")
                                    elif fallback_data:
                                        print(f"[Restore] {rel_path} (Fallback extraction)")
                                        fallback_data = strip_minio_header(fallback_data)
                                        with open(out_file, "wb") as f_out:
                                            f_out.write(fallback_data)
                                        count_restored += 1
                                    else:
                                        print(f"  [Warn] {rel_path} no Versions or valid inline data found.")
                                    
                                except Exception as e:
                                    print(f"  [Error] Failed to unpack msgpack for {rel_path}: {e}")
                                    traceback.print_exc()
                                    pdb.set_trace()
                            else:
                                print(f"  [Skip] {rel_path}/xl.meta does not start with XL2")

                    except Exception as e:
                        print(f"  [Error] Processing {rel_path}: {e}")
                        traceback.print_exc()
                        pdb.set_trace()
                else:
                    print(f"  [Skip] {rel_path} (Inline data suspected, but msgpack not installed)")
                    count_skipped += 1
    
    print("-" * 40)
    print(f"Restored: {count_restored}")
    if count_skipped > 0:
        print(f"Skipped: {count_skipped} (Already exists or missing 'msgpack')")
    print(f"Output Directory: {dest_path}")

if __name__ == "__main__":
    if len(sys.argv) >= 2:
        first_arg = sys.argv[1]
        
        # Inspection mode
        if (os.path.isfile(first_arg) and first_arg.endswith("xl.meta")) or \
           (first_arg == "inspect" and len(sys.argv) >= 3):
            
            target_file = sys.argv[2] if first_arg == "inspect" else first_arg
            inspect_xl_meta(target_file)
            sys.exit(0)

    # Restore mode
    SRC = "source/file/caitou"
    DST = "source/files/caitou"
    
    if len(sys.argv) >= 2:
        SRC = sys.argv[1]
    if len(sys.argv) >= 3:
        DST = sys.argv[2]
        
    restore_minio_files(SRC, DST)
