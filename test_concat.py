
import json
import re

def __date_ts__(v):
    return v

safe_globals = {
    "__builtins__": {
        "str": str, "int": int, "float": float, "len": len, "round": round,
        "dict": dict, "list": list, "__date_ts__": __date_ts__,
    },
    "re": re,
    "json": json,
    "__date_ts__": __date_ts__,
}

# Simulate record
data = {
    "i6ueah9q6z_a": "信息技术",
    "i6ueah9q6z_b": "通信",
    "i6ueah9q6z_c": "",  # Empty
    "i6ueah9q6z_d": "卫星服务"
}

class SafeRecord:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
    # Allow attribute access
    def __getattr__(self, name):
        return self.__dict__.get(name, "")

rec_obj = SafeRecord(**data)

# Readable Expression (Skip Empty)
expr_readable = "'>'.join([str(x) for x in [record.i6ueah9q6z_a, record.i6ueah9q6z_b, record.i6ueah9q6z_c, record.i6ueah9q6z_d] if x])"

# Readable Expression (Keep Empty)
expr_keep_readable = "'>'.join([str(x or '') for x in [record.i6ueah9q6z_a, record.i6ueah9q6z_b, record.i6ueah9q6z_c, record.i6ueah9q6z_d]])"

print("--- Testing Readable Concatenation ---")
try:
    val = eval(expr_readable, safe_globals, {"record": rec_obj, **data})
    print(f"Skip Empty: {val}")
except Exception as e:
    print(f"Error Skip: {e}")

try:
    val_keep = eval(expr_keep_readable, safe_globals, {"record": rec_obj, **data})
    print(f"Keep Empty: {val_keep}")
except Exception as e:
    print(f"Error Keep: {e}")
