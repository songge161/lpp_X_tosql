import sys
import json

def analyze_debug(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read().strip()
    
    # Find the start of the JSON data column
    # Pattern: '{"id":
    marker = "'{\"id\":"
    start_idx = content.find(marker)
    if start_idx == -1:
        print(f"Could not find marker {marker}")
        return

    # Skip the opening quote
    s = content[start_idx + 1:]
    
    json_content = ""
    i = 0
    while i < len(s):
        char = s[i]
        if char == "'":
            if i + 1 < len(s) and s[i+1] == "'":
                # SQL escaped quote '' -> '
                json_content += "'"
                i += 2
                continue
            else:
                # End of SQL string
                break
        
        # Capture character as is
        # Note: We do NOT process backslashes here because we assume
        # the file content is already what PG receives (except for SQL quote escaping).
        # In PG standard strings, \ is just \.
        json_content += char
        i += 1
            
    print(f"Extracted JSON length: {len(json_content)}")
    
    # Write extracted JSON to file for inspection
    with open("extracted.json", "w", encoding="utf-8") as f:
        f.write(json_content)
    
    # Try to parse JSON
    try:
        json.loads(json_content)
        print("JSON is valid.")
    except json.JSONDecodeError as e:
        print(f"JSON Error: {e}")
        print(f"Error at position: {e.pos}")
        start = max(0, e.pos - 50)
        end = min(len(json_content), e.pos + 50)
        print(f"Context: ...{json_content[start:end]}...")
        
        # Specific check for "expected \" char"
        # It often means a string was opened but not closed properly, 
        # or closed early due to unescaped quote.

if __name__ == "__main__":
    analyze_debug("debug_line.sql")
