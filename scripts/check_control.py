import sys

def check_control_chars(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    found = False
    for i, char in enumerate(content):
        if ord(char) < 32:
            print(f"Control char {ord(char)} at position {i}")
            # print context
            start = max(0, i - 20)
            end = min(len(content), i + 20)
            print(f"Context: {content[start:end]!r}")
            found = True
            break
            
    if not found:
        print("No control characters found.")

if __name__ == "__main__":
    check_control_chars("extracted.json")
