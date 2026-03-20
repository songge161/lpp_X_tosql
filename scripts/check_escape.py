
with open('debug_part3_line.sql', 'r', encoding='utf-8') as f:
    c = f.read()

# Count \" (single backslash + quote)
# Note: In Python string literal, \" is "
# So r'\"' means \ and "
count1 = c.count(r'\"')

# Count \\" (double backslash + quote)
count2 = c.count(r'\\"')

print(f"Count of \\\": {count1}")
print(f"Count of \\\\\": {count2}")

idx = c.find('fjsc_label')
if idx != -1:
    # Look at context around fjsc_label
    print(f"Context: {c[idx-20:idx+50]!r}")
