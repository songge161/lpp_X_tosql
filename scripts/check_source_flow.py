
with open('entity_processed_part3.sql', 'r', encoding='utf-8') as f:
    # read first line containing source_flow
    for line in f:
        if 'source_flow' in line:
            idx = line.find('source_flow')
            print(f"Snippet: {line[idx:idx+30]!r}")
            break
