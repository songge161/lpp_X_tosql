
import os
import re

def split_sql_file(in_path: str, base_out_path: str, sql_size: int):
    """
    Reads a SQL file and splits it into multiple files, each containing `sql_size` INSERT statements.
    
    Args:
        in_path: Path to the input SQL file.
        base_out_path: Base path for output files. Suffixes will be added (e.g., _part1.sql, _part2.sql).
        sql_size: Number of INSERT statements per output file.
    """
    if not os.path.exists(in_path):
        print(f"Error: Input file '{in_path}' not found.")
        return

    # Prepare output filename pattern
    # If base_out_path is "data.sql", output will be "data_part1.sql", "data_part2.sql", etc.
    root, ext = os.path.splitext(base_out_path)
    
    current_part = 1
    current_count = 0
    current_lines = []
    
    # Simple regex to identify the start of an INSERT statement
    # Assumes standard SQL formatting where INSERT starts a new logical command
    # NOTE: This regex might need adjustment if INSERT is not at the start of a line or if there are comments
    # But for standard dumps, it usually works. 
    # To be more robust, we can just count lines starting with INSERT.
    insert_pattern = re.compile(r"^\s*INSERT\s+INTO", re.IGNORECASE)

    try:
        with open(in_path, 'r', encoding='utf-8') as f_in:
            for line in f_in:
                # Check if this line starts a new INSERT statement
                if insert_pattern.match(line):
                    # If we reached the limit, write the current batch to a file
                    if current_count >= sql_size:
                        out_file = f"{root}_part{current_part}{ext}"
                        with open(out_file, 'w', encoding='utf-8') as f_out:
                            f_out.writelines(current_lines)
                        print(f"Created: {out_file} (Statements: {current_count})")
                        
                        # Reset for next part
                        current_part += 1
                        current_count = 0
                        current_lines = []
                    
                    current_count += 1
                
                current_lines.append(line)
            
            # Write any remaining lines
            if current_lines:
                out_file = f"{root}_part{current_part}{ext}"
                with open(out_file, 'w', encoding='utf-8') as f_out:
                    f_out.writelines(current_lines)
                print(f"Created: {out_file} (Statements: {current_count})")

    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    base = os.path.dirname(os.path.abspath(__file__))
    
    # Configuration
    in_file = 'entity_processed.sql'  # Change this to your input file name
    in_path = os.path.join(base, in_file)
    
    # Output base name (will become entity_processed_part1.sql, etc.)
    out_path = os.path.join(base, in_file)
    
    # Number of INSERT statements per file
    sql_size = 2000

    print(f"Splitting '{in_path}' into chunks of {sql_size} statements...")
    split_sql_file(in_path, out_path, sql_size)
    print("Done.")

if __name__ == '__main__':
    main()
