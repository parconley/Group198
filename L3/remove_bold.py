#!/usr/bin/env python3
"""
Carefully remove \textbf{} formatting from LaTeX document.
Handles nested braces correctly by matching opening and closing braces.
"""

def remove_textbf(text):
    """Remove \textbf{} while preserving content and handling nested braces."""
    result = []
    i = 0

    while i < len(text):
        # Check if we found \textbf{
        if text[i:i+8] == r'\textbf{':
            # Skip the \textbf{ part
            i += 8

            # Now find the matching closing brace
            brace_count = 1
            content_start = i

            while i < len(text) and brace_count > 0:
                if text[i] == '{' and (i == 0 or text[i-1] != '\\'):
                    brace_count += 1
                elif text[i] == '}' and (i == 0 or text[i-1] != '\\'):
                    brace_count -= 1

                if brace_count > 0:  # Don't include the final closing brace
                    i += 1
                else:
                    # Found the matching closing brace
                    content = text[content_start:i]
                    result.append(content)
                    i += 1  # Skip the closing brace
                    break
        else:
            result.append(text[i])
            i += 1

    return ''.join(result)


def main():
    input_file = 'Lab3_Submission.tex'

    # Read the file
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Remove \textbf{} formatting
    modified_content = remove_textbf(content)

    # Write back to file
    with open(input_file, 'w', encoding='utf-8') as f:
        f.write(modified_content)

    print(f"Removed \\textbf{{}} formatting from {input_file}")
    print(f"Original length: {len(content)}")
    print(f"New length: {len(modified_content)}")
    print(f"Difference: {len(content) - len(modified_content)} characters removed")


if __name__ == '__main__':
    main()
