# Paste one line of your data here and run this
line = "RPSD37-S|US58039P3055|CRMYRR-R|LW16DN-L"  # Replace with actual line

print("Characters at end of line:")
for i, char in enumerate(line[-5:]):  # Last 5 characters
    print(f"Position {i}: '{char}' (Unicode: U+{ord(char):04X}, ASCII: {ord(char)})")

# Check for common invisible characters
invisible_chars = {
    '\u200B': 'Zero Width Space',
    '\u200C': 'Zero Width Non-Joiner', 
    '\u200D': 'Zero Width Joiner',
    '\u00A0': 'Non-breaking Space',
    '\u2060': 'Word Joiner',
    '\u180E': 'Mongolian Vowel Separator'
}

for char in line:
    if char in invisible_chars:
        print(f"Found: {invisible_chars[char]} (U+{ord(char):04X})")