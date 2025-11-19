# One CLI command > Multiple tool calls

## Essential Commands:

  1. Pattern Search:
    - rg -n "pattern" --glob '!node_modules/*' instead of multiple Grep calls
  2. File Finding:
    - fd filename or fd .ext directory instead of Glob tool
  3. File Preview:
    - bat -n filepath for syntax-highlighted preview with line numbers
  4. Bulk Refactoring:
    - rg -l "pattern" | xargs sed -i 's/old/new/g' for mass replacements
    - RESPECT WHITE SPACE in .py files
  5. Project Structure:
    - 'cmd //c tree' -L 2 directories for quick overview
  6. JSON Inspection:
    - jq '.key' file.json for quick JSON parsing

## The Game-Changing Pattern:

  # Find files → Pipe to xargs → Apply sed transformation
  rg -l "find_this" | xargs sed -i 's/replace_this/with_this/g'

  Efficiently replace dozens of Edit tool calls!

  Before reaching for Read/Edit/Glob tools, ask myself:

  - Can rg find this pattern faster?
  - Can fd locate these files quicker?
  - Can sed fix all instances at once?
  - Can jq extract this JSON data directly?
  - Use e.g. rg --help
