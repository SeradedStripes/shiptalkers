# Agent Instructions

## Rules

- Read the source before changing behavior.
- When documentation and source disagree, trust the source.
- Keep changes minimal and localized.
- Follow existing code style and conventions.
- Do not divert from the active task without being asked.
- Avoid blocking I/O in async code.
- Prefer explicit error handling over panics.
- No dead code.
  - Remove it or comment it out.
  - Never add `#[allow(dead_code)]`.

## Writing

These rules apply to code, comments, and documentation.

- One sentence per line.
  - Never wrap a sentence across lines.
- No em dashes.
  - Use a single hyphen instead, or a triple hyphen for a double em dash.
  - Replace em dashes with hyphens when editing text that already has them.
- Add comments only when they clarify non-obvious behavior.

## Human In The Loop

- Keep the user informed when making substantial changes.
- Do not modify unrelated user changes in the worktree.
- Do not commit, push, deploy, or change shared infrastructure without explicit approval.