# Claude Guidelines for Folio

## Commit Rules

- Do not commit CLAUDE.md
- Do not commit `*-audit.md` files

## Communication

When asked a question, answer it directly. Do not go off on tangents or provide related but unasked-for information. If the user has to re-ask the same question in a different way, the first answer missed the point.

When asked a question, stop to answer it. Do not continue coding or making changes while a question is pending.

If unsure about requirements, intent, or approach, stop and ask clarifying questions before proceeding.

## Collaboration

Provide recommendations. Do not ask "what direction do you want to go" - think through the problem and propose the best solution. Engage collaboratively, not passively.

Think through problems before responding. Consider DRY opportunities, developer experience, and best practices. Do not provide minimum-effort answers that push decisions back to the user.

Look for ways to simplify and consolidate. One function that handles multiple cases is better than separate functions for each case.

Use standard library functions directly. Do not create wrapper functions for things like `strings.Index`, `bytes.Contains`, etc. If Go stdlib has it, use it - do not document it as a custom primitive or create a `find()` wrapper.

## Design vs Implementation

When asked to create a design document, write design documentation, not implementation code. Use descriptions, diagrams, tables, and pseudocode where appropriate. Save actual code for implementation tasks.

When starting a new project or feature from scratch, treat it as greenfield. Do not reference previous versions, migrations, or "changes from" anything.

## Code Style

Follow Go conventions and the standard library style guide. Package-level doc comments on each file. Exported functions have doc comments. Use `slog` for structured logging. Wrap errors with context via `fmt.Errorf("context: %w", err)`. No panics in library code. All public methods safe for concurrent use.

Use short, Go-idiomatic names. No long method names like `findClosingQuote` or `waitForCompaction`. Prefer `find`, `block`, `seek`. Single-word or two-word names. If a name feels long, it is wrong.

Use British English for code comments (e.g., "organised", "behaviour", "colour"). American English is acceptable for method names and identifiers.

## Build

- Use `go build ./...` to check compilation - never `go build .` which writes a binary
- When editing Go files, do not worry about precise whitespace or indentation - `gofmt` will normalise formatting automatically

