# Contributing to MoG

First off, thank you for considering contributing to MoG! It's people like you that make MoG such a great tool.

## Code of Conduct

By participating in this project, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md).

## How Can I Contribute?

### Reporting Bugs

- **Check for duplicates**: Before opening a new issue, please search existing issues to see if the bug has already been reported.
- **Use the template**: When creating a bug report, please provide as much detail as possible, including:
  - MoG version/commit hash
  - Your environment (OS, Go version, MonkDB version)
  - Steps to reproduce
  - Expected vs. actual behavior
  - Logs (run with `MOG_LOG_LEVEL=debug`)

### Suggesting Enhancements

- **Open an issue**: Explain the feature you'd like to see and why it would be useful.
- **Discuss**: We'll discuss the proposal and see how it fits into the roadmap.

### Pull Requests

1. **Fork the repo** and create your branch from `main`.
2. **Copy `.env.example` to `.env`** and configure your local MonkDB instance.
3. **Make your changes**. If you've added code that should be tested, add tests.
4. **Ensure the test suite passes**:
   ```bash
   go test ./...
   ```
5. **Format your code**:
   ```bash
   go fmt ./...
   ```
6. **Issue a pull request**: Provide a clear description of the changes and link any related issues.

## Development Setup

### Prerequisites

- Go 1.25+
- A running MonkDB instance

### Running MoG from Source

```bash
go run ./cmd/mog
```

### Running Tests

```bash
# Run all tests
go test ./...

# Run a specific test
go test -v ./internal/mongo -run TestHandler
```

## Project Structure

- `cmd/mog/`: Main entry point for the MoG server.
- `internal/mongo/`: Implementation of the MongoDB wire protocol and command handlers.
- `internal/translator/`: Logic for translating MongoDB filters and updates into SQL.
- `internal/monkdb/`: Connection pool and interaction logic for the MonkDB backend.
- `internal/server/`: TCP server lifecycle and metrics handling.
- `docs/`: Source files for the project's documentation.

## Style Guide

- Follow standard Go idioms and naming conventions.
- Keep functions small and focused.
- Add comments for exported symbols.
- Use `uber-go/zap` for logging.

## Questions?

If you have any questions, feel free to open an issue or reach out to the maintainers.

Happy coding!

