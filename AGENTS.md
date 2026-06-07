# Repository Guidelines

## Project Structure & Module Organization

This repository is a collection of mini big data labs. The main overview lives in `README.md`, and the roadmap and lab standards are in `docs/mini-big-data-labs.md`.

Each lab lives under `labs/<lab-name>/` and follows the same layout:

- `README.md`: lab overview and goals
- `notes/scope.md`: scoped design notes and milestones
- `src/`: implementation code
- `tests/`: lab-specific checks

Use `templates/lab/` when creating a new lab or extending an existing one.

Repository-level build config lives in `.mvn/`. Maven artifacts are cached in the repository-local `.m2/` directory instead of `~/.m2/`.

Language rules are fixed at the repository level:

- all labs use JVM languages except `labs/mini-terraform/`
- JVM-based labs use Maven as the build and dependency manager
- `mini-terraform` uses Go

Always use paths relative to the repository root in documentation and code comments. Do not write absolute local filesystem paths.

## Build, Test, and Development Commands

There is no global root build yet. Work at the lab level and use the stack required by that lab.

- `find labs -maxdepth 3 | sort`: inspect the current lab structure
- `sed -n '1,200p' docs/mini-big-data-labs.md`: review the overall roadmap
- `git status --short`: verify only intended files changed
- `cd labs/mini-kafka && mvn test`: run tests for a JVM-based lab
- `cd labs/mini-lsm-kv-store && mvn package`: build a JVM-based lab
- `cd labs/mini-terraform && go test ./...`: run tests for the Go lab

JVM labs should rely on the repository root `.mvn/maven.config`, which sets `-Dmaven.repo.local=.m2/repository`. Do not switch Maven back to `~/.m2`, and do not commit `.m2/` or `target/`.

When a lab adds runnable code, document its local commands in that lab’s `README.md`.

## Coding Style & Naming Conventions

Keep files and directories lowercase with hyphenated names, matching existing labs such as `mini-kafka` and `mini-lsm-kv-store`.

Prefer small, readable modules in `src/` and keep design reasoning in `notes/` rather than in large code comments. For JVM labs, follow standard Maven layout and conventions. For Go code in `mini-terraform`, follow `gofmt` output and standard Go package naming.

## Testing Guidelines

Place tests in the corresponding lab’s `tests/` directory or in the language-standard test location adopted by that lab once implementation starts. Name test files after the unit under test, for example `WalTest.java`, `PlannerTest.java`, or `plan_test.go`.

Each lab should eventually cover:

- happy path behavior
- failure or recovery cases
- one or two scope-defining edge cases

Document the exact test command in the lab’s `README.md` once a test framework is added. JVM labs should default to Maven commands such as `mvn test`.

## Commit & Pull Request Guidelines

Use short, imperative commit messages in the existing style, for example: `init mini big data labs scaffold`.

Pull requests should include:

- a concise summary of what changed
- the lab(s) affected
- any new commands needed to run or test locally
- updated docs when scope or structure changes

## Documentation Expectations

If you change lab scope, update both the lab-local `notes/scope.md` and the shared roadmap in `docs/mini-big-data-labs.md`.
