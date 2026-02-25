<!--
SYNC IMPACT REPORT
==================
Version change: N/A → 1.0.0 (initial constitution)
Modified principles: N/A (all principles newly created)
Added sections:
  - Performance & Optimization Standards
  - Development & Quality Workflow
Removed sections: None (template placeholder sections replaced)
Templates requiring updates:
  ✅ .specify/templates/plan-template.md (references constitution dynamically)
  ✅ .specify/templates/spec-template.md (no direct references)
  ✅ .specify/templates/tasks-template.md (no direct references)
  ✅ .specify/templates/checklist-template.md (no direct references)
  ✅ .specify/templates/constitution-template.md (source template)
Follow-up TODOs: None (all placeholders filled)
-->

# 100 Million Row Challenge Constitution

## Core Principles

### I. Performance-First Optimization (NON-NEGOTIABLE)
Every implementation decision MUST prioritize execution speed and resource efficiency. Performance benchmarks MUST be established before implementation and validated after each change. The primary goal is to win the challenge by producing the fastest solution within the 1.5GB memory constraint. All code MUST be profiled and optimized for the specific benchmark environment (2vCPUs, PHP 8.x, no JIT, no FFI).

### II. Test-Driven Development (NON-NEGOTIABLE)
TDD mandatory: Tests written → User approved → Tests fail → Then implement; Red-Green-Refactor cycle strictly enforced. All functionality MUST have corresponding unit tests. Integration tests MUST validate the complete parsing pipeline with the provided test dataset. Performance tests MUST measure and track execution time and memory usage.

### III. Code Quality & Maintainability
Code MUST follow PSR standards and be thoroughly documented. Complex algorithms MUST include inline explanations. The solution MUST be readable and maintainable, not just fast. Technical debt MUST be justified and documented if incurred for performance gains. All code MUST pass static analysis (PHPStan/Psalm) at the highest practical level.

### IV. Memory & Resource Efficiency
Memory usage MUST be optimized for the 100 million row dataset within the 1.5GB constraint. Streaming processing MUST be used where possible to avoid loading entire dataset into memory. Data structures MUST be chosen for minimal memory overhead. Garbage collection MUST be considered in tight loops. Memory profiling MUST be performed to identify and eliminate leaks.

### V. Validation & Correctness
The parser MUST produce exactly the expected JSON format as specified in the challenge requirements. Output MUST be validated against the provided test dataset using the `data:validate` command. Edge cases MUST be handled gracefully (malformed CSV, invalid dates, empty files). The solution MUST be deterministic and produce identical output for identical input.

## Performance & Optimization Standards

### Hardware Constraints
- Maximum memory: 1.5GB
- CPU: 2 vCPUs (Intel)
- No JIT compilation
- No FFI allowed

### Performance Targets
- Parse 100 million rows within competitive time (target: top 3 leaderboard position)
- Memory usage must stay under 1.5GB at all times
- CPU utilization should maximize available cores
- I/O operations must be optimized for sequential reads

### Optimization Techniques
- Use native PHP functions where faster than custom implementations
- Minimize string copying and object creation in hot paths
- Leverage PHP's built-in CSV parsing capabilities appropriately
- Consider memory-mapped files or streaming if beneficial
- Profile with Xdebug/Blackfire to identify bottlenecks

## Development & Quality Workflow

### Development Process
1. Research phase: Analyze existing solutions and performance techniques
2. Design phase: Create performance-optimized architecture
3. Implementation phase: Follow TDD with performance benchmarks
4. Optimization phase: Profile, measure, and refine
5. Validation phase: Verify correctness and performance

### Code Review Requirements
- All changes MUST be reviewed for performance implications
- Performance regression tests MUST be included
- Memory usage MUST be documented for major components
- Complex optimizations MUST include explanatory comments

### Testing Strategy
- Unit tests for individual components
- Integration tests with the test dataset
- Performance tests with varying dataset sizes
- Memory leak detection tests
- Correctness validation against expected output

## Governance
This constitution supersedes all other practices. Amendments require documentation, approval, and migration plan. All PRs/reviews must verify compliance with these principles. Complexity must be justified with measurable performance gains. Use `.specify/memory/constitution.md` for runtime development guidance.

**Version**: 1.0.0 | **Ratified**: 2026-02-25 | **Last Amended**: 2026-02-25