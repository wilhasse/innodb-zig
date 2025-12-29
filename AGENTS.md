# Project Scope
- Educational, non-production Zig port of early InnoDB sources.
- C source reference: /home/cslog/oss-embedded-innodb (authoritative behavior).
- Zig port lives in this repo; module tree under zig/.
- Work is tracked as Plane tickets in project INNODB (identifier IBD).

# Workflow
- For each ticket: study C implementation, summarize in the ticket, implement Zig equivalent, add Zig unit tests, commit.
- Keep steps small and wait for user approval before moving to the next ticket.

# Documentation
- Maintain porting roadmap and module map under zig/.
