# Kafka3O CLI

`kafka3o` gives the Kafka3O console's information and actions in a terminal, for every cluster you may access.
It talks only to the Kafka3O Console (the same one the browser uses), so roles, Production rules and the audit
trail are the same as in the UI.

Specs and the roadmap live in the UI repository: `Kafka3O-UI/spec` (milestones M18–M22).

```bash
go build -o kafka3o ./cmd/kafka3o
go test ./...
```
