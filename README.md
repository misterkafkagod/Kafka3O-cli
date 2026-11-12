# Kafka3O CLI

`kafka3o` gives the Kafka3O console's information and actions in a terminal, for every cluster you may access.
It talks only to the Kafka3O Console (the same one the browser uses), so roles, Production rules and the audit
trail are the same as in the UI.

Specs and the roadmap live in the UI repository: `Kafka3O-UI/spec` (milestones M18–M22).

## Build and test

```bash
go build -o kafka3o ./cmd/kafka3o
go test ./...
```

## Use

```bash
kafka3o login --console http://localhost:5173      # the URL you open in the browser; asks for user and password
kafka3o whoami                                     # account, session expiry, role per cluster
kafka3o clusters                                   # every accessible cluster with its health
kafka3o topics [--cluster ID] [--filter TEXT] [--internal]
kafka3o topic CLUSTER TOPIC                        # partitions, configuration, consumer groups, backup
kafka3o logout
```

Every command takes `-o json` for scripts. Without `--cluster`, commands read every accessible cluster in parallel.

Exit codes: `0` ok, `1` usage error or not found, `2` a cluster could not answer (the others are still shown),
`3` not signed in or sign-in failed.

The session is kept in your user configuration directory (`kafka3o/session.json`, readable only by you) and lasts
at most 8 hours; `logout` ends it on the Console so no copy of the file works afterwards.
