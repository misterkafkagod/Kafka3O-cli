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
kafka3o brokers [--cluster ID]
kafka3o broker-config CLUSTER BROKER [--filter TEXT]
kafka3o metrics CLUSTER [-o json|csv]              # the Metrics tab; json/csv are the UI's downloads
kafka3o messages CLUSTER TOPIC [--partition N (--offset O | --timestamp T)] [--value text|json|base64]
kafka3o search CLUSTER TOPIC TEXT                  # the 500 most recent messages, key or value, case-sensitive
kafka3o groups [--cluster ID] [--filter TEXT]      # with exact total lag
kafka3o group CLUSTER GROUP                        # members and offsets per topic and partition
kafka3o logout
```

Changes (the same ones the UI makes):

```bash
kafka3o produce CLUSTER TOPIC --value TEXT [--key TEXT] [--header k=v]...
kafka3o create-topic CLUSTER TOPIC [--partitions N] [--replication-factor N]
kafka3o delete-topic CLUSTER TOPIC [--confirm TOPIC]   # asks you to type the topic name
kafka3o config set CLUSTER topic|broker NAME SETTING VALUE   # put -- before a negative value
kafka3o config reset CLUSTER topic|broker NAME SETTING
kafka3o partitions add CLUSTER TOPIC TOTAL             # the new total; partitions cannot be removed
kafka3o partitions reassign CLUSTER TOPIC PARTITION BROKER,BROKER...
kafka3o reassignments CLUSTER TOPIC                    # moves in progress
kafka3o reset-offsets CLUSTER GROUP TOPIC --to earliest|latest|TIME   # previews first; the group must be stopped
```

Kafka Connect and backups:

```bash
kafka3o connectors [--cluster ID]                      # connectors and task states, with errors
kafka3o connector pause|resume|restart CLUSTER NAME
kafka3o connector restart-task CLUSTER NAME TASK
kafka3o backups [--cluster ID]                         # state, last write, size, settings, restore progress
kafka3o backup start CLUSTER TOPIC [--start-from earliest|latest] [--compression gzip|zstd|snappy|none] [--records-per-file N]
kafka3o backup pause|resume|delete CLUSTER TOPIC       # delete stops the job; stored objects are kept
kafka3o restore CLUSTER TOPIC [--from TIME] [--to TIME] [--partitions 0,2] [--wait]   # appends to the topic
```

Every change shows what it will do and asks before sending; `--yes` skips the question. On a Production
cluster a change reason is required: pass `--reason` or type it when asked (with `--yes` and no reason,
nothing is sent).

Every command takes `-o json` for scripts. Message content is printed with control characters escaped, so a
message cannot send escape sequences to your terminal. Without `--cluster`, commands read every accessible cluster in parallel.

Exit codes: `0` ok, `1` usage error, not found, refused, denied or canceled, `2` a cluster could not answer
(the others are still shown), `3` not signed in or sign-in failed, `4` the change may have been applied
(check before retrying).

The session is kept in your user configuration directory (`kafka3o/session.json`, readable only by you) and lasts
at most 8 hours; `logout` ends it on the Console so no copy of the file works afterwards.
