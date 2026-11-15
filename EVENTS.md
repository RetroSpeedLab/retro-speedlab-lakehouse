# Events

Every event on Kafka has the same JSON envelope. The key is the envelope's `key`; one partition per topic keeps
the order of the backend's outbox.

```json
{"id": 123, "topic": "runs.live", "key": "comet_courier", "at": "2026-11-15T12:00:00Z", "payload": {}}
```

| Field | Type | Meaning |
|-------|------|---------|
| `id` | integer | The backend outbox id, increasing; the stream keeps only the newest id per key |
| `topic` | string | One of the topics below |
| `key` | string | What the event is about, per topic |
| `at` | UTC timestamp | When the backend received the data |
| `payload` | object | The topic's payload |

## Topics in use

| Topic | Key | Payload | Producer | Delta tables |
|-------|-----|---------|----------|--------------|
| `runs.live` | game slug | The speedrun the search reports to `PUT /live/snapshot` (`metadata.speedrun`: game, frames, seconds, splits, levels, history, knowledge, ...) | backend outbox | `bronze.events`, `live.live_run`, `silver.run_versions`, `silver.game_runs` |
| `runs.snapshots` | game slug | The speedrun a slot off stream reports to `PUT /games/{slug}/snapshot` | backend outbox | `bronze.events`, `silver.run_versions`, `silver.game_runs` |
| `lab.runs` | `{game}/{stamp}` | `game`, `stamp`, `outcome` (`rejected`, `notes`, `deployed`), `cost_usd`, `minutes`, `turns`, `subagents`, `commits`, `tool_errors`, `timeline` | backend outbox | `bronze.events`, `silver.lab_runs` |
| `lab.reports` | `{game}/{name}` | `id`, `game`, `name`, `headline`, `lines`, `created`, `updated` | backend outbox | `bronze.events`, `silver.reports` |
| `highscores.entries` | highscore id | `id`, `game`, `version`, `name` (the public Discord display name on the board), `frames`, `set_at`, `inputs` (the run's inputs, one byte per frame, base64) | backend outbox | `bronze.events`, `silver.highscores` |

The backend writes a run event only when the run changed apart from `updated_at`, and at least once a minute.

## Planned topics

Created and empty; phase 2 makes the runner and core publish to them directly from black-omen.

| Topic | Key | Payload |
|-------|-----|---------|
| `runs.attempts` | game slug | One search attempt: `game`, `state`, `seed`, `frames`, `reached`, `at` |
| `runs.records` | game slug | A verified record: `game`, `frames`, `seconds`, `movie`, `version`, `at` |
| `policy.rounds` | game slug | One PPO training round: `game`, `release`, `round`, `steps`, `exits`, `reward`, `at` |
| `lab.events` | `{game}/{stamp}` | A lab-run step: `game`, `stamp`, `kind` (`started`, `tool`, `commit`, `check`, `shipped`), `at` |
| `chat.events` | channel | Aggregated chat activity per minute: `channel`, `messages`, `chatters`, `at`; never message bodies or user names |
