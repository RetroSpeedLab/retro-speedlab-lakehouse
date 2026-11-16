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

## Search topics

The runner on black-omen publishes these straight to Kafka through core's event sink, and the lakehouse's `chat`
process publishes `chat.events`. Their envelope has no outbox `id`; the Kafka offset orders them:

```json
{"topic": "runs.attempts", "key": "comet_courier", "at": "2026-11-15T12:00:00.000+00:00", "payload": {}}
```

Every payload carries `game` (except `chat.events`) and `at`. All of them land in `bronze.search_events`; the
`search` query of the stream counts the last hour into the `live` tables after every micro-batch: it triggers every
minute, and on the busy Home Lab a batch takes about five minutes.

| Topic | Key | Payload | Producer | Delta tables |
|-------|-----|---------|----------|--------------|
| `runs.attempts` | game slug | One finished search or polisher job: `state`, `seed`, `role`, `seconds`, `worker_seconds`, `outcome` (`crossing`, `checkpoint`, `nothing`), `crossings`, `frames`, `reached` | core's searcher | `bronze.search_events`, `live.search_pulse`, `gold.search_allocation` |
| `runs.steps` | game slug | One search step: `kind` (`found`, `checkpoint`, `stuck`, `improved`, `lab`, `unverified`), `state`, `frames`, `saved`, and the `role` and `worker_seconds` of the job a `found` or `improved` step took its crossing from | core's search | `bronze.search_events`, `live.search_pulse`, `gold.search_allocation` |
| `runs.allocations` | game slug | The workers of one game's turn while the search bandit is on: `turn`, `arm` (`bandit`, `holdout`, `static`), `workers` per kind, `recommended_at`, `turn_started_at`, `turn_ends_at` | the runner | `bronze.search_events`, `gold.search_bandit` |
| `runs.records` | game slug | A new record from power-on that replays: `version`, `frames`, `seconds`, `pieces`, `previous_frames` | core's record | `bronze.search_events`, `live.search_pulse` |
| `policy.rounds` | game slug | One PPO round: `run` (W&B run path), `round`, `steps`, `reward`, `exits`, `episodes`, `exit_rate`, `entropy`, `explained_variance`, `loss` | core's policy trainer | `bronze.search_events`, `live.policy_pulse` |
| `lab.events` | `{game}/{stamp}` | A lab-run step: `stamp`, `kind` (`started`, `judging`, `finished`), and on `finished` its `outcome` and `commits` | the runner's lab job | `bronze.search_events`, `live.lab_status` |
| `chat.events` | channel | One minute of Twitch chat: `channel`, `minute`, `messages`, `chatters`; never a message or a name | the lakehouse's `chat` process | `bronze.search_events`, `live.chat_pulse` |
