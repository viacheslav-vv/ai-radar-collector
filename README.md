# ai-radar-collector

Deterministic collector for the weekly AI/Data radar. It pulls the last 7 days from free
sources, clusters items describing the same event, ranks them by **real metrics** and emits a
pre-ranked brief. The LLM only writes the issue up in the given order.

Stdlib-only Python 3.11+, no keys required.

## Sources

| Source | Access | Signal |
|---|---|---|
| Hacker News (Algolia) | free | points, comments (stories + Show HN, ≥80 pts) |
| Reddit — **disabled** (`reddit.enabled: false`) | pending Reddit Data API approval; code kept | rank in sub (RSS) / score+comments (OAuth) |
| Lobsters `ai`, `vibecoding` | free JSON | score, comments |
| OpenAI, DeepMind, Google AI, Mistral, HF blog | RSS | primary source |
| Anthropic | scrape of anthropic.com/news (no RSS) | primary source |
| Simon Willison, Latent Space, Import AI, TLDR AI | RSS | curator coverage |
| Techmeme, TechCrunch AI | RSS | news / funding |
| Hugging Face trending models | free API | likes, trending rank |
| GitHub new repos (llm, ai-agents, mcp, generative-ai) | free API (`GITHUB_TOKEN` optional) | stars |

Configure in `sources.json`. Every source is optional and isolated: a failure is recorded in
**Source status** and never aborts the run.

### Reddit (disabled until API approval)

Reddit's Developer Terms require approval before *any* access to Reddit data through its API,
and anonymous RSS / `.json` count as API access. So the source is **off**: `sources.json` has
`"reddit": {"enabled": false, ...}` and the collector is fail-closed. A missing flag or any
value other than `true` means no request to reddit.com (covered by
`test_reddit_disabled_makes_no_request`). Source status shows `reddit: disabled: <reason>`.
Do not re-enable RSS/.json. After approval the plan is OAuth with the issued credentials.

Kept code, not in use: the collector still contains an OAuth path (`REDDIT_CLIENT_ID` /
`REDDIT_CLIENT_SECRET`) and an anonymous-RSS fallback (`mode: auto`). **The anonymous RSS
path is not permitted** under the current policy stance, even though it technically works
without a key. After approval, enable OAuth only; the RSS fallback must be removed or
hard-disabled in the same change.

## Ranking

Score = discussion + breadth + novelty + primary source + popularity + funding:

- **discussion**: log(HN points) + 0.5·log(HN comments) + Reddit (score, or rank points) + Lobsters.
  The strongest post per source counts, so ten threads in one sub don't stack.
- **breadth**: +1.5 per additional independent source.
- **novelty**: +3 for a named frontier model, +1.5 for launch language, −1.5 for a patch/version bump.
- **primary**: +1.5 if a lab's own post is in the cluster.

Clustering: same canonical URL, a title Jaccard ≥ 0.5, or the same named model entity
(`opus 5.5`, `gpt 6 sol` ≠ `gpt 6 astra`).

Sections: main signal · what people are arguing about (high discussion, no new capability) ·
startups & money · tools/repos/models · data-engineering corner. Each cluster carries `why`
reasons so the writer can show why it was included.

Weights live at the top of `score_cluster`. Tune them offline:
`--save-raw` once, then re-rank with `--replay`, without network calls.

## Usage

```bash
python3 radar_collect.py                          # markdown brief to stdout (dedupe vs state/seen.json)
python3 radar_collect.py --format json --out out/latest.json
python3 radar_collect.py --history-file history.txt   # also dedupe against URLs in a text dump
python3 radar_collect.py --commit                 # record this issue's URLs after it is published
python3 radar_collect.py --save-raw out/raw.json  # keep raw items for tuning
python3 radar_collect.py --replay out/raw.json --no-dedupe   # offline re-rank
python3 -m unittest discover -s tests             # offline tests
```

A run takes ~10–20 s (Reddit disabled).

## Integration

The collector is meant to run as a pre-run step of a private weekly digest job: its ranked
output is handed to a writer (human or LLM) that writes up the items in the given order.
It has no posting, messaging or voting capability on any platform.

## Known limitations

- Reddit is disabled until the Reddit Data API application is approved; the data-engineering section is thinner without r/dataengineering.
- The Anthropic scraper depends on the page markup. If it breaks, the status shows `anthropic: ok (0)` or an error.
- The HN launch thread for a model may use a title the clusterer doesn't link. It then shows as a
  separate cluster. Check the first dry runs.
- X/Twitter is not covered. Big X discussions usually show up on HN or Reddit.

## Data handling

- Read-only. Reddit access is disabled by default (`reddit.enabled: false`) and pending approval;
  after approval it uses OAuth only (credentials from environment variables, never committed).
- Stored locally: `state/seen.json` (canonical URLs of items already included, for de-duplication)
  and optional run outputs in `out/` (titles, links, metrics). No post bodies, comments or user data.
- `out/` and `state/` are git-ignored and are not part of this repository.
