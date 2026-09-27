"""Offline tests for radar_collect (stdlib unittest; no network)."""

import sys
import unittest
import urllib.error
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import radar_collect as rc  # noqa: E402

NOW = datetime(2026, 9, 27, tzinfo=timezone.utc)
SINCE = NOW - timedelta(days=7)

REDDIT_ATOM = b"""<?xml version="1.0" encoding="UTF-8"?><feed xmlns="http://www.w3.org/2005/Atom">
<entry><category term="LocalLLaMA"/><title>Qwen 4 announced</title>
<link href="https://www.reddit.com/r/LocalLLaMA/comments/abc/qwen_4/"/>
<content type="html">&lt;a href=&quot;https://qwen.ai/blog/qwen4&quot;&gt;[link]&lt;/a&gt;</content>
<published>2026-09-25T10:00:00+00:00</published></entry>
<entry><category term="ClaudeAI"/><title>Opus 5.5 thoughts</title>
<link href="https://www.reddit.com/r/ClaudeAI/comments/def/opus/"/>
<content type="html">self post</content><published>2026-09-24T10:00:00+00:00</published></entry>
<entry><category term="LocalLLaMA"/><title>Old post</title>
<link href="https://www.reddit.com/r/LocalLLaMA/comments/old/"/>
<content type="html">x</content><published>2026-09-01T10:00:00+00:00</published></entry>
</feed>"""

RSS = b"""<?xml version="1.0"?><rss version="2.0"><channel>
<item><title>Introducing GPT-6 Sol</title><link>https://openai.com/index/gpt-6-sol</link>
<pubDate>Tue, 22 Sep 2026 17:00:00 GMT</pubDate></item>
<item><title>Old thing</title><link>https://openai.com/index/old</link>
<pubDate>Tue, 01 Sep 2026 17:00:00 GMT</pubDate></item>
</channel></rss>"""


def item(title, url, source="Hacker News", kind="community", **metrics):
    return rc.Item(source, kind, title, url, NOW.isoformat(), None, metrics)


class ParsingTests(unittest.TestCase):
    def test_rss_window(self):
        items = rc.parse_feed(RSS, "OpenAI", "lab", SINCE)
        self.assertEqual([i.title for i in items], ["Introducing GPT-6 Sol"])

    def test_reddit_atom_external_link_rank_and_window(self):
        items = rc.parse_reddit_atom(REDDIT_ATOM, SINCE)
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].url, "https://qwen.ai/blog/qwen4")
        self.assertEqual(items[0].source, "r/LocalLLaMA")
        self.assertEqual(items[1].url, items[1].discussion_url)  # self post
        self.assertEqual(items[1].metrics["rank"], 1)  # per-subreddit rank

    def test_reddit_bot_wall_raises(self):
        with self.assertRaises(RuntimeError):
            rc.parse_reddit_atom(b"<!DOCTYPE html><title>Welcome to Reddit</title>", SINCE)

    def test_canonical_url(self):
        self.assertEqual(
            rc.canonical_url("http://www.openai.com/index/x/?utm_source=a&id=1"),
            "https://openai.com/index/x?id=1",
        )


class RankingTests(unittest.TestCase):
    def test_same_url_and_same_model_cluster(self):
        items = [
            item("Introducing GPT-6 Sol and Luna", "https://openai.com/index/sol", "OpenAI", "lab"),
            item("GPT-6 Sol and Luna", "https://openai.com/index/sol/", points=1700, comments=800),
            item("GPT-6 Sol and Luna pricing war", "https://simonwillison.net/x", "Simon Willison", "curator"),
            item("Unrelated cat pictures with AI", "https://cats.example"),
        ]
        clusters = rc.cluster_items(items)
        self.assertEqual(len(clusters), 2)
        self.assertEqual(len(clusters[0].items), 3)

    def test_frontier_launch_beats_patch_and_sections(self):
        sections, _ = rc.rank(
            [
                item("Introducing Claude Opus 5.5", "https://anthropic.com/opus", "Anthropic", "lab"),
                item("dbt-core v1.11.3 patch release", "https://github.com/dbt/rel", points=120, comments=10),
                item("Acme AI raises $100M Series B", "https://tc.example/acme", "TechCrunch AI", "news"),
                item("Is AI making us worse programmers?", "https://blog.example/ai", points=600, comments=700),
            ],
            history=set(),
        )
        self.assertEqual(sections["main"][0].title, "Introducing Claude Opus 5.5")
        self.assertEqual(sections["startups_funding"][0].factors["funding"] > 0, True)
        self.assertEqual(sections["debate"][0].title, "Is AI making us worse programmers?")
        self.assertLessEqual(sections["data_engineering"][0].factors["novelty"], 0)

    def test_history_dedupe(self):
        sections, dropped = rc.rank(
            [item("Claude Opus 5.5 is out", "https://anthropic.com/opus", points=500)],
            history={rc.canonical_url("https://anthropic.com/opus")},
        )
        self.assertEqual(dropped, 1)
        self.assertFalse(any(sections.values()))

    def test_evidence_rule(self):
        sections, _ = rc.rank(
            [
                item("Introducing Claude Opus 5.5", "https://www.anthropic.com/claude-opus-5-5", "Anthropic", "lab"),
                item("Acme AI raises $100M Series B", "https://tc.example/acme", "TechCrunch AI", "news"),
                item("Court upholds AI ban for Foo", "https://cnbc.example/x", points=500, comments=900),
            ],
            history=set(),
        )
        flat = {c.title: c for v in sections.values() for c in v}
        self.assertEqual(flat["Introducing Claude Opus 5.5"].evidence, "primary")
        self.assertEqual(flat["Acme AI raises $100M Series B"].evidence, "reported")
        self.assertEqual(flat["Acme AI raises $100M Series B"].claim_type, "funding")
        self.assertEqual(flat["Court upholds AI ban for Foo"].evidence, "discussion-only")
        self.assertEqual(flat["Court upholds AI ban for Foo"].claim_type, "court/regulation")

    def test_funding_usd(self):
        self.assertEqual(rc.funding_usd("secures $3.36B in financing"), 3.36e9)
        self.assertEqual(rc.funding_usd("raised a $27.5M Series A"), 27.5e6)


class RobustnessTests(unittest.TestCase):
    def test_429_retry_then_success(self):
        calls = {"n": 0}

        def fn():
            calls["n"] += 1
            if calls["n"] < 3:
                raise urllib.error.HTTPError("u", 429, "Too Many", {}, None)
            return ["ok"]

        self.assertEqual(rc._with_429_retry(fn, attempts=3, wait=0), ["ok"])

    def test_reddit_failure_never_raises(self):
        class Boom(rc.Http):
            def get(self, *a, **k):
                raise urllib.error.HTTPError("u", 429, "Too Many", {}, None)

        status = {}
        cfg = {"reddit": {"groups": [["a"], ["b"]], "rss_pause_seconds": 0, "rss_retries": 0}}
        out = rc.fetch_reddit(Boom("ua", 1), cfg, SINCE, status)
        self.assertEqual(out, [])
        self.assertIn("error", status["reddit:r/a"])
        self.assertTrue(status["reddit:mode"].startswith("rss"))

    def test_reddit_disabled_makes_no_request(self):
        seen = []
        orig = rc.Http.get, rc.Http.json

        def spy_get(self, url, *a, **k):
            seen.append(url)
            raise urllib.error.URLError("offline test")

        def spy_json(self, url, *a, **k):
            seen.append(url)
            raise urllib.error.URLError("offline test")

        rc.Http.get, rc.Http.json = spy_get, spy_json
        try:
            off = {"hn": {"enabled": False}, "lobsters": {"enabled": False}, "huggingface": {"enabled": False},
                   "github": {"enabled": False}, "anthropic": {"enabled": False}, "feeds": []}
            for reddit in ({"enabled": False, "groups": [["a"]]}, {"groups": [["a"]]}, None):
                cfg = dict(off, **({"reddit": reddit} if reddit is not None else {}))
                items, meta = rc.collect(cfg, 7, NOW)
                self.assertEqual(items, [])
                self.assertTrue(meta["status"]["reddit"].startswith("disabled"))
                self.assertNotIn("reddit:mode", meta["status"])
        finally:
            rc.Http.get, rc.Http.json = orig
        self.assertFalse([u for u in seen if "reddit.com" in u])


if __name__ == "__main__":
    unittest.main()
