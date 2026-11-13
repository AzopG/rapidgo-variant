"""Pruebas de scripts/contributions.py: parseo, tabla determinista y CLI (sin red)."""

import pytest

from scripts import contributions as c

RAW_ISSUES = [
    {"number": 21, "assignees": [{"login": "vannysys"}], "state": "OPEN"},
    {"number": 20, "assignees": [{"login": "AzopG"}], "state": "CLOSED"},
    {"number": 22, "assignees": [{"login": "FelipeMaldonado10"}, {"login": "AzopG"}], "state": "OPEN"},
]

RAW_PRS = [
    {"number": 50, "author": {"login": "AzopG"}, "closingIssuesReferences": [{"number": 20}]},
    {"number": 47, "author": {"login": "vannysys"}, "closingIssuesReferences": [{"number": 16}]},
    {"number": 46, "author": {"login": "FelipeMaldonado10"}, "closingIssuesReferences": []},
]

RAW_REVIEWS = [
    {"user": {"login": "FelipeMaldonado10"}, "state": "APPROVED"},
    {"user": {"login": "AzopG"}, "state": "COMMENTED"},
    {"user": {"login": ""}, "state": "COMMENTED"},
]

RAW_RUNS = {
    "workflow_runs": [
        {"id": 43112804120, "actor": {"login": "AzopG"}},
        {"id": 43106882082, "actor": {"login": "FelipeMaldonado10"}},
        {"id": 42128737738, "actor": {"login": "vannysys"}},
    ]
}


def history(**overrides):
    values = {
        "issues": c.parse_issues(RAW_ISSUES),
        "pull_requests": c.parse_pull_requests(RAW_PRS),
        "reviews": c.parse_reviews(50, RAW_REVIEWS),
        "dispatch": c.parse_dispatch(RAW_RUNS),
    }
    values.update(overrides)
    return c.History(**values)


def test_parse_issues_sorts_and_keeps_assignees():
    issues = c.parse_issues(RAW_ISSUES)

    assert [issue.number for issue in issues] == [20, 21, 22]
    assert issues[2].assignees == ("AzopG", "FelipeMaldonado10")
    assert issues[1].state == "OPEN"


def test_parse_pull_requests_extracts_closing_issues():
    pull_requests = c.parse_pull_requests(RAW_PRS)

    assert [pr.number for pr in pull_requests] == [46, 47, 50]
    assert pull_requests[2].author == "AzopG"
    assert pull_requests[2].issues == (20,)
    assert pull_requests[0].issues == ()


def test_parse_reviews_drops_entries_without_user():
    reviews = c.parse_reviews(50, RAW_REVIEWS)

    assert [(review.reviewer, review.state) for review in reviews] == [
        ("FelipeMaldonado10", "APPROVED"),
        ("AzopG", "COMMENTED"),
    ]
    assert {review.pr_number for review in reviews} == {50}


def test_parse_dispatch_sorts_runs():
    runs = c.parse_dispatch(RAW_RUNS)

    assert [run.run_id for run in runs] == [42128737738, 43106882082, 43112804120]
    assert runs[0].actor == "vannysys"


def test_build_table_keeps_the_team_order():
    rows = c.build_table(history()).splitlines()[2:]

    assert len(rows) == 3
    assert rows[0].startswith("| Ivan Palacio |")
    assert rows[1].startswith("| Andrés Maldonado |")
    assert rows[2].startswith("| Luis Jaraba |")


def test_build_table_links_each_pr_under_its_author():
    table = c.build_table(history())

    ivan = next(line for line in table.splitlines() if line.startswith("| Ivan Palacio |"))
    luis = next(line for line in table.splitlines() if line.startswith("| Luis Jaraba |"))

    assert "[#50](https://github.com/AzopG/rapidgo-variant/pull/50)" in ivan
    assert "[#47](https://github.com/AzopG/rapidgo-variant/pull/47)" in luis
    assert "#46" not in ivan


def test_build_table_lists_assigned_issues():
    table = c.build_table(history())

    ivan = next(line for line in table.splitlines() if line.startswith("| Ivan Palacio |"))
    luis = next(line for line in table.splitlines() if line.startswith("| Luis Jaraba |"))

    assert "[#20](" in ivan and "[#22](" in ivan
    assert "[#21](" in luis and "[#20](" not in luis


def test_build_table_credits_reviews_to_the_reviewer():
    reviews = (
        c.Review(pr_number=50, reviewer="FelipeMaldonado10"),
        c.Review(pr_number=47, reviewer="AzopG"),
    )
    table = c.build_table(history(reviews=reviews))

    andres = next(line for line in table.splitlines() if line.startswith("| Andrés Maldonado |"))
    ivan = next(line for line in table.splitlines() if line.startswith("| Ivan Palacio |"))

    assert "[#50](" in andres.split("|")[4]
    assert "[#47](" in ivan.split("|")[4]


def test_build_table_ignores_self_reviews():
    reviews = (c.Review(pr_number=50, reviewer="AzopG"),)
    table = c.build_table(history(reviews=reviews))

    ivan = next(line for line in table.splitlines() if line.startswith("| Ivan Palacio |"))

    assert ivan.split("|")[4].strip() == "—"


def test_build_table_lists_dispatch_runs_per_actor():
    table = c.build_table(history())

    luis = next(line for line in table.splitlines() if line.startswith("| Luis Jaraba |"))

    assert "[42128737738](https://github.com/AzopG/rapidgo-variant/actions/runs/42128737738)" in luis


def test_build_table_is_deterministic_under_shuffled_input():
    ordered = history()
    shuffled = c.History(
        issues=tuple(reversed(ordered.issues)),
        pull_requests=tuple(reversed(ordered.pull_requests)),
        reviews=tuple(reversed(ordered.reviews)),
        dispatch=tuple(reversed(ordered.dispatch)),
    )

    assert c.build_table(shuffled) == c.build_table(ordered)


def test_build_table_renders_dashes_when_empty():
    table = c.build_table(c.History())

    assert table.count("—") == 12


def test_main_prints_the_table(monkeypatch, capsys):
    monkeypatch.setattr(c, "build_history", lambda repo: history())

    assert c.main(["--repo", "AzopG/rapidgo-variant"]) == 0
    out = capsys.readouterr().out
    assert "| Integrante |" in out
    assert "[#50](https://github.com/AzopG/rapidgo-variant/pull/50)" in out


def test_gh_json_reports_the_real_error(monkeypatch):
    class Result:
        returncode = 1
        stdout = ""
        stderr = "HTTP 404\n"

    monkeypatch.setattr(c.subprocess, "run", lambda *a, **k: Result())

    with pytest.raises(RuntimeError, match="HTTP 404"):
        c.gh_json("api", "repos/x/y")
