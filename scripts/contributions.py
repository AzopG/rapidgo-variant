"""Tabla de contribuciones de la entrega, derivada del historial de GitHub.

Consulta `gh` en modo lectura (issues, PRs mergeados, reviews y corridas manuales)
y emite una tabla Markdown por integrante. La salida es determinista: el orden de
las filas y de las referencias no depende del orden en que responde la API.

Uso:

    python -m scripts.contributions
    python -m scripts.contributions --repo OWNER/REPO

Requiere `gh` autenticado. No escribe nada en el repositorio: la tabla se copia al
README cuando el historial de la semana ya esta cerrado.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

GITHUB = "https://github.com"
DEFAULT_REPO = "AzopG/rapidgo-variant"
EMPTY = "—"


@dataclass(frozen=True)
class Member:
    name: str
    login: str


TEAM: tuple[Member, ...] = (
    Member("Ivan Palacio", "AzopG"),
    Member("Andrés Maldonado", "FelipeMaldonado10"),
    Member("Luis Jaraba", "vannysys"),
)


@dataclass(frozen=True)
class Issue:
    number: int
    assignees: tuple[str, ...] = ()
    state: str = "OPEN"


@dataclass(frozen=True)
class PullRequest:
    number: int
    author: str
    issues: tuple[int, ...] = ()


@dataclass(frozen=True)
class Review:
    pr_number: int
    reviewer: str
    state: str = "APPROVED"


@dataclass(frozen=True)
class DispatchRun:
    run_id: int
    actor: str


@dataclass(frozen=True)
class History:
    issues: tuple[Issue, ...] = ()
    pull_requests: tuple[PullRequest, ...] = ()
    reviews: tuple[Review, ...] = ()
    dispatch: tuple[DispatchRun, ...] = ()


def parse_issues(raw: Iterable[dict[str, Any]]) -> tuple[Issue, ...]:
    """Normaliza la salida de `gh issue list --json number,assignees,state`."""
    issues = []
    for item in raw:
        logins = tuple(sorted(assignee["login"] for assignee in item.get("assignees") or []))
        issues.append(
            Issue(number=int(item["number"]), assignees=logins, state=str(item.get("state", "OPEN")))
        )
    return tuple(sorted(issues, key=lambda issue: issue.number))


def parse_pull_requests(raw: Iterable[dict[str, Any]]) -> tuple[PullRequest, ...]:
    """Normaliza `gh pr list --json number,author,closingIssuesReferences`."""
    pull_requests = []
    for item in raw:
        references = item.get("closingIssuesReferences") or []
        issues = tuple(sorted(int(ref["number"]) for ref in references if "number" in ref))
        author = (item.get("author") or {}).get("login", "")
        pull_requests.append(
            PullRequest(number=int(item["number"]), author=author, issues=issues)
        )
    return tuple(sorted(pull_requests, key=lambda pr: pr.number))


def parse_reviews(pr_number: int, raw: Iterable[dict[str, Any]]) -> tuple[Review, ...]:
    """Normaliza `gh api repos/OWNER/REPO/pulls/<n>/reviews`."""
    reviews = []
    for item in raw:
        reviewer = (item.get("user") or {}).get("login", "")
        if not reviewer:
            continue
        reviews.append(
            Review(pr_number=int(pr_number), reviewer=reviewer, state=str(item.get("state", "")))
        )
    return tuple(reviews)


def parse_dispatch(raw: dict[str, Any]) -> tuple[DispatchRun, ...]:
    """Normaliza `gh api repos/OWNER/REPO/actions/runs?event=workflow_dispatch`."""
    runs = []
    for item in raw.get("workflow_runs") or []:
        actor = (item.get("actor") or {}).get("login", "")
        if not actor:
            continue
        runs.append(DispatchRun(run_id=int(item["id"]), actor=actor))
    return tuple(sorted(runs, key=lambda run: run.run_id))


def _sequence(value: Any) -> list[Any]:
    """Acepta listas y tuplas sin atarse al tipo concreto del fixture."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    raise TypeError(f"se esperaba una secuencia, llego {type(value).__name__}")


def _links(numbers: Sequence[int], repo: str, path: str, prefix: str = "#") -> str:
    if not numbers:
        return EMPTY
    return ", ".join(
        f"[{prefix}{number}]({GITHUB}/{repo}/{path}/{number})" for number in numbers
    )


def _run_links(run_ids: Sequence[int], repo: str) -> str:
    if not run_ids:
        return EMPTY
    return ", ".join(f"[{run_id}]({GITHUB}/{repo}/actions/runs/{run_id})" for run_id in run_ids)


def build_table(history: History, repo: str = DEFAULT_REPO, team: Sequence[Member] = TEAM) -> str:
    """Emite la tabla Markdown; mismo `history` produce siempre el mismo texto."""
    authors = {pr.number: pr.author for pr in _sequence(history.pull_requests)}
    issues = _sequence(history.issues)
    pull_requests = _sequence(history.pull_requests)
    reviews = _sequence(history.reviews)
    dispatch = _sequence(history.dispatch)

    lines = [
        "| Integrante | Issues asignados | PRs mergeados | Reviews a PR ajenos | `workflow_dispatch` |",
        "| --- | --- | --- | --- | --- |",
    ]
    for member in team:
        assigned = sorted(issue.number for issue in issues if member.login in issue.assignees)
        authored = sorted(pr.number for pr in pull_requests if pr.author == member.login)
        reviewed = sorted(
            {
                review.pr_number
                for review in reviews
                if review.reviewer == member.login
                and authors.get(review.pr_number, "") != member.login
            }
        )
        runs = sorted({run.run_id for run in dispatch if run.actor == member.login})
        lines.append(
            "| {name} | {issues} | {prs} | {reviews} | {runs} |".format(
                name=member.name,
                issues=_links(assigned, repo, "issues"),
                prs=_links(authored, repo, "pull"),
                reviews=_links(reviewed, repo, "pull"),
                runs=_run_links(runs, repo),
            )
        )
    return "\n".join(lines)


def gh_json(*args: str) -> Any:
    """Ejecuta `gh` y devuelve el JSON; falla con el stderr real si el comando falla."""
    completed = subprocess.run(
        ["gh", *args], capture_output=True, text=True, encoding="utf-8", check=False
    )
    if completed.returncode != 0:
        detail = (completed.stderr or "").strip() or "sin detalle"
        raise RuntimeError(f"gh {' '.join(args)} fallo: {detail}")
    return json.loads(completed.stdout or "null")


def build_history(repo: str = DEFAULT_REPO) -> History:
    """Lee el historial real del repositorio. Las reviews se piden por PR mergeado."""
    issues = parse_issues(
        gh_json(
            "issue", "list", "--repo", repo, "--state", "all", "--limit", "200",
            "--json", "number,assignees,state",
        )
    )
    pull_requests = parse_pull_requests(
        gh_json(
            "pr", "list", "--repo", repo, "--state", "merged", "--limit", "200",
            "--json", "number,author,closingIssuesReferences",
        )
    )
    reviews = tuple(
        review
        for pr in pull_requests
        for review in parse_reviews(
            pr.number, gh_json("api", f"repos/{repo}/pulls/{pr.number}/reviews")
        )
    )
    dispatch = parse_dispatch(
        gh_json("api", f"repos/{repo}/actions/runs?event=workflow_dispatch&per_page=100")
    )
    return History(
        issues=issues, pull_requests=pull_requests, reviews=reviews, dispatch=dispatch
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Tabla de contribuciones desde el historial de GitHub.")
    parser.add_argument("--repo", default=DEFAULT_REPO, help="repositorio OWNER/REPO (default: %(default)s)")
    args = parser.parse_args(argv)
    print(build_table(build_history(args.repo), args.repo))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
