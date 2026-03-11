#!/usr/bin/env python3
# ============================================================
# scripts/create_pull_request.py
# Crea automáticamente un Pull Request de dev → qa en GitHub
#
# Uso:
#   python scripts/create_pull_request.py
#   python scripts/create_pull_request.py --title "Mi PR" --body "Descripción"
# ============================================================

import sys
import json
import argparse
import subprocess
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).parent.parent))


def load_env() -> dict:
    """Lee el .env manualmente sin dependencias externas."""
    env = {}
    env_file = Path(__file__).parent.parent / ".env"
    if not env_file.exists():
        print("✗ No se encontró el archivo .env")
        sys.exit(1)
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip()
    return env


def get_current_branch() -> str:
    """Obtiene la rama actual del repositorio local."""
    result = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True, text=True
    )
    return result.stdout.strip()


def get_last_commit_message() -> str:
    """Obtiene el mensaje del último commit para usarlo en el PR."""
    result = subprocess.run(
        ["git", "log", "-1", "--pretty=%s"],
        capture_output=True, text=True
    )
    return result.stdout.strip()


def push_current_branch(branch: str):
    """Hace push de la rama actual antes de crear el PR."""
    print(f"  → Haciendo push de '{branch}'...")
    result = subprocess.run(
        ["git", "push", "-u", "origin", branch],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"  ⚠ Warning en push: {result.stderr.strip()}")
    else:
        print(f"  ✓ Push exitoso")


def create_pull_request(
    token: str,
    repo: str,
    title: str,
    body: str,
    head: str = "dev",
    base: str = "qa",
) -> dict:
    """
    Crea un Pull Request via GitHub API.

    Args:
        token: Token de GitHub con permisos repo
        repo:  Formato 'usuario/repositorio'
        title: Título del PR
        body:  Descripción del PR
        head:  Rama origen (default: dev)
        base:  Rama destino (default: qa)
    """
    url = f"https://api.github.com/repos/{repo}/pulls"

    payload = json.dumps({
        "title": title,
        "body":  body,
        "head":  head,
        "base":  base,
    }).encode("utf-8")

    req = Request(
        url,
        data=payload,
        headers={
            "Authorization": f"token {token}",
            "Accept":        "application/vnd.github+json",
            "Content-Type":  "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        method="POST",
    )

    try:
        with urlopen(req) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as e:
        error_body = json.loads(e.read().decode("utf-8"))
        raise RuntimeError(f"GitHub API error {e.code}: {error_body}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Crea un Pull Request automático de dev → qa"
    )
    parser.add_argument("--title", type=str, default=None,
                        help="Título del PR (default: último commit)")
    parser.add_argument("--body", type=str, default=None,
                        help="Descripción del PR")
    parser.add_argument("--head", type=str, default="dev",
                        help="Rama origen (default: dev)")
    parser.add_argument("--base", type=str, default="qa",
                        help="Rama destino (default: qa)")
    return parser.parse_args()


def main():
    args   = parse_args()
    env    = load_env()

    token = env.get("GITHUB_TOKEN")
    repo  = env.get("GITHUB_REPO")

    if not token:
        print("✗ GITHUB_TOKEN no está configurado en .env")
        sys.exit(1)
    if not repo:
        print("✗ GITHUB_REPO no está configurado en .env")
        sys.exit(1)

    current_branch = get_current_branch()
    last_commit    = get_last_commit_message()

    title = args.title or f"[{args.head.upper()} → {args.base.upper()}] {last_commit}"
    body  = args.body  or (
        f"## Pull Request automático\n\n"
        f"**Origen:** `{args.head}`  \n"
        f"**Destino:** `{args.base}`  \n\n"
        f"**Último commit:** {last_commit}\n\n"
        f"---\n"
        f"_PR generado automáticamente desde el pipeline DataWarehouse_"
    )

    print()
    print("=" * 55)
    print("  CREANDO PULL REQUEST")
    print("=" * 55)
    print(f"  Repositorio : {repo}")
    print(f"  Origen      : {args.head}")
    print(f"  Destino     : {args.base}")
    print(f"  Título      : {title}")
    print()

    # Push de la rama actual antes de crear el PR
    push_current_branch(current_branch)

    # Crear el PR
    try:
        pr = create_pull_request(
            token=token,
            repo=repo,
            title=title,
            body=body,
            head=args.head,
            base=args.base,
        )
        print()
        print(f"  ✓ Pull Request creado exitosamente!")
        print(f"  № {pr['number']}  →  {pr['html_url']}")
        print()
        print("  Recuerda aprobarlo manualmente en GitHub.")
        print("=" * 55)

    except RuntimeError as e:
        error_msg = str(e)
        if "already exists" in error_msg or "422" in error_msg:
            print("  ⚠ Ya existe un Pull Request abierto entre estas ramas.")
            print("    Revísalo en GitHub directamente.")
        else:
            print(f"  ✗ Error: {error_msg}")
        print("=" * 55)
        sys.exit(1)


if __name__ == "__main__":
    main()
