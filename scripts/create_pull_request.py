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
import shutil
import argparse
import subprocess
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).parent.parent))


def find_git() -> str:
    """
    Busca el ejecutable de git en el sistema.
    Funciona en Windows, Mac y Linux.
    """
    # Primero intenta encontrarlo en el PATH
    git = shutil.which("git")
    if git:
        return git

    # Rutas comunes en Windows
    windows_paths = [
        r"C:\Program Files\Git\bin\git.exe",
        r"C:\Program Files (x86)\Git\bin\git.exe",
        r"C:\Users\{}\AppData\Local\Programs\Git\bin\git.exe".format(
            Path.home().name
        ),
    ]
    for path in windows_paths:
        if Path(path).exists():
            return path

    print("✗ No se encontró Git instalado.")
    print("  Descárgalo desde: https://git-scm.com/download/win")
    sys.exit(1)


GIT = find_git()


def run_git(*args) -> subprocess.CompletedProcess:
    """Ejecuta un comando git con la ruta correcta."""
    return subprocess.run(
        [GIT] + list(args),
        capture_output=True,
        text=True,
        cwd=str(Path(__file__).parent.parent)
    )


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
    result = run_git("rev-parse", "--abbrev-ref", "HEAD")
    return result.stdout.strip()


def get_last_commit_message() -> str:
    result = run_git("log", "-1", "--pretty=%s")
    return result.stdout.strip()


def push_current_branch(branch: str):
    print(f"  → Haciendo push de '{branch}'...")
    result = run_git("push", "-u", "origin", branch)

    if result.returncode == 0:
        print(f"  ✓ Push exitoso")
        return

    # Si el remoto tiene cambios que no tenemos → pull --rebase y reintentar
    if "fetch first" in result.stderr or "rejected" in result.stderr:
        print(f"  ⚠ Remoto desactualizado, sincronizando...")
        rebase = run_git("pull", "origin", branch, "--rebase")
        if rebase.returncode != 0:
            print(f"  ✗ Error en rebase: {rebase.stderr.strip()}")
            sys.exit(1)
        print(f"  ✓ Sincronización exitosa, reintentando push...")
        retry = run_git("push", "-u", "origin", branch)
        if retry.returncode != 0:
            print(f"  ✗ Error en push tras rebase: {retry.stderr.strip()}")
            sys.exit(1)
        print(f"  ✓ Push exitoso")
    else:
        print(f"  ✗ Error en push: {result.stderr.strip()}")
        sys.exit(1)


def create_pull_request(
    token: str,
    repo: str,
    title: str,
    body: str,
    head: str = "dev",
    base: str = "qa",
) -> dict:
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
            "Authorization":        f"token {token}",
            "Accept":               "application/vnd.github+json",
            "Content-Type":         "application/json",
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
    parser.add_argument("--body",  type=str, default=None,
                        help="Descripción del PR")
    parser.add_argument("--head",  type=str, default="dev",
                        help="Rama origen (default: dev)")
    parser.add_argument("--base",  type=str, default="qa",
                        help="Rama destino (default: qa)")
    return parser.parse_args()


def main():
    args  = parse_args()
    env   = load_env()

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
    print(f"  Rama actual : {current_branch}")
    print(f"  Origen      : {args.head}")
    print(f"  Destino     : {args.base}")
    print(f"  Título      : {title}")
    print()

    push_current_branch(current_branch)

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
