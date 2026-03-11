#!/usr/bin/env python3
# ============================================================
# setup.py — Setup automático del proyecto DataWarehouse
# Ejecutar UNA sola vez desde la terminal de PyCharm:
#
#   python setup.py
#
# Compatible con Windows, macOS y Linux
# ============================================================

import os
import sys
import json
import shutil
import subprocess
import platform
from pathlib import Path


# ── Colores para la terminal ────────────────────────────────
class C:
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    GREEN  = "\033[92m"
    YELLOW = "\033[93m"
    RED    = "\033[91m"
    CYAN   = "\033[96m"
    BLUE   = "\033[94m"
    WHITE  = "\033[97m"

def ok(msg):    print(f"  {C.GREEN}✓{C.RESET}  {msg}")
def warn(msg):  print(f"  {C.YELLOW}⚠{C.RESET}  {msg}")
def err(msg):   print(f"  {C.RED}✗{C.RESET}  {msg}")
def info(msg):  print(f"  {C.CYAN}→{C.RESET}  {msg}")
def title(msg): print(f"\n{C.BOLD}{C.BLUE}{'─'*55}{C.RESET}\n{C.BOLD}{C.WHITE}  {msg}{C.RESET}\n{C.BOLD}{C.BLUE}{'─'*55}{C.RESET}")
def step(n, msg): print(f"\n{C.BOLD}{C.CYAN}[{n}]{C.RESET} {C.BOLD}{msg}{C.RESET}")


# ── Detección del sistema operativo ────────────────────────
IS_WINDOWS = platform.system() == "Windows"
ROOT       = Path(__file__).parent.resolve()
VENV_DIR   = ROOT / "venv"
VENV_PYTHON = VENV_DIR / ("Scripts/python.exe" if IS_WINDOWS else "bin/python")
VENV_PIP    = VENV_DIR / ("Scripts/pip.exe"    if IS_WINDOWS else "bin/pip")


def run(cmd: list, check=True, capture=False) -> subprocess.CompletedProcess:
    """Ejecuta un comando y muestra output en tiempo real."""
    return subprocess.run(
        cmd,
        check=check,
        capture_output=capture,
        text=True,
        cwd=str(ROOT),
    )


def run_silent(cmd: list) -> tuple[int, str, str]:
    """Ejecuta un comando silenciosamente y devuelve (returncode, stdout, stderr)."""
    r = subprocess.run(cmd, capture_output=True, text=True, cwd=str(ROOT))
    return r.returncode, r.stdout, r.stderr


# ════════════════════════════════════════════════════════════
# PASO 1 — Verificar Python
# ════════════════════════════════════════════════════════════
def check_python():
    step(1, "Verificando versión de Python")
    version = sys.version_info
    info(f"Python {version.major}.{version.minor}.{version.micro} detectado")

    if version < (3, 10):
        err("Se requiere Python 3.10 o superior.")
        err(f"Versión actual: {version.major}.{version.minor}")
        sys.exit(1)
    ok(f"Python {version.major}.{version.minor} ✓")


# ════════════════════════════════════════════════════════════
# PASO 2 — Crear entorno virtual
# ════════════════════════════════════════════════════════════
def create_virtualenv():
    step(2, "Creando entorno virtual (venv)")

    if VENV_DIR.exists():
        warn("El entorno virtual ya existe.")
        resp = input(f"  {C.YELLOW}¿Recrearlo desde cero? [s/N]:{C.RESET} ").strip().lower()
        if resp == "s":
            info("Eliminando entorno virtual anterior...")
            shutil.rmtree(VENV_DIR)
        else:
            ok("Usando entorno virtual existente.")
            return

    info("Creando venv...")
    run([sys.executable, "-m", "venv", str(VENV_DIR)])
    ok(f"Entorno virtual creado en: {VENV_DIR}")


# ════════════════════════════════════════════════════════════
# PASO 3 — Instalar dependencias
# ════════════════════════════════════════════════════════════
def install_requirements():
    step(3, "Instalando dependencias (requirements.txt)")

    req_file = ROOT / "requirements.txt"
    if not req_file.exists():
        err("No se encontró requirements.txt")
        sys.exit(1)

    info("Actualizando pip...")
    run_silent([str(VENV_PYTHON), "-m", "pip", "install", "--upgrade", "pip", "--quiet"])

    info("Instalando paquetes (puede tardar unos minutos)...")
    result = subprocess.run(
        [str(VENV_PIP), "install", "-r", str(req_file)],
        capture_output=False,
        text=True,
        cwd=str(ROOT),
    )
    if result.returncode != 0:
        err("Error instalando dependencias.")
        sys.exit(1)

    ok("Todas las dependencias instaladas correctamente.")


# ════════════════════════════════════════════════════════════
# PASO 4 — Crear archivo .env
# ════════════════════════════════════════════════════════════
def setup_env_file():
    step(4, "Configurando archivo .env")

    env_file    = ROOT / ".env"
    env_example = ROOT / ".env.example"

    if env_file.exists():
        ok(".env ya existe, no se sobreescribe.")
        return

    if not env_example.exists():
        err(".env.example no encontrado.")
        sys.exit(1)

    shutil.copy(env_example, env_file)
    ok(".env creado desde .env.example")
    print()
    warn("Debes completar las siguientes variables en .env:")

    print(f"""
  {C.CYAN}┌─────────────────────────────────────────────────┐
  │  DB_HOST=localhost                               │
  │  DB_PORT=5432                                    │
  │  DB_NAME=datawarehouse                           │
  │  DB_USER=tu_usuario                              │
  │  DB_PASSWORD=tu_password        ← COMPLETAR      │
  │  GDRIVE_FOLDER_ID=1xxxxx        ← COMPLETAR      │
  └─────────────────────────────────────────────────┘{C.RESET}""")

    collect_env_values(env_file)


def collect_env_values(env_file: Path):
    """Solicita al usuario los valores críticos del .env de forma interactiva."""
    print(f"\n  {C.BOLD}Puedes ingresar los valores ahora o editar .env manualmente después.{C.RESET}")
    resp = input(f"  {C.YELLOW}¿Configurar ahora? [S/n]:{C.RESET} ").strip().lower()
    if resp == "n":
        warn("Recuerda completar .env antes de ejecutar el pipeline.")
        return

    values = {}

    print(f"\n  {C.CYAN}PostgreSQL:{C.RESET}")
    values["DB_HOST"]     = input(f"    DB_HOST     [{C.WHITE}localhost{C.RESET}]: ").strip() or "localhost"
    values["DB_PORT"]     = input(f"    DB_PORT     [{C.WHITE}5432{C.RESET}]: ").strip() or "5432"
    values["DB_NAME"]     = input(f"    DB_NAME     [{C.WHITE}datawarehouse{C.RESET}]: ").strip() or "datawarehouse"
    values["DB_USER"]     = input(f"    DB_USER     [{C.WHITE}dw_user{C.RESET}]: ").strip() or "dw_user"
    values["DB_PASSWORD"] = input(f"    DB_PASSWORD : ").strip()

    print(f"\n  {C.CYAN}Google Drive:{C.RESET}")
    values["GDRIVE_FOLDER_ID"] = input(f"    GDRIVE_FOLDER_ID (ID de la carpeta en Drive): ").strip()

    # Reescribir .env con los valores ingresados
    content = env_file.read_text(encoding="utf-8")
    for key, val in values.items():
        if val:
            import re
            content = re.sub(rf"^{key}=.*", f"{key}={val}", content, flags=re.MULTILINE)
    env_file.write_text(content, encoding="utf-8")
    ok(".env actualizado con tus valores.")


# ════════════════════════════════════════════════════════════
# PASO 5 — Crear carpeta de credenciales Google
# ════════════════════════════════════════════════════════════
def setup_credentials_folder():
    step(5, "Preparando carpeta de credenciales Google Drive")

    creds_dir = ROOT / "config" / "credentials"
    creds_dir.mkdir(parents=True, exist_ok=True)

    gitkeep = creds_dir / ".gitkeep"
    gitkeep.touch()

    readme = creds_dir / "README.txt"
    readme.write_text(
        "Coloca aquí el archivo google_credentials.json\n"
        "descargado desde Google Cloud Console.\n"
        "¡Este directorio está en .gitignore y NO se sube a Git!\n",
        encoding="utf-8"
    )

    creds_file = creds_dir / "google_credentials.json"
    if creds_file.exists():
        ok("google_credentials.json ya presente.")
    else:
        warn("Falta el archivo de credenciales de Google.")
        print(f"""
  {C.CYAN}Pasos para obtenerlo:{C.RESET}
  1. Ve a {C.WHITE}https://console.cloud.google.com/{C.RESET}
  2. Crea o selecciona un proyecto
  3. Habilita: {C.WHITE}Google Drive API{C.RESET} y {C.WHITE}Google Sheets API{C.RESET}
  4. Ve a {C.WHITE}APIs & Services > Credentials{C.RESET}
  5. Crea {C.WHITE}OAuth 2.0 Client ID{C.RESET} (tipo: Desktop app)
  6. Descarga el JSON y guárdalo como:
     {C.GREEN}{creds_dir}/google_credentials.json{C.RESET}
        """)


# ════════════════════════════════════════════════════════════
# PASO 6 — Crear carpeta de logs
# ════════════════════════════════════════════════════════════
def setup_logs_folder():
    step(6, "Creando estructura de carpetas adicionales")
    for folder in ["logs", "data/temp", "data/raw"]:
        path = ROOT / folder
        path.mkdir(parents=True, exist_ok=True)
        gitkeep = path / ".gitkeep"
        gitkeep.touch()
    ok("Carpetas logs/ y data/ creadas.")


# ════════════════════════════════════════════════════════════
# PASO 7 — Verificar conexión a PostgreSQL
# ════════════════════════════════════════════════════════════
def verify_postgres():
    step(7, "Verificando conexión a PostgreSQL")

    env_file = ROOT / ".env"
    if not env_file.exists():
        warn("No hay .env configurado, saltando verificación.")
        return

    # Leer variables del .env
    env_vars = {}
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            env_vars[k.strip()] = v.strip()

    if not env_vars.get("DB_PASSWORD"):
        warn("DB_PASSWORD no configurado en .env, saltando verificación.")
        return

    info("Probando conexión a PostgreSQL...")
    test_script = f"""
import sys
sys.path.insert(0, r'{ROOT}')
try:
    from connectors.postgres_connector import PostgresConnector
    conn = PostgresConnector()
    _ = conn.engine
    print("OK")
except Exception as e:
    print(f"ERROR: {{e}}")
"""
    rc, stdout, stderr = run_silent([str(VENV_PYTHON), "-c", test_script])

    if "OK" in stdout:
        ok("Conexión a PostgreSQL exitosa.")
    else:
        warn(f"No se pudo conectar a PostgreSQL: {stdout.strip() or stderr.strip()}")
        warn("Verifica que PostgreSQL esté corriendo y que las credenciales en .env sean correctas.")


# ════════════════════════════════════════════════════════════
# PASO 8 — Inicializar base de datos
# ════════════════════════════════════════════════════════════
def init_database():
    step(8, "Inicializando base de datos (schemas + tablas de control)")

    env_file = ROOT / ".env"
    env_vars = {}
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env_vars[k.strip()] = v.strip()

    if not env_vars.get("DB_PASSWORD"):
        warn("DB_PASSWORD no configurado, saltando inicialización de DB.")
        warn("Cuando lo configures, ejecuta: python scripts/setup_database.py")
        return

    info("Ejecutando scripts/setup_database.py...")
    result = subprocess.run(
        [str(VENV_PYTHON), str(ROOT / "scripts" / "setup_database.py")],
        capture_output=False,
        text=True,
        cwd=str(ROOT),
    )
    if result.returncode == 0:
        ok("Base de datos inicializada correctamente.")
    else:
        warn("Hubo un problema inicializando la DB.")
        warn("Puedes hacerlo manualmente: python scripts/setup_database.py")


# ════════════════════════════════════════════════════════════
# PASO 9 — Ejecutar tests unitarios
# ════════════════════════════════════════════════════════════
def run_tests():
    step(9, "Ejecutando tests unitarios")
    info("Corriendo pytest...")
    result = subprocess.run(
        [str(VENV_PYTHON), "-m", "pytest", "tests/", "-v", "--tb=short"],
        capture_output=False,
        text=True,
        cwd=str(ROOT),
    )
    if result.returncode == 0:
        ok("Todos los tests pasaron correctamente.")
    else:
        warn("Algunos tests fallaron. Revisa la salida de pytest.")


# ════════════════════════════════════════════════════════════
# RESUMEN FINAL
# ════════════════════════════════════════════════════════════
def print_summary():
    activate = (
        r"venv\Scripts\activate"
        if IS_WINDOWS
        else "source venv/bin/activate"
    )

    print(f"""
{C.BOLD}{C.GREEN}{'═'*55}
  ✅  SETUP COMPLETADO
{'═'*55}{C.RESET}

{C.BOLD}Próximos pasos:{C.RESET}

  {C.CYAN}1.{C.RESET} Activar el entorno virtual:
     {C.WHITE}{activate}{C.RESET}

  {C.CYAN}2.{C.RESET} Completar credenciales de Google (si no lo hiciste):
     {C.WHITE}config/credentials/google_credentials.json{C.RESET}

  {C.CYAN}3.{C.RESET} Ejecutar el pipeline Bronce:
     {C.WHITE}python main.py --layer bronze{C.RESET}

  {C.CYAN}4.{C.RESET} Otros comandos útiles:
     {C.WHITE}python main.py --layer bronze --force{C.RESET}   ← recarga todo
     {C.WHITE}python main.py --layer bronze --log-level DEBUG{C.RESET}
     {C.WHITE}python scripts/setup_database.py{C.RESET}        ← reiniciar DB
     {C.WHITE}pytest tests/ -v{C.RESET}                        ← correr tests

{C.BOLD}{C.GREEN}{'═'*55}{C.RESET}
""")


# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════
def main():
    title("DataWarehouse Pipeline — Setup Automático")
    print(f"  Sistema : {platform.system()} {platform.release()}")
    print(f"  Ruta    : {ROOT}")

    check_python()
    create_virtualenv()
    install_requirements()
    setup_env_file()
    setup_credentials_folder()
    setup_logs_folder()
    verify_postgres()
    init_database()
    run_tests()
    print_summary()


if __name__ == "__main__":
    main()
