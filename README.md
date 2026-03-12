# 🏛️ DataWarehouse Pipeline — Arquitectura Medallón SHT

Pipeline de datos en Python para ingesta de archivos Excel desde **Google Drive** 
hacia una bodega de datos local en **PostgreSQL**, siguiendo la **arquitectura Medallón** 
(Bronce → Plata → Oro).

---

## 📁 Estructura del Proyecto

```
datawarehouse_project/
│
├── config/
│   ├── __init__.py
│   ├── settings.py              # Configuración central (Pydantic Settings)
│   └── credentials/             # ⚠️ NO subir a Git
│       ├── google_credentials.json
│       └── token.json           # Se genera automáticamente
│
├── connectors/
│   ├── __init__.py
│   ├── gdrive_connector.py      # Conexión a Google Drive API
│   └── postgres_connector.py   # Conexión a PostgreSQL (SQLAlchemy)
│
├── ingestion/
│   ├── __init__.py
│   └── bronze/
│       ├── __init__.py
│       ├── bronze_pipeline.py   # Orquestador principal de Bronce
│       └── bronze_loader.py     # Normalización y carga a PostgreSQL
│
├── models/
│   ├── __init__.py
│   └── bronze/
│       ├── __init__.py
│       └── ingestion_control.py # Tabla de control de ingesta
│
├── utils/
│   ├── __init__.py
│   ├── excel_reader.py          # Lectura y normalización de Excel
│   └── logger.py                # Logger centralizado (Loguru)
│
├── scripts/
│   └── setup_database.py        # Inicialización de DB (ejecutar 1 vez)
│
├── tests/
│   ├── __init__.py
│   └── test_bronze_loader.py    # Tests unitarios
│
├── logs/                        # Logs generados automáticamente
├── docs/                        # Documentación adicional
│
├── main.py                      # Punto de entrada principal
├── requirements.txt
├── .env.example                 # Plantilla de variables de entorno
├── .env                         # ⚠️ NO subir a Git (copiar de .env.example)
└── .gitignore
```

---

## 🚀 Setup Inicial — Un Solo Comando

Abre la terminal de PyCharm (`View > Tool Windows > Terminal`) y ejecuta:

```bash
python setup.py
```

El script hace todo automáticamente:

| Paso | Qué hace |
|------|----------|
| 1 | Verifica que tengas Python 3.10+ |
| 2 | Crea el entorno virtual `venv/` |
| 3 | Instala todas las dependencias de `requirements.txt` |
| 4 | Crea el `.env` y te pide las credenciales de forma interactiva |
| 5 | Crea la carpeta `config/credentials/` con instrucciones |
| 6 | Crea las carpetas `logs/` y `data/` |
| 7 | Verifica la conexión a PostgreSQL |
| 8 | Inicializa los schemas y tablas en la base de datos |
| 9 | Corre los tests unitarios |

### Después del setup, configurar Google Drive API

1. Ve a [Google Cloud Console](https://console.cloud.google.com/)
2. Habilita **Google Drive API** y **Google Sheets API**
3. Crea credenciales **OAuth 2.0 Client ID** (tipo: Desktop app)
4. Descarga el JSON y guárdalo como:
   ```
   config/credentials/google_credentials.json
   ```

### Ejecutar el pipeline

```bash
# Activar entorno virtual (si no está activo)
source venv/bin/activate       # Linux/Mac
venv\Scripts\activate          # Windows

# Ingesta normal
python main.py --layer bronze

# Forzar recarga de todos los archivos
python main.py --layer bronze --force

# Carpeta específica de Drive
python main.py --layer bronze --folder 1xxxxxxxxxxx

# Con debug
python main.py --layer bronze --log-level DEBUG
```

---

## 🗄️ Arquitectura Medallón

```
Google Drive (Excel)
        │
        ▼
  ┌─────────────┐
  │   BRONCE    │  Datos crudos, tal como vienen de la fuente
  │  (bronze.*) │  Solo limpieza de nombres + columnas de auditoría
  └─────────────┘
        │
        ▼  (próximamente)
  ┌─────────────┐
  │    PLATA    │  Datos limpios, tipados y validados
  │  (silver.*) │  Deduplicación, normalización de tipos
  └─────────────┘
        │
        ▼  (próximamente)
  ┌─────────────┐
  │     ORO     │  Datos listos para análisis / BI
  │   (gold.*)  │  Agregaciones, métricas, dimensiones
  └─────────────┘
```

---

## 📊 Tabla de Control de Ingesta

Cada carga queda registrada en `bronze.ingestion_control`:

| Columna | Descripción |
|---|---|
| `file_name` | Nombre del archivo Excel |
| `sheet_name` | Nombre de la hoja |
| `target_table` | Tabla destino en PostgreSQL |
| `rows_loaded` | Filas cargadas |
| `status` | SUCCESS / ERROR / SKIPPED |
| `error_message` | Detalle del error si aplica |
| `started_at` | Inicio de la carga |
| `finished_at` | Fin de la carga |
| `source_modified_at` | Fecha de modificación del archivo fuente |

---

## 🧪 Tests

```bash
pytest tests/ -v
pytest tests/ -v --cov=ingestion --cov=utils
```

---

## 🔜 Próximos pasos

- [ ] Capa Plata: transformaciones y validaciones de datos
- [ ] Capa Oro: modelos analíticos y métricas de negocio
- [ ] Integración con repositorio Git (versionamiento)
- [ ] Scheduler para ejecución automática
- [ ] Notificaciones por email/Slack al finalizar
