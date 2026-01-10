# app.py
import os
import io
import uuid
import base64
import time
from datetime import datetime, date
from zoneinfo import ZoneInfo

import streamlit as st
import pandas as pd
from PIL import Image
import bcrypt
import requests


# ============================================================
# CONFIG
# ============================================================
st.set_page_config(page_title="Checklist UDL", layout="wide")

TZ_MX = ZoneInfo("America/Mexico_City")

STATUS_OPTIONS = [
    "OK",
    "FALTA_REPOSICION",
    "NO_FUNCIONA_MANTENIMIENTO",
    "DAÑADO_REPOSICION",
    "DAÑADO_MANTENIMIENTO",
    "N_A",
]
COND_OPTIONS = ["BUENO", "REGULAR", "MALO", "N_A"]

MAX_UPLOAD_MB = 3
MAX_STORED_KB = 600
MAX_IMG_SIDE = 1280

CAMPUSES = [
    "Luis Cabrera",
    "Queretaro",
    "Insurgentes",
    "Frontera",
    "San Luis",
    "Orizaba",
    "Medellin",
]

ASSETS = [
    ("Laptop", 1),
    ("Pantalla", 2),
    ("Control remoto", 3),
    ("Cable HDMI", 4),
    ("Pedestal", 5),
    ("Conector HDMI en el pedestal", 6),
    ("Baterias del control remoto", 7),
    ("Microfono USB", 8),
]

ROOMS_BY_CAMPUS = {
    "Luis Cabrera": [f"Salon {i:02d}" for i in range(1, 25)] + ["Lab PA", "Lab PB"],
    "Insurgentes": [f"Salon {i:02d}" for i in range(1, 24)] + ["Lab Computo"],
    "Orizaba": [f"Salon {i:02d}" for i in range(1, 9)] + ["Lab Microbiologia", "Lab Nutricion", "Lab Alimentos"],
    "Queretaro": [f"Salon {i:02d}" for i in range(1, 15)] + ["Sala Juicios Orales 1", "Sala Juicios Orales 2"],
    "San Luis": (
        [f"Salon {i:02d}" for i in range(1, 19)]
        + ["MAC 1", "MAC 2", "MAC 3", "MAC 4"]
        + [f"Salon Dibujo {i}" for i in range(1, 6)]
        + ["Lab Ciencias", "Foro TV", "Salon Negro", "Serigrafia"]
    ),
    "Frontera": (
        [f"Salon {i:02d}" for i in range(1, 12)]
        + ["Lab Computo"]
        + [f"Taller Costura {i}" for i in range(1, 6)]
        + ["Taller Estampado", "Salon Dibujo"]
    ),
    "Medellin": [],  # manual, pero dejamos "Agregar nuevo" para todos
}


# ============================================================
# SECRETS/ENV
# ============================================================
def get_secret(key: str, default=None):
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default


# ============================================================
# TURSO HTTP CLIENT (robusto contra 10054/Connection Reset)
# ============================================================
class TursoHTTPClient:
    def __init__(self, base_url: str, token: str, timeout: int = 45, retries: int = 6):
        base_url = (base_url or "").strip().rstrip("/")
        if base_url.startswith("libsql://"):
            base_url = "https://" + base_url[len("libsql://"):]
        if not base_url.startswith("http"):
            raise ValueError(f"TURSO_DATABASE_URL inválida: {base_url}")

        self.base_url = base_url
        self.endpoint = f"{base_url}/v2/pipeline"
        self.token = (token or "").strip()
        self.timeout = timeout
        self.retries = retries
        self.session = requests.Session()

    def _arg(self, v):
        if v is None:
            return {"type": "null"}
        if isinstance(v, bool):
            return {"type": "integer", "value": "1" if v else "0"}
        if isinstance(v, int):
            return {"type": "integer", "value": str(v)}
        if isinstance(v, float):
            return {"type": "float", "value": str(v)}
        if isinstance(v, (bytes, bytearray)):
            return {"type": "blob", "base64": base64.b64encode(bytes(v)).decode("ascii")}
        return {"type": "text", "value": str(v)}

    def _parse_cell(self, cell):
        # Turso pipeline devuelve {type,value} o {type,base64}
        if isinstance(cell, dict):
            t = cell.get("type")
            if t == "null":
                return None
            if t == "blob":
                b64 = cell.get("base64")
                if not b64:
                    return None
                try:
                    return base64.b64decode(b64)
                except Exception:
                    return b64
            return cell.get("value")
        return cell

    def execute(self, sql: str, args=None):
        """
        Retorna dict:
          {
            "cols": [...],
            "rows": [...],
            "affected_row_count": int|None,
            "last_insert_rowid": int|None
          }
        """
        args = args or []
        payload = {
            "requests": [
                {"type": "execute", "stmt": {"sql": sql, "args": [self._arg(a) for a in args]}},
                {"type": "close"},
            ]
        }

        # Connection: close = evita que proxy/EDR mate keep-alive (10054)
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Connection": "close",
        }

        last_err = None
        for attempt in range(1, self.retries + 1):
            try:
                r = self.session.post(
                    self.endpoint,
                    json=payload,
                    headers=headers,
                    timeout=self.timeout,
                )
                if r.status_code >= 400:
                    raise RuntimeError(f"Turso HTTP {r.status_code}: {r.text[:600]}")

                data = r.json()
                results = data.get("results", [])
                if not results:
                    return {"cols": [], "rows": [], "affected_row_count": 0, "last_insert_rowid": None}

                res0 = results[0]
                # algunos formatos: {"type":"ok","response":{...}}
                if isinstance(res0, dict) and res0.get("type") == "error":
                    raise RuntimeError(f"Turso error: {res0}")

                resp = res0.get("response") if isinstance(res0, dict) else None
                if resp and resp.get("type") == "error":
                    raise RuntimeError(f"Turso error: {resp}")

                # result puede venir en res0["response"]["result"] o directo en res0["result"]
                result = None
                if isinstance(res0, dict):
                    if resp and isinstance(resp, dict):
                        # ejemplo real: response: { type: execute, result: { cols, rows, ... } }
                        result = resp.get("result") or resp.get("response", {}).get("result")
                    if result is None:
                        result = res0.get("result")

                if result is None:
                    # fallback (por si cambia el shape)
                    return {"cols": [], "rows": [], "affected_row_count": None, "last_insert_rowid": None}

                cols = result.get("cols") or []
                rows_raw = result.get("rows") or []
                rows = [[self._parse_cell(c) for c in row] for row in rows_raw]

                return {
                    "cols": cols,
                    "rows": rows,
                    "affected_row_count": result.get("affected_row_count"),
                    "last_insert_rowid": result.get("last_insert_rowid"),
                }

            except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError) as e:
                last_err = e
                time.sleep(min(1.3 * attempt, 6))
                continue
            except Exception as e:
                last_err = e
                break

        raise last_err


@st.cache_resource
def get_client_cached(url: str, token: str):
    # timeouts/retries “agresivos” para redes inestables
    return TursoHTTPClient(url, token, timeout=45, retries=6)


def get_client():
    # secrets primero; env después
    url = get_secret("TURSO_DATABASE_URL") or os.environ.get("TURSO_DATABASE_URL")
    token = get_secret("TURSO_AUTH_TOKEN") or os.environ.get("TURSO_AUTH_TOKEN")

    if not url or not token:
        st.error("Faltan TURSO_DATABASE_URL / TURSO_AUTH_TOKEN en secrets.toml o variables de entorno.")
        st.stop()

    return get_client_cached(url, token), url


# ============================================================
# DB HELPERS
# ============================================================
def exec_many(client, sql: str):
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        client.execute(stmt)


def fetch_one(client, sql, args=None):
    args = args or []
    res = client.execute(sql, args)
    return res["rows"][0] if res["rows"] else None


def fetch_all(client, sql, args=None):
    args = args or []
    res = client.execute(sql, args)
    return res["rows"]


# ============================================================
# SCHEMA + SEED
# ============================================================
def init_db(client):
    schema = """
    PRAGMA foreign_keys = ON;

    CREATE TABLE IF NOT EXISTS campuses (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS rooms (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      campus_id INTEGER NOT NULL,
      room_code TEXT NOT NULL,
      notes TEXT,
      UNIQUE (campus_id, room_code),
      FOREIGN KEY (campus_id) REFERENCES campuses(id)
    );

    CREATE TABLE IF NOT EXISTS asset_types (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE,
      sort_order INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS inspections (
      id TEXT PRIMARY KEY,
      campus_id INTEGER NOT NULL,
      room_id INTEGER NOT NULL,
      guard_name TEXT NOT NULL,
      inspected_on TEXT NOT NULL, -- YYYY-MM-DD
      inspected_at TEXT,          -- YYYY-MM-DD HH:MM:SS (MX)
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      comments TEXT,
      FOREIGN KEY (campus_id) REFERENCES campuses(id),
      FOREIGN KEY (room_id) REFERENCES rooms(id),
      UNIQUE (room_id, inspected_on)
    );

    CREATE TABLE IF NOT EXISTS inspection_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      inspection_id TEXT NOT NULL,
      asset_type_id INTEGER NOT NULL,
      status TEXT NOT NULL CHECK (
        status IN (
          'OK',
          'FALTA_REPOSICION',
          'NO_FUNCIONA_MANTENIMIENTO',
          'DAÑADO_REPOSICION',
          'DAÑADO_MANTENIMIENTO',
          'N_A'
        )
      ),
      condition TEXT NOT NULL CHECK (condition IN ('BUENO','REGULAR','MALO','N_A')),
      notes TEXT,
      FOREIGN KEY (inspection_id) REFERENCES inspections(id) ON DELETE CASCADE,
      FOREIGN KEY (asset_type_id) REFERENCES asset_types(id),
      UNIQUE (inspection_id, asset_type_id)
    );

    CREATE TABLE IF NOT EXISTS inspection_item_photos (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      inspection_item_id INTEGER NOT NULL UNIQUE,
      mime_type TEXT NOT NULL,
      file_name TEXT,
      image_blob BLOB NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (inspection_item_id) REFERENCES inspection_items(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS app_settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_inspections_room_date ON inspections(room_id, inspected_on);
    CREATE INDEX IF NOT EXISTS idx_inspections_campus_date ON inspections(campus_id, inspected_on);
    CREATE INDEX IF NOT EXISTS idx_items_asset_status ON inspection_items(asset_type_id, status);
    """
    exec_many(client, schema)

    # migración suave de posibles valores viejos
    try:
        client.execute("UPDATE inspection_items SET status='DAÑADO_REPOSICION' WHERE status='DANADO_REPOSICION'")
        client.execute("UPDATE inspection_items SET status='DAÑADO_MANTENIMIENTO' WHERE status='DANADO_MANTENIMIENTO'")
    except Exception:
        pass


def seed_data(client):
    for c in CAMPUSES:
        client.execute("INSERT OR IGNORE INTO campuses(name) VALUES (?)", [c])

    for name, order in ASSETS:
        client.execute(
            "INSERT OR IGNORE INTO asset_types(name, sort_order) VALUES (?, ?)",
            [name, order],
        )

    for campus_name, rooms in ROOMS_BY_CAMPUS.items():
        row = fetch_one(client, "SELECT id FROM campuses WHERE name = ?", [campus_name])
        if not row:
            continue
        campus_id = int(row[0])
        for room_code in rooms:
            client.execute(
                "INSERT OR IGNORE INTO rooms(campus_id, room_code) VALUES (?, ?)",
                [campus_id, room_code],
            )


def settings_get(client, key: str):
    row = fetch_one(client, "SELECT value FROM app_settings WHERE key = ?", [key])
    return row[0] if row else None


def settings_set(client, key: str, value: str):
    client.execute(
        """
        INSERT INTO app_settings(key, value, updated_at)
        VALUES(?, ?, datetime('now'))
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')
        """,
        [key, value],
    )


def ensure_db_setup(client):
    if not st.session_state.get("_schema_ok", False):
        init_db(client)
        st.session_state["_schema_ok"] = True

    done = settings_get(client, "db_setup_done")
    if done == "1":
        return
    seed_data(client)
    settings_set(client, "db_setup_done", "1")


# ============================================================
# IMAGE
# ============================================================
def compress_image(uploaded_file) -> tuple[bytes, str, str]:
    data = uploaded_file.getvalue()
    size_mb = len(data) / (1024 * 1024)
    if size_mb > MAX_UPLOAD_MB:
        raise ValueError(f"Archivo muy grande: {size_mb:.2f} MB. Máximo: {MAX_UPLOAD_MB} MB.")

    img = Image.open(io.BytesIO(data)).convert("RGB")
    w, h = img.size
    scale = min(MAX_IMG_SIDE / max(w, h), 1.0)
    if scale < 1.0:
        img = img.resize((int(w * scale), int(h * scale)))

    mime_type = "image/jpeg"
    file_name = uploaded_file.name or "foto.jpg"

    target_bytes = MAX_STORED_KB * 1024
    best = None
    for q in [85, 80, 75, 70, 65, 60, 55]:
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=q, optimize=True)
        b = buf.getvalue()
        best = b
        if len(b) <= target_bytes:
            break

    return best, mime_type, file_name


# ============================================================
# ADMIN (bcrypt + pista)
# ============================================================
def admin_is_configured(client) -> bool:
    return (settings_get(client, "admin_user") is not None) and (settings_get(client, "admin_pass_hash") is not None)


def admin_password_hint(client) -> str:
    return settings_get(client, "admin_hint") or ""


def admin_check_login(client, user: str, password: str) -> bool:
    stored_user = settings_get(client, "admin_user")
    stored_hash = settings_get(client, "admin_pass_hash")
    if not stored_user or not stored_hash:
        return False
    if user != stored_user:
        return False
    try:
        return bcrypt.checkpw(password.encode("utf-8"), stored_hash.encode("utf-8"))
    except Exception:
        return False


def admin_first_run_setup(client):
    # setup inline (sin st.dialog) para máxima compatibilidad
    if admin_is_configured(client):
        return

    st.warning("🔧 Configuración inicial: crea el usuario/contraseña ADMIN (solo 1 vez).")
    user = st.text_input("Usuario admin", value="admin")
    pw1 = st.text_input("Contraseña", type="password")
    pw2 = st.text_input("Confirmar contraseña", type="password")
    hint = st.text_input("Indicio / pista (opcional)", placeholder="Ej. 'mi mascota + año' (NO pongas la contraseña)")

    if st.button("Guardar admin y continuar", type="primary"):
        if not user.strip():
            st.error("El usuario no puede ir vacío.")
            st.stop()
        if not pw1 or len(pw1) < 6:
            st.error("La contraseña debe tener al menos 6 caracteres.")
            st.stop()
        if pw1 != pw2:
            st.error("Las contraseñas no coinciden.")
            st.stop()

        salt = bcrypt.gensalt(rounds=12)
        pw_hash = bcrypt.hashpw(pw1.encode("utf-8"), salt).decode("utf-8")

        settings_set(client, "admin_user", user.strip())
        settings_set(client, "admin_pass_hash", pw_hash)
        settings_set(client, "admin_hint", (hint or "").strip())

        st.success("Admin configurado. Ahora inicia sesión desde la barra lateral.")
        st.rerun()

    st.caption("La contraseña se guarda encriptada (bcrypt). La pista NO debe contener la contraseña.")
    st.stop()


def is_admin():
    return st.session_state.get("is_admin", False)


def admin_login_sidebar(client):
    with st.sidebar:
        st.markdown("## 🔐 Admin")
        if is_admin():
            st.success("Sesión admin activa")
            if st.button("Cerrar sesión"):
                st.session_state["is_admin"] = False
        else:
            user_default = settings_get(client, "admin_user") or "admin"
            user = st.text_input("Usuario", value=user_default)
            pw = st.text_input("Contraseña", type="password")

            hint = admin_password_hint(client)
            if hint:
                st.caption(f"Indicio: {hint}")

            if st.button("Entrar"):
                if admin_check_login(client, user.strip(), pw):
                    st.session_state["is_admin"] = True
                    st.success("Bienvenido, admin.")
                else:
                    st.error("Usuario o contraseña incorrectos.")


# ============================================================
# CACHES
# ============================================================
@st.cache_data(ttl=300)
def cached_campus_id(campus_name: str):
    row = fetch_one(CLIENT, "SELECT id FROM campuses WHERE name = ?", [campus_name])
    return int(row[0])


@st.cache_data(ttl=300)
def cached_assets():
    return fetch_all(CLIENT, "SELECT id, name FROM asset_types ORDER BY sort_order, name")


@st.cache_data(ttl=120)
def cached_rooms(campus_id: int):
    return fetch_all(CLIENT, "SELECT id, room_code FROM rooms WHERE campus_id = ? ORDER BY room_code", [campus_id])


# ============================================================
# APP START (anti pantalla negra + mensajes claros)
# ============================================================
st.title("Checklist diario de activos - UDL")

boot = st.empty()
boot.info("Iniciando… conectando a Turso (HTTP /v2/pipeline).")

with st.spinner("Conectando a Turso…"):
    CLIENT, DB_URL = get_client()
    try:
        CLIENT.execute("SELECT 1")
    except Exception as e:
        boot.error(f"No puedo conectar con Turso por HTTP (/v2/pipeline).\n\nDetalle: {e}")
        st.stop()

boot.success("Conectado ✅")
boot.empty()

ensure_db_setup(CLIENT)

# Admin
admin_first_run_setup(CLIENT)
admin_login_sidebar(CLIENT)

tab_new, tab_query = st.tabs(["📝 Nueva revisión", "🔎 Consultas",])


# ============================================================
# TAB: NUEVA REVISION
# ============================================================
with tab_new:
    st.subheader("Nueva revisión diaria (1 por salón por día)")
    st.caption("La fecha es automática (hora México CDMX).")

    c1, c2, c3 = st.columns(3)
    with c1:
        campus = st.selectbox("Plantel", CAMPUSES, index=0, key="campus_sel")

    campus_id = cached_campus_id(campus)

    with c2:
        rooms = cached_rooms(campus_id)
        room_map = {r[1]: int(r[0]) for r in rooms}

        st.caption("Selecciona un salón existente o agrega uno nuevo.")
        options = ["(Agregar nuevo...)"] + list(room_map.keys())
        choice = st.selectbox("Salón / Área", options, key="room_sel")

        room_id = None
        room_code = None

        if choice == "(Agregar nuevo...)":
            new_room = st.text_input(
                "Nombre del salón/área (nuevo)",
                key="new_room",
                placeholder="Ej. Salon Morado / Aula Azul / Lab X",
            )
            new_room = " ".join((new_room or "").strip().split())
            if new_room:
                CLIENT.execute(
                    "INSERT OR IGNORE INTO rooms(campus_id, room_code) VALUES (?, ?)",
                    [campus_id, new_room],
                )
                cached_rooms.clear()
                row = fetch_one(
                    CLIENT,
                    "SELECT id FROM rooms WHERE campus_id = ? AND room_code = ?",
                    [campus_id, new_room],
                )
                room_id = int(row[0]) if row else None
                room_code = new_room
        else:
            room_code = choice
            room_id = room_map.get(choice)

    with c3:
        today_mx = datetime.now(TZ_MX).date()
        inspected_on = today_mx
        st.text_input("Fecha (automática)", value=inspected_on.strftime("%Y-%m-%d"), disabled=True)

    guard_name = st.text_input("Nombre del vigilante (obligatorio)", key="guard_name", placeholder="Ej. Juan Pérez")
    comments = st.text_area("Comentarios generales (opcional)", key="comments")

    asset_rows = cached_assets()
    st.markdown("### Checklist (📷 Tomar/Subir foto **solo** si está **DAÑADO**)")

    items_payload = []
    damaged_missing_photo = []

    for asset_id, asset_name in asset_rows:
        asset_id = int(asset_id)
        with st.container(border=True):
            st.markdown(f"**{asset_name}**")

            a1, a2, a3 = st.columns([1, 1, 2])

            with a1:
                status = st.selectbox("Estatus/Acción", STATUS_OPTIONS, key=f"status_{asset_id}")

            with a2:
                cond_key = f"cond_{asset_id}"
                if cond_key not in st.session_state:
                    st.session_state[cond_key] = "BUENO"

                if status == "N_A":
                    st.session_state[cond_key] = "N_A"
                    cond = st.selectbox("Condición", COND_OPTIONS, key=cond_key, disabled=True)
                    cond = "N_A"
                else:
                    if st.session_state.get(cond_key) == "N_A":
                        st.session_state[cond_key] = "BUENO"
                    cond = st.selectbox("Condición", COND_OPTIONS, key=cond_key)

            with a3:
                note = st.text_input("Notas", key=f"note_{asset_id}", placeholder="Opcional")

            photo = None
            if status.startswith("DAÑADO"):
                st.markdown("📷 **Tomar/Subir foto (obligatorio si está DAÑADO)**")
                photo = st.file_uploader(
                    f"Tomar/Subir foto - {asset_name}",
                    type=["jpg", "jpeg", "png"],
                    accept_multiple_files=False,
                    key=f"photo_{asset_id}",
                    help="En celular suele aparecer Cámara/Galería según el navegador.",
                )
                if photo is None:
                    damaged_missing_photo.append(asset_name)

        items_payload.append((asset_id, asset_name, status, cond, note, photo))

    save_disabled = (not room_id) or (not (guard_name or "").strip())
    submitted = st.button("Guardar revisión", type="primary", disabled=save_disabled)

    if submitted:
        if damaged_missing_photo:
            st.error("Falta foto en activos marcados como **DAÑADO**: " + ", ".join(damaged_missing_photo))
            st.stop()

        inspected_on_str = inspected_on.strftime("%Y-%m-%d")
        inspected_at_str = datetime.now(TZ_MX).strftime("%Y-%m-%d %H:%M:%S")

        inspection_id = str(uuid.uuid4())
        try:
            CLIENT.execute(
                """
                INSERT INTO inspections(id, campus_id, room_id, guard_name, inspected_on, inspected_at, comments)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    inspection_id,
                    campus_id,
                    room_id,
                    guard_name.strip(),
                    inspected_on_str,
                    inspected_at_str,
                    (comments or "").strip() or None,
                ],
            )
        except Exception:
            st.error(
                f"⚠️ Ya existe una revisión para **{campus} / {room_code}** "
                f"en la fecha **{inspected_on_str}**.\n\n"
                "No se puede guardar dos veces el mismo salón el mismo día."
            )
            st.stop()

        for asset_type_id, asset_name, status, cond, note, photo in items_payload:
            CLIENT.execute(
                """
                INSERT INTO inspection_items(inspection_id, asset_type_id, status, condition, notes)
                VALUES (?, ?, ?, ?, ?)
                """,
                [inspection_id, asset_type_id, status, cond, (note or "").strip() or None],
            )

            row = fetch_one(
                CLIENT,
                "SELECT id FROM inspection_items WHERE inspection_id = ? AND asset_type_id = ?",
                [inspection_id, asset_type_id],
            )
            inspection_item_id = int(row[0])

            if photo is not None:
                try:
                    blob, mime_type, file_name = compress_image(photo)
                    CLIENT.execute(
                        """
                        INSERT OR REPLACE INTO inspection_item_photos(inspection_item_id, mime_type, file_name, image_blob)
                        VALUES (?, ?, ?, ?)
                        """,
                        [inspection_item_id, mime_type, file_name, blob],
                    )
                except Exception as e:
                    st.warning(f"No se guardó la foto de '{asset_name}': {e}")

        st.success("✅ Revisión guardada correctamente.")
        cached_rooms.clear()


# ============================================================
# TAB: CONSULTAS
# ============================================================
with tab_query:
    st.subheader("Consultar revisiones (lectura)")

    q1, q2, q3 = st.columns(3)
    with q1:
        campus_q = st.selectbox("Plantel", ["(Todos)"] + CAMPUSES, index=0, key="campus_q")
    with q2:
        from_d = st.date_input("Desde", value=date.today().replace(day=1), key="from_d")
    with q3:
        to_d = st.date_input("Hasta", value=date.today(), key="to_d")

    where = []
    args = []

    if campus_q != "(Todos)":
        campus_id_q = cached_campus_id(campus_q)
        where.append("i.campus_id = ?")
        args.append(campus_id_q)

    where.append("i.inspected_on BETWEEN ? AND ?")
    args.extend([from_d.strftime("%Y-%m-%d"), to_d.strftime("%Y-%m-%d")])

    sql = f"""
    SELECT i.inspected_on, c.name, r.room_code, i.guard_name, i.comments, i.id
    FROM inspections i
    JOIN campuses c ON c.id = i.campus_id
    JOIN rooms r ON r.id = i.room_id
    WHERE {" AND ".join(where)}
    ORDER BY i.inspected_on DESC, c.name, r.room_code
    LIMIT 300
    """
    rows = fetch_all(CLIENT, sql, args)
    st.write(f"Resultados: **{len(rows)}**")

    for inspected_on, campus_name, room_code, guard_name, comments, ins_id in rows[:60]:
        with st.expander(f"{inspected_on} | {campus_name} | {room_code} | {guard_name}"):
            items = fetch_all(
                CLIENT,
                """
                SELECT it.id, a.name, it.status, it.condition, it.notes
                FROM inspection_items it
                JOIN asset_types a ON a.id = it.asset_type_id
                WHERE it.inspection_id = ?
                ORDER BY a.sort_order, a.name
                """,
                [ins_id],
            )
            for item_id, a_name, stt, cond, note in items:
                st.write(f"- **{a_name}**: {stt} / {cond}" + (f" — {note}" if note else ""))

                ph = fetch_one(
                    CLIENT,
                    "SELECT image_blob FROM inspection_item_photos WHERE inspection_item_id = ?",
                    [int(item_id)],
                )
                if ph and ph[0]:
                    blob = ph[0]
                    if isinstance(blob, str):
                        try:
                            blob = base64.b64decode(blob)
                        except Exception:
                            pass
                    if isinstance(blob, (bytes, bytearray)):
                        st.image(bytes(blob), caption=f"Foto: {a_name}", use_container_width=True)

            if comments:
                st.info(f"Comentarios: {comments}")


# ============================================================
# ADMIN PANEL
# ============================================================
if is_admin():
    st.divider()
    st.header("🛠️ Panel Admin")

    t1, t2 = st.tabs(["🗑️ Eliminar revisión", "📊 Reportes & Excel"])

    with t1:
        st.subheader("Eliminar una revisión completa (incluye activos y fotos)")

        e1, e2 = st.columns(2)
        with e1:
            campus_e = st.selectbox("Plantel", CAMPUSES, key="campus_e")
        with e2:
            day_e = st.date_input("Fecha (día)", value=date.today(), key="day_e")

        campus_id_e = cached_campus_id(campus_e)
        ins_list = fetch_all(
            CLIENT,
            """
            SELECT i.id, r.room_code, i.guard_name, i.inspected_at
            FROM inspections i
            JOIN rooms r ON r.id = i.room_id
            WHERE i.campus_id = ? AND i.inspected_on = ?
            ORDER BY r.room_code
            """,
            [campus_id_e, day_e.strftime("%Y-%m-%d")],
        )

        if not ins_list:
            st.info("No hay revisiones para ese plantel y día.")
        else:
            options = [f"{room} | {guard} | {ts or ''} | {ins_id}" for ins_id, room, guard, ts in ins_list]
            pick = st.selectbox("Selecciona una revisión", options, key="pick_ins")
            picked_id = pick.split("|")[-1].strip()

            st.warning("⚠️ Esto borra TODO. No se puede deshacer.")
            confirm = st.checkbox("Confirmo que quiero eliminarla definitivamente", key="confirm_delete")
            if st.button("Eliminar revisión", type="primary", disabled=not confirm):
                CLIENT.execute("DELETE FROM inspections WHERE id = ?", [picked_id])
                st.success("✅ Revisión eliminada.")

    with t2:
        st.subheader("Incidencias (status ≠ OK / N_A) y exportación a Excel")

        r1, r2, r3 = st.columns(3)
        with r1:
            campus_r = st.selectbox("Plantel", ["(Todos)"] + CAMPUSES, key="campus_r")
        with r2:
            from_r = st.date_input("Desde", value=date.today().replace(day=1), key="from_r")
        with r3:
            to_r = st.date_input("Hasta", value=date.today(), key="to_r")

        args = [from_r.strftime("%Y-%m-%d"), to_r.strftime("%Y-%m-%d")]
        campus_filter_sql = ""
        if campus_r != "(Todos)":
            campus_id_r = cached_campus_id(campus_r)
            campus_filter_sql = "AND i.campus_id = ?"
            args.append(campus_id_r)

        inc = fetch_all(
            CLIENT,
            f"""
            SELECT
              i.inspected_on AS fecha,
              c.name AS plantel,
              r.room_code AS salon,
              i.guard_name AS vigilante,
              a.name AS activo,
              it.status AS status_accion,
              it.condition AS condicion,
              it.notes AS notas,
              CASE WHEN p.id IS NULL THEN 0 ELSE 1 END AS foto
            FROM inspections i
            JOIN campuses c ON c.id = i.campus_id
            JOIN rooms r ON r.id = i.room_id
            JOIN inspection_items it ON it.inspection_id = i.id
            JOIN asset_types a ON a.id = it.asset_type_id
            LEFT JOIN inspection_item_photos p ON p.inspection_item_id = it.id
            WHERE i.inspected_on BETWEEN ? AND ?
              {campus_filter_sql}
              AND it.status NOT IN ('OK','N_A')
            ORDER BY i.inspected_on DESC, c.name, r.room_code, a.sort_order
            """,
            args,
        )

        st.write(f"Total incidencias: **{len(inc)}**")
        if inc:
            df = pd.DataFrame(
                inc,
                columns=["Fecha", "Plantel", "Salon", "Vigilante", "Activo", "Status/Accion", "Condicion", "Notas", "Foto"],
            )
            st.dataframe(df, use_container_width=True, height=350)

            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Incidencias")

            st.download_button(
                "⬇️ Descargar Excel (Incidencias)",
                data=buf.getvalue(),
                file_name=f"incidencias_{from_r.strftime('%Y%m%d')}_{to_r.strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            st.info("Sin incidencias en ese rango.")
