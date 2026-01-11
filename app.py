import os
import uuid
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import streamlit as st
import pandas as pd
import bcrypt
import requests


# =========================
# CONFIG
# =========================
st.set_page_config(page_title="Checklist diario equipo HyFlex - UDLondres", layout="wide")
TZ_MX = ZoneInfo("America/Mexico_City")

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
    "Medellin": [],
}

STATUS_OPTIONS = [
    "OK",
    "FALTA_REPOSICION",
    "NO_FUNCIONA_MANTENIMIENTO",
    "DAÑADO_REPOSICION",
    "DAÑADO_MANTENIMIENTO",
    "N_A",
]
COND_OPTIONS = ["BUENO", "REGULAR", "MALO", "N_A"]


# =========================
# SECRETS/ENV
# =========================
def get_secret(key: str, default=None):
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default


# =========================
# TURSO HTTP CLIENT
# =========================
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
        return {"type": "text", "value": str(v)}

    def _parse_cell(self, cell):
        if isinstance(cell, dict):
            t = cell.get("type")
            if t == "null":
                return None
            return cell.get("value")
        return cell

    def execute(self, sql: str, args=None):
        args = args or []
        payload = {
            "requests": [
                {"type": "execute", "stmt": {"sql": sql, "args": [self._arg(a) for a in args]}},
                {"type": "close"},
            ]
        }

        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Connection": "close",
        }

        last_err = None
        for attempt in range(1, self.retries + 1):
            try:
                r = self.session.post(self.endpoint, json=payload, headers=headers, timeout=self.timeout)
                if r.status_code >= 400:
                    raise RuntimeError(f"Turso HTTP {r.status_code}: {r.text[:600]}")

                data = r.json()
                results = data.get("results", [])
                if not results:
                    return {"cols": [], "rows": []}

                res0 = results[0]
                if isinstance(res0, dict) and res0.get("type") == "error":
                    raise RuntimeError(f"Turso error: {res0}")

                resp = res0.get("response") if isinstance(res0, dict) else None
                if resp and resp.get("type") == "error":
                    raise RuntimeError(f"Turso error: {resp}")

                result = None
                if isinstance(res0, dict):
                    if resp and isinstance(resp, dict):
                        result = resp.get("result")
                    if result is None:
                        result = res0.get("result")

                if result is None:
                    return {"cols": [], "rows": []}

                cols = result.get("cols") or []
                rows_raw = result.get("rows") or []
                rows = [[self._parse_cell(c) for c in row] for row in rows_raw]
                return {"cols": cols, "rows": rows}

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
    return TursoHTTPClient(url, token, timeout=45, retries=6)


def get_client():
    url = get_secret("TURSO_DATABASE_URL") or os.environ.get("TURSO_DATABASE_URL")
    token = get_secret("TURSO_AUTH_TOKEN") or os.environ.get("TURSO_AUTH_TOKEN")
    if not url or not token:
        st.error("Faltan TURSO_DATABASE_URL / TURSO_AUTH_TOKEN en secrets.toml.")
        st.stop()
    return get_client_cached(url, token), url


# =========================
# DB HELPERS
# =========================
def exec_many(client, sql: str):
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        client.execute(stmt, [])


def fetch_one(client, sql, args=None):
    res = client.execute(sql, args or [])
    return res["rows"][0] if res["rows"] else None


def fetch_all(client, sql, args=None):
    res = client.execute(sql, args or [])
    return res["rows"]


# =========================
# SCHEMA
# =========================
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
      inspected_on TEXT NOT NULL,
      inspected_at TEXT,
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
          'DANADO_REPOSICION',
          'DANADO_MANTENIMIENTO',
          'N_A'
        )
      ),
      condition TEXT NOT NULL CHECK (condition IN ('BUENO','REGULAR','MALO','N_A')),
      notes TEXT,
      FOREIGN KEY (inspection_id) REFERENCES inspections(id) ON DELETE CASCADE,
      FOREIGN KEY (asset_type_id) REFERENCES asset_types(id),
      UNIQUE (inspection_id, asset_type_id)
    );

    CREATE TABLE IF NOT EXISTS app_settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT NOT NULL UNIQUE,
      pass_hash TEXT NOT NULL,
      is_admin INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_inspections_room_date ON inspections(room_id, inspected_on);
    CREATE INDEX IF NOT EXISTS idx_inspections_campus_date ON inspections(campus_id, inspected_on);
    CREATE INDEX IF NOT EXISTS idx_items_asset_status ON inspection_items(asset_type_id, status);
    CREATE INDEX IF NOT EXISTS idx_users_admin ON users(is_admin);
    """
    exec_many(client, schema)


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

    if settings_get(client, "seed_done_v3") != "1":
        for c in CAMPUSES:
            client.execute("INSERT OR IGNORE INTO campuses(name) VALUES (?)", [c])

        for name, order in ASSETS:
            client.execute("INSERT OR IGNORE INTO asset_types(name, sort_order) VALUES (?, ?)", [name, order])

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

        settings_set(client, "seed_done_v3", "1")


# =========================
# USERS
# =========================
def user_count(client) -> int:
    row = fetch_one(client, "SELECT COUNT(*) FROM users", [])
    return int(row[0]) if row else 0


def user_get_by_username(client, username: str):
    return fetch_one(client, "SELECT id, username, pass_hash, is_admin FROM users WHERE username = ?", [username])


def user_create(client, username: str, password: str, is_admin: bool):
    username = (username or "").strip()
    if not username:
        raise ValueError("Usuario vacío.")
    if not password or len(password) < 6:
        raise ValueError("Contraseña mínima 6 caracteres.")
    salt = bcrypt.gensalt(rounds=12)
    pw_hash = bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")
    client.execute(
        "INSERT INTO users(username, pass_hash, is_admin, updated_at) VALUES(?, ?, ?, datetime('now'))",
        [username, pw_hash, 1 if is_admin else 0],
    )


def user_set_admin(client, user_id: int, is_admin: bool):
    client.execute(
        "UPDATE users SET is_admin = ?, updated_at=datetime('now') WHERE id = ?",
        [1 if is_admin else 0, int(user_id)],
    )


def user_check_login(client, username: str, password: str) -> bool:
    row = user_get_by_username(client, (username or "").strip())
    if not row:
        return False
    _, _, stored_hash, _ = row
    try:
        return bcrypt.checkpw(password.encode("utf-8"), stored_hash.encode("utf-8"))
    except Exception:
        return False


def user_is_admin(client, username: str) -> bool:
    row = user_get_by_username(client, (username or "").strip())
    if not row:
        return False
    return int(row[3]) == 1


def users_list(client):
    return fetch_all(client, "SELECT id, username, is_admin, created_at FROM users ORDER BY is_admin DESC, username", [])


def user_delete(client, user_id: int):
    client.execute("DELETE FROM users WHERE id = ?", [int(user_id)])


def user_update_password(client, username: str, new_password: str):
    if not new_password or len(new_password) < 6:
        raise ValueError("La contraseña debe tener al menos 6 caracteres.")
    salt = bcrypt.gensalt(rounds=12)
    pw_hash = bcrypt.hashpw(new_password.encode("utf-8"), salt).decode("utf-8")
    client.execute(
        "UPDATE users SET pass_hash=?, updated_at=datetime('now') WHERE username=?",
        [pw_hash, username.strip()],
    )


def user_update_password_by_id(client, user_id: int, new_password: str):
    if not new_password or len(new_password) < 6:
        raise ValueError("La contraseña debe tener al menos 6 caracteres.")
    salt = bcrypt.gensalt(rounds=12)
    pw_hash = bcrypt.hashpw(new_password.encode("utf-8"), salt).decode("utf-8")
    client.execute(
        "UPDATE users SET pass_hash=?, updated_at=datetime('now') WHERE id=?",
        [pw_hash, int(user_id)],
    )


def migrate_legacy_admin_to_users(client):
    if settings_get(client, "users_migrated_v1") == "1":
        return

    if user_count(client) > 0:
        settings_set(client, "users_migrated_v1", "1")
        return

    legacy_user = settings_get(client, "admin_user")
    legacy_hash = settings_get(client, "admin_pass_hash")

    if legacy_user and legacy_hash:
        client.execute(
            "INSERT OR IGNORE INTO users(username, pass_hash, is_admin, updated_at) VALUES(?, ?, 1, datetime('now'))",
            [legacy_user.strip(), legacy_hash],
        )
        settings_set(client, "users_migrated_v1", "1")


def admin_password_hint(client) -> str:
    return settings_get(client, "admin_hint") or ""


def admin_first_run_setup(client):
    if user_count(client) > 0:
        return

    st.warning("🔧 Configuración inicial: crea el usuario/contraseña ADMIN (solo 1 vez).")
    user = st.text_input("Usuario admin", value="admin")
    pw1 = st.text_input("Contraseña", type="password")
    pw2 = st.text_input("Confirmar contraseña", type="password")
    hint = st.text_input("Indicio / pista (opcional)", placeholder="NO pongas la contraseña")

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

        user_create(client, user.strip(), pw1, is_admin=True)
        settings_set(client, "admin_hint", (hint or "").strip())
        st.success("Admin configurado. Ahora inicia sesión desde la barra lateral.")
        st.rerun()

    st.stop()


def is_admin():
    return st.session_state.get("is_admin", False)


def is_logged():
    return bool(st.session_state.get("logged_user"))


def admin_login_sidebar(client):
    with st.sidebar:
        st.markdown("## 🔐 Consultas (Login)")
        if st.session_state.get("logged_user"):
            st.success(f"Sesión: {st.session_state['logged_user']}")
            st.caption("Perfil: Administrador" if is_admin() else "Perfil: Usuario")
            if st.button("Cerrar sesión"):
                st.session_state["logged_user"] = None
                st.session_state["is_admin"] = False
                st.rerun()
        else:
            hint = admin_password_hint(client)
            user = st.text_input("Usuario", value="")
            pw = st.text_input("Contraseña", type="password")
            if hint:
                st.caption(f"Indicio: {hint}")

            if st.button("Entrar"):
                if user_check_login(client, user.strip(), pw):
                    st.session_state["logged_user"] = user.strip()
                    st.session_state["is_admin"] = user_is_admin(client, user.strip())
                    st.success("Sesión iniciada.")
                    st.rerun()
                else:
                    st.error("Usuario o contraseña incorrectos.")


# =========================
# CACHES (DB)
# =========================
@st.cache_data(ttl=900)
def cached_campus_id(campus_name: str):
    row = fetch_one(CLIENT, "SELECT id FROM campuses WHERE name = ?", [campus_name])
    return int(row[0])


@st.cache_data(ttl=900)
def cached_assets():
    return fetch_all(
        CLIENT,
        """
        SELECT MIN(id) as id, name
        FROM asset_types
        GROUP BY name
        ORDER BY MIN(sort_order), name
        """,
        [],
    )


@st.cache_data(ttl=900)
def cached_rooms(campus_id: int):
    return fetch_all(CLIENT, "SELECT id, room_code FROM rooms WHERE campus_id=? ORDER BY room_code", [campus_id])


def get_rooms_for_campus(campus_id: int):
    key = f"rooms_{campus_id}"
    if key not in st.session_state:
        st.session_state[key] = cached_rooms(campus_id)
    return st.session_state[key]


def invalidate_rooms_cache(campus_id: int):
    key = f"rooms_{campus_id}"
    if key in st.session_state:
        del st.session_state[key]
    cached_rooms.clear()


# =========================
# FORM RESET
# =========================
def new_form_nonce():
    st.session_state["form_nonce"] = str(uuid.uuid4())


def current_nonce() -> str:
    if "form_nonce" not in st.session_state:
        new_form_nonce()
    return st.session_state["form_nonce"]


def track_campus_change(campus_value: str):
    prev = st.session_state.get("campus_prev")
    if prev != campus_value:
        st.session_state["campus_prev"] = campus_value
        st.session_state["room_choice"] = "(Selecciona...)"
        st.session_state["new_room_code"] = ""
        new_form_nonce()


@st.dialog("✅ Registro enviado")
def registro_enviado_dialog(resumen: str):
    st.success("Tu registro fue enviado correctamente.")
    st.write(resumen)
    if st.button("Aceptar / Nuevo registro", type="primary"):
        st.session_state["room_choice"] = "(Selecciona...)"
        st.session_state["new_room_code"] = ""
        new_form_nonce()
        st.session_state["show_sent_dialog"] = False
        st.rerun()


# =========================
# EDITAR / ELIMINAR REVISIONES
# =========================
def get_inspection_header(ins_id: str):
    return fetch_one(
        CLIENT,
        """
        SELECT i.id, i.inspected_on, c.name, r.room_code, i.guard_name, i.inspected_at, i.comments
        FROM inspections i
        JOIN campuses c ON c.id = i.campus_id
        JOIN rooms r ON r.id = i.room_id
        WHERE i.id = ?
        """,
        [ins_id],
    )


def get_inspection_items(ins_id: str):
    return fetch_all(
        CLIENT,
        """
        SELECT a.id as asset_type_id, a.name, it.status, it.condition, COALESCE(it.notes,'')
        FROM inspection_items it
        JOIN asset_types a ON a.id = it.asset_type_id
        WHERE it.inspection_id = ?
        ORDER BY a.sort_order, a.name
        """,
        [ins_id],
    )


@st.dialog("✏️ Editar / Eliminar revisión")
def edit_inspection_dialog(ins_id: str):
    hdr = get_inspection_header(ins_id)
    if not hdr:
        st.error("No encontré la revisión.")
        return

    _id, inspected_on, campus_name, room_code, guard_name, inspected_at, comments = hdr
    st.caption(f"{inspected_on} | {campus_name} | {room_code}")

    guard_new = st.text_input("Vigilante", value=guard_name or "")
    comments_new = st.text_area("Comentarios generales", value=comments or "")

    st.markdown("### Items")
    items = get_inspection_items(ins_id)

    edited = []
    for asset_type_id, asset_name, stt, cond, note in items:
        with st.container(border=True):
            st.markdown(f"**{asset_name}**")

            k_status = f"edit_st_{ins_id}_{asset_type_id}"
            k_cond = f"edit_cd_{ins_id}_{asset_type_id}"
            k_note = f"edit_nt_{ins_id}_{asset_type_id}"

            status_val = st.selectbox(
                "Estatus/Acción",
                STATUS_OPTIONS,
                index=STATUS_OPTIONS.index(stt) if stt in STATUS_OPTIONS else 0,
                key=k_status,
            )

            if status_val == "N_A":
                st.selectbox("Condición", ["N_A"], index=0, disabled=True, key=k_cond)
                cond_val = "N_A"
            else:
                opts = ["BUENO", "REGULAR", "MALO"]
                default_cond = cond if cond in opts else "BUENO"
                cond_val = st.selectbox(
                    "Condición",
                    opts,
                    index=opts.index(default_cond),
                    key=k_cond,
                )

            note_val = st.text_input("Notas", value=note or "", key=k_note)

            if status_val == "N_A":
                cond_val = "N_A"

            edited.append((int(asset_type_id), status_val, cond_val, (note_val or "").strip() or None))

    st.divider()
    c1, c2 = st.columns(2)

    with c1:
        if st.button("💾 Guardar cambios", type="primary"):
            CLIENT.execute(
                "UPDATE inspections SET guard_name=?, comments=?, inspected_at=? WHERE id=?",
                [
                    (guard_new or "").strip() or guard_name,
                    (comments_new or "").strip() or None,
                    datetime.now(TZ_MX).strftime("%Y-%m-%d %H:%M:%S"),
                    ins_id,
                ],
            )

            for asset_type_id, status_val, cond_val, note_val in edited:
                CLIENT.execute(
                    """
                    UPDATE inspection_items
                    SET status=?, condition=?, notes=?
                    WHERE inspection_id=? AND asset_type_id=?
                    """,
                    [status_val, cond_val, note_val, ins_id, asset_type_id],
                )

            st.success("✅ Cambios guardados.")
            st.session_state["open_edit_dialog"] = False
            st.session_state["edit_target"] = None
            st.rerun()

    with c2:
        st.warning("⚠️ Eliminar borra la revisión completa.")
        confirm = st.checkbox("Confirmo eliminar definitivamente", value=False, key=f"del_ok_{ins_id}")
        if st.button("🗑️ Eliminar revisión", disabled=not confirm):
            CLIENT.execute("DELETE FROM inspections WHERE id = ?", [ins_id])
            st.success("✅ Revisión eliminada.")
            st.session_state["open_edit_dialog"] = False
            st.session_state["edit_target"] = None
            st.rerun()


# =========================
# APP START
# =========================
st.title("Checklist diario equipo HyFlex - UDLondres")

boot = st.empty()
boot.info("Conectando a Turso…")
with st.spinner("Conectando…"):
    CLIENT, DB_URL = get_client()
    try:
        CLIENT.execute("SELECT 1", [])
    except Exception as e:
        boot.error(f"No puedo conectar con Turso.\n\nDetalle: {e}")
        st.stop()
boot.empty()

ensure_db_setup(CLIENT)
migrate_legacy_admin_to_users(CLIENT)
admin_first_run_setup(CLIENT)
admin_login_sidebar(CLIENT)

# Tabs: Nueva revisión siempre; Consultas y Gestión solo con login
if is_logged():
    tab_new, tab_query, tab_users = st.tabs(["📝 Nueva revisión", "🔎 Consultas", "👥 Usuarios"])
else:
    (tab_new,) = st.tabs(["📝 Nueva revisión"])


# =========================
# TAB: NUEVA REVISION (abierto)
# =========================
with tab_new:
    st.subheader("Nueva revisión diaria (1 por salón por día)")
    st.caption("✅ Captura abierta para vigilantes sin cuenta. 🔐 Consultas y Usuarios requieren iniciar sesión (CDMX).")

    if st.session_state.get("show_sent_dialog") and st.session_state.get("sent_summary"):
        registro_enviado_dialog(st.session_state["sent_summary"])

    campus = st.selectbox("Plantel", ["(Selecciona...)"] + CAMPUSES, index=0, key="campus_selector")
    if campus != "(Selecciona...)":
        track_campus_change(campus)

    if campus == "(Selecciona...)":
        st.info("Selecciona un plantel para iniciar un registro.")
    else:
        campus_id = cached_campus_id(campus)
        today_mx = datetime.now(TZ_MX).date()
        inspected_on_str = today_mx.strftime("%Y-%m-%d")

        rooms = get_rooms_for_campus(campus_id)
        room_map = {r[1]: int(r[0]) for r in rooms}
        room_options = ["(Selecciona...)"] + list(room_map.keys()) + ["(Agregar nuevo...)"]

        cA, cB, cC = st.columns(3)
        with cA:
            st.text_input("Fecha (automática)", value=inspected_on_str, disabled=True)
        with cB:
            choice = st.selectbox("Salón / Área", room_options, index=0, key="room_choice")
        with cC:
            guard_name_out = st.text_input("Nombre del vigilante (obligatorio)", key="guard_name_out")

        room_id = None
        room_code = None

        if choice == "(Agregar nuevo...)":
            new_room = st.text_input(
                "Escribe el salón/área nuevo",
                key="new_room_code",
                placeholder="Ej. Salon 01 / Laboratorio / Auditorio",
            )
            new_room = " ".join((new_room or "").strip().split())
            if new_room:
                room_code = new_room
        elif choice != "(Selecciona...)":
            room_code = choice
            room_id = room_map.get(choice)

        comments_out = st.text_area("Comentarios generales (opcional)", key="comments_out")

        nonce = current_nonce()

        with st.form(f"new_inspection_form_{nonce}", clear_on_submit=False):
            st.markdown("### Checklist de activos")
            st.caption("Selecciona estatus y condición por cada activo (obligatorio).")

            asset_rows = cached_assets()
            items_payload = []
            missing = []

            for asset_id, asset_name in asset_rows:
                asset_id = int(asset_id)
                with st.container(border=True):
                    st.markdown(f"**{asset_name}**")

                    status_key = f"status_{nonce}_{asset_id}"
                    cond_key = f"cond_{nonce}_{asset_id}"

                    status = st.selectbox(
                        "Estatus/Acción",
                        ["(Selecciona...)"] + STATUS_OPTIONS,
                        index=0,
                        key=status_key,
                    )

                    if status == "N_A":
                        st.selectbox("Condición", ["N_A"], index=0, disabled=True, key=cond_key)
                        cond = "N_A"
                    else:
                        cond = st.selectbox(
                            "Condición",
                            ["(Selecciona...)"] + COND_OPTIONS,
                            index=0,
                            key=cond_key,
                        )

                    note = st.text_input("Notas (opcional)", key=f"note_{nonce}_{asset_id}")
                    items_payload.append((asset_id, asset_name, status, cond, note))

            submitted = st.form_submit_button("Guardar revisión")

        if submitted:
            if not (guard_name_out or "").strip():
                st.error("Falta el nombre del vigilante.")
                st.stop()

            if not room_code:
                st.error("Selecciona un salón/área o agrega uno nuevo.")
                st.stop()

            items_payload = [
                (asset_type_id, asset_name, status, ("N_A" if status == "N_A" else cond), note)
                for (asset_type_id, asset_name, status, cond, note) in items_payload
            ]

            for _, asset_name, status, cond, _ in items_payload:
                if status == "(Selecciona...)":
                    missing.append(asset_name)
                elif status != "N_A" and cond == "(Selecciona...)":
                    missing.append(asset_name)

            if missing:
                st.error("Faltan seleccionar estatus/condición en: " + ", ".join(missing))
                st.stop()

            if choice == "(Agregar nuevo...)":
                CLIENT.execute(
                    "INSERT OR IGNORE INTO rooms(campus_id, room_code) VALUES (?, ?)",
                    [campus_id, room_code],
                )
                invalidate_rooms_cache(campus_id)
                row = fetch_one(
                    CLIENT,
                    "SELECT id FROM rooms WHERE campus_id=? AND room_code=?",
                    [campus_id, room_code],
                )
                room_id = int(row[0]) if row else None

            if not room_id:
                st.error("No se pudo resolver el salón. Intenta de nuevo.")
                st.stop()

            inspection_id = str(uuid.uuid4())
            inspected_at_str = datetime.now(TZ_MX).strftime("%Y-%m-%d %H:%M:%S")

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
                        guard_name_out.strip(),
                        inspected_on_str,
                        inspected_at_str,
                        (comments_out or "").strip() or None,
                    ],
                )
            except Exception:
                st.error(
                    f"⚠️ Ya existe una revisión para **{campus} / {room_code}** en **{inspected_on_str}**.\n\n"
                    "No se puede guardar dos veces el mismo salón el mismo día."
                )
                st.stop()

            for asset_type_id, _, status, cond, note in items_payload:
                CLIENT.execute(
                    """
                    INSERT INTO inspection_items(inspection_id, asset_type_id, status, condition, notes)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        inspection_id,
                        asset_type_id,
                        status,
                        cond,
                        (note or "").strip() or None,
                    ],
                )

            st.session_state["sent_summary"] = (
                f"Plantel: **{campus}**\n\n"
                f"Salón/Área: **{room_code}**\n\n"
                f"Vigilante: **{guard_name_out.strip()}**\n\n"
                f"Fecha: **{inspected_on_str}**"
            )
            st.session_state["show_sent_dialog"] = True
            st.rerun()


# =========================
# TAB: CONSULTAS (solo login)
# =========================
if is_logged():
    with tab_query:
        st.subheader("Consultas (dinámicas y visuales)")

        today_mx = datetime.now(TZ_MX).date()
        preset = st.selectbox(
            "Rango rápido",
            ["Este mes", "Últimos 7 días", "Últimos 30 días", "Hoy", "Personalizado"],
            index=0,
            key="preset_q",
        )

        if preset == "Hoy":
            from_d = today_mx
            to_d = today_mx
        elif preset == "Últimos 7 días":
            from_d = today_mx - timedelta(days=6)
            to_d = today_mx
        elif preset == "Últimos 30 días":
            from_d = today_mx - timedelta(days=29)
            to_d = today_mx
        elif preset == "Este mes":
            from_d = today_mx.replace(day=1)
            to_d = today_mx
        else:
            cA, cB = st.columns(2)
            with cA:
                from_d = st.date_input("Desde", value=today_mx.replace(day=1), key="q_from")
            with cB:
                to_d = st.date_input("Hasta", value=today_mx, key="q_to")

        f1, f2, f3, f4 = st.columns(4)
        with f1:
            campus_q = st.selectbox("Plantel", ["(Todos)"] + CAMPUSES, index=0, key="campus_q")
        with f2:
            guard_q = st.text_input("Filtrar por vigilante (contiene)", value="", key="guard_q")
        with f3:
            only_incidencias = st.checkbox("Solo incidencias (status ≠ OK/N_A)", value=False, key="only_inc_q")
        with f4:
            view_mode = st.selectbox("Vista", ["Resumen", "Detalle por revisión", "Detalle por activo"], index=0, key="view_mode")

        campus_id_q = None
        room_q = "(Todos)"
        if campus_q != "(Todos)":
            campus_id_q = cached_campus_id(campus_q)
            rooms = get_rooms_for_campus(campus_id_q)
            room_codes = [r[1] for r in rooms]
            room_q = st.selectbox("Salón / Área", ["(Todos)"] + room_codes, index=0, key="room_q")

        asset_rows = cached_assets()
        asset_names = [r[1] for r in asset_rows]
        activo_q = st.multiselect(
            "Filtrar activos (opcional)",
            options=["(Todos)"] + asset_names,
            default=["(Todos)"],
            key="activo_q",
        )

        where = ["i.inspected_on BETWEEN ? AND ?"]
        args = [from_d.strftime("%Y-%m-%d"), to_d.strftime("%Y-%m-%d")]

        if campus_id_q is not None:
            where.append("i.campus_id = ?")
            args.append(campus_id_q)

        if room_q != "(Todos)" and campus_id_q is not None:
            where.append("r.room_code = ?")
            args.append(room_q)

        if guard_q.strip():
            where.append("LOWER(i.guard_name) LIKE ?")
            args.append(f"%{guard_q.strip().lower()}%")

        where_sql = " AND ".join(where)

        if view_mode == "Resumen":
            st.markdown("### Resumen")

            rows = fetch_all(
                CLIENT,
                f"""
                SELECT
                  i.inspected_on AS fecha,
                  c.name AS plantel,
                  COUNT(*) AS revisiones
                FROM inspections i
                JOIN campuses c ON c.id = i.campus_id
                JOIN rooms r ON r.id = i.room_id
                WHERE {where_sql}
                GROUP BY i.inspected_on, c.name
                ORDER BY i.inspected_on DESC, c.name
                """,
                args,
            )
            df_rev = pd.DataFrame(rows, columns=["Fecha", "Plantel", "Revisiones"]) if rows else pd.DataFrame(
                columns=["Fecha", "Plantel", "Revisiones"]
            )

            if not df_rev.empty:
                df_rev["Revisiones"] = pd.to_numeric(df_rev["Revisiones"], errors="coerce").fillna(0).astype(int)

            status_where = ""
            if only_incidencias:
                status_where = " AND it.status NOT IN ('OK','N_A')"

            rows2 = fetch_all(
                CLIENT,
                f"""
                SELECT
                  it.status AS status,
                  COUNT(*) AS total
                FROM inspections i
                JOIN inspection_items it ON it.inspection_id = i.id
                WHERE {where_sql}
                  {status_where}
                GROUP BY it.status
                ORDER BY total DESC
                """,
                args,
            )
            df_st = pd.DataFrame(rows2, columns=["Status", "Total"]) if rows2 else pd.DataFrame(columns=["Status", "Total"])
            if not df_st.empty:
                df_st["Total"] = pd.to_numeric(df_st["Total"], errors="coerce").fillna(0).astype(int)

            c1, c2 = st.columns([2, 1])
            with c1:
                st.markdown("#### Revisiones por día/plantel")
                if df_rev.empty:
                    st.info("Sin resultados con los filtros actuales.")
                else:
                    pivot = df_rev.pivot_table(
                        index="Fecha",
                        columns="Plantel",
                        values="Revisiones",
                        aggfunc="sum",
                        fill_value=0,
                    )
                    st.dataframe(df_rev, use_container_width=True, hide_index=True)
                    st.bar_chart(pivot)

            with c2:
                st.markdown("#### Distribución por estatus")
                if df_st.empty:
                    st.info("Sin resultados.")
                else:
                    st.dataframe(df_st, use_container_width=True, hide_index=True)
                    st.bar_chart(df_st.set_index("Status"))

        elif view_mode == "Detalle por revisión":
            st.markdown("### Detalle por revisión (Editar/Eliminar)")

            rows = fetch_all(
                CLIENT,
                f"""
                SELECT
                  i.inspected_on, c.name, r.room_code, i.guard_name, i.inspected_at, i.comments, i.id
                FROM inspections i
                JOIN campuses c ON c.id = i.campus_id
                JOIN rooms r ON r.id = i.room_id
                WHERE {where_sql}
                ORDER BY i.inspected_on DESC, c.name, r.room_code
                LIMIT 300
                """,
                args,
            )

            st.write(f"Resultados: **{len(rows)}**")
            if not rows:
                st.info("Sin resultados con los filtros actuales.")
            else:
                for inspected_on, campus_name, room_code, guard_name, inspected_at, comments, ins_id in rows[:150]:
                    with st.expander(f"{inspected_on} | {campus_name} | {room_code} | {guard_name}"):
                        if inspected_at:
                            st.caption(f"Registrado: {inspected_at}")
                        if comments:
                            st.info(f"Comentarios: {comments}")

                        items = fetch_all(
                            CLIENT,
                            """
                            SELECT a.name, it.status, it.condition, it.notes
                            FROM inspection_items it
                            JOIN asset_types a ON a.id = it.asset_type_id
                            WHERE it.inspection_id = ?
                            ORDER BY a.sort_order, a.name
                            """,
                            [ins_id],
                        )

                        if "(Todos)" not in activo_q:
                            items = [x for x in items if x[0] in activo_q]
                        if only_incidencias:
                            items = [x for x in items if x[1] not in ("OK", "N_A")]

                        if not items:
                            st.caption("Sin items que coincidan con los filtros.")
                        else:
                            for a_name, stt, cond, note in items:
                                st.write(f"- **{a_name}**: {stt} / {cond}" + (f" — {note}" if note else ""))

                        b1, b2 = st.columns(2)
                        with b1:
                            if st.button("✏️ Editar", key=f"btn_edit_{ins_id}"):
                                st.session_state["edit_target"] = ins_id
                                st.session_state["open_edit_dialog"] = True
                                st.rerun()
                        with b2:
                            if st.button("🗑️ Eliminar", key=f"btn_del_{ins_id}"):
                                st.session_state["edit_target"] = ins_id
                                st.session_state["open_edit_dialog"] = True
                                st.rerun()

                if st.session_state.get("open_edit_dialog") and st.session_state.get("edit_target"):
                    edit_inspection_dialog(st.session_state["edit_target"])

        else:
            st.markdown("### Detalle por activo (tabla)")

            status_filter = st.multiselect(
                "Filtrar estatus",
                options=["(Todos)"] + STATUS_OPTIONS,
                default=["(Todos)"],
                key="status_filter",
            )

            extra_sql = ""
            extra_args = list(args)

            if only_incidencias:
                extra_sql += " AND it.status NOT IN ('OK','N_A')"

            if "(Todos)" not in status_filter:
                placeholders = ",".join(["?"] * len(status_filter))
                extra_sql += f" AND it.status IN ({placeholders})"
                extra_args.extend(status_filter)

            if "(Todos)" not in activo_q:
                placeholders = ",".join(["?"] * len(activo_q))
                extra_sql += f" AND a.name IN ({placeholders})"
                extra_args.extend(activo_q)

            rows = fetch_all(
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
                  COALESCE(it.notes,'') AS notas,
                  COALESCE(i.comments,'') AS comentarios,
                  i.id AS inspection_id
                FROM inspections i
                JOIN campuses c ON c.id = i.campus_id
                JOIN rooms r ON r.id = i.room_id
                JOIN inspection_items it ON it.inspection_id = i.id
                JOIN asset_types a ON a.id = it.asset_type_id
                WHERE {where_sql}
                  {extra_sql}
                ORDER BY i.inspected_on DESC, c.name, r.room_code, a.sort_order
                LIMIT 4000
                """,
                extra_args,
            )

            if not rows:
                st.info("Sin resultados con los filtros actuales (activos/status/fechas).")
            else:
                df = pd.DataFrame(
                    rows,
                    columns=[
                        "Fecha", "Plantel", "Salon", "Vigilante", "Activo",
                        "Status/Accion", "Condicion", "Notas", "Comentarios", "InspectionID"
                    ],
                )
                st.dataframe(df, use_container_width=True, height=520)

                st.download_button(
                    "⬇️ Descargar CSV (detalle por activo)",
                    data=df.drop(columns=["InspectionID"]).to_csv(index=False).encode("utf-8"),
                    file_name=f"detalle_activos_{from_d.strftime('%Y%m%d')}_{to_d.strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                )


# =========================
# TAB: USUARIOS (solo login)
# =========================
if is_logged():
    with tab_users:
        st.subheader("👤 Mi cuenta")
        st.caption("Aquí puedes cambiar tu contraseña. (Mínimo 6 caracteres)")

        current_user = st.session_state.get("logged_user") or ""
        st.text_input("Usuario", value=current_user, disabled=True)

        pw_old = st.text_input("Contraseña actual", type="password", key="pw_old")
        pw_new1 = st.text_input("Nueva contraseña", type="password", key="pw_new1")
        pw_new2 = st.text_input("Confirmar nueva contraseña", type="password", key="pw_new2")

        if st.button("Actualizar contraseña", type="primary", key="btn_change_pw"):
            if not pw_old:
                st.error("Escribe tu contraseña actual.")
            elif not user_check_login(CLIENT, current_user, pw_old):
                st.error("Contraseña actual incorrecta.")
            elif pw_new1 != pw_new2:
                st.error("La nueva contraseña no coincide en ambos campos.")
            else:
                try:
                    user_update_password(CLIENT, current_user, pw_new1)
                    st.success("✅ Contraseña actualizada.")
                    st.session_state["pw_old"] = ""
                    st.session_state["pw_new1"] = ""
                    st.session_state["pw_new2"] = ""
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo actualizar: {e}")

        st.divider()
        st.subheader("👥 Gestión de usuarios")
        if not is_admin():
            st.info("Solo el administrador puede gestionar usuarios.")
        else:
            sub1, sub2, sub3 = st.tabs(["➕ Crear", "🧰 Administrar", "🔁 Reset contraseña"])

            with sub1:
                st.markdown("### Crear nuevo usuario")
                u1, u2 = st.columns(2)
                with u1:
                    new_user = st.text_input("Usuario", key="new_user")
                    new_is_admin = st.checkbox("¿Es administrador?", value=False, key="new_is_admin")
                with u2:
                    p1 = st.text_input("Contraseña", type="password", key="new_pw1")
                    p2 = st.text_input("Confirmar contraseña", type="password", key="new_pw2")

                if st.button("Crear usuario", type="primary", key="btn_create_user"):
                    try:
                        if p1 != p2:
                            st.error("Las contraseñas no coinciden.")
                        else:
                            user_create(CLIENT, new_user.strip(), p1, is_admin=new_is_admin)
                            st.success("✅ Usuario creado.")
                            st.session_state["new_user"] = ""
                            st.session_state["new_is_admin"] = False
                            st.session_state["new_pw1"] = ""
                            st.session_state["new_pw2"] = ""
                            st.rerun()
                    except Exception as e:
                        st.error(f"No se pudo crear el usuario: {e}")

            with sub2:
                st.markdown("### Lista de usuarios")
                users = users_list(CLIENT)
                dfu = (
                    pd.DataFrame(users, columns=["ID", "Usuario", "Admin", "Creado"])
                    if users
                    else pd.DataFrame(columns=["ID", "Usuario", "Admin", "Creado"])
                )
                if not dfu.empty:
                    dfu["Admin"] = dfu["Admin"].apply(lambda x: "Sí" if int(x) == 1 else "No")
                    st.dataframe(dfu, use_container_width=True, hide_index=True)
                else:
                    st.info("No hay usuarios.")

                st.divider()
                st.markdown("### Cambiar rol (Admin/Usuario)")
                current_user = st.session_state.get("logged_user") or ""
                candidates = [(uid, uname, adm) for uid, uname, adm, _ in users if uname != current_user]

                if candidates:
                    pick = st.selectbox(
                        "Selecciona usuario",
                        [f"{uname} (Admin: {'Sí' if int(adm)==1 else 'No'}) | ID {uid}" for uid, uname, adm in candidates],
                        key="pick_role_user",
                    )
                    picked_id = int(pick.split("ID")[-1].strip())

                    current_admin_state = None
                    for uid, uname, adm in candidates:
                        if uid == picked_id:
                            current_admin_state = int(adm)
                            break

                    desired = st.checkbox(
                        "Marcar como ADMIN",
                        value=True if current_admin_state == 1 else False,
                        key="set_admin_chk",
                    )

                    if st.button("Guardar rol", key="btn_save_role"):
                        user_set_admin(CLIENT, picked_id, desired)
                        st.success("✅ Rol actualizado.")
                        st.rerun()
                else:
                    st.info("No puedes cambiar tu propio rol desde aquí.")

                st.divider()
                st.markdown("### Eliminar usuario")
                if candidates:
                    pick2 = st.selectbox(
                        "Selecciona usuario",
                        [f"{uname} (Admin: {'Sí' if int(adm)==1 else 'No'}) | ID {uid}" for uid, uname, adm in candidates],
                        key="pick_del_user",
                    )
                    picked_id2 = int(pick2.split("ID")[-1].strip())
                    confirm = st.checkbox(
                        "Confirmo eliminar este usuario definitivamente",
                        value=False,
                        key="confirm_del_user",
                    )
                    if st.button("Eliminar usuario", disabled=not confirm, key="btn_del_user"):
                        user_delete(CLIENT, picked_id2)
                        st.success("✅ Usuario eliminado.")
                        st.rerun()

            with sub3:
                st.markdown("### Resetear contraseña (solo admin)")
                st.caption("Asigna una contraseña nueva a un usuario sin conocer la actual.")

                users = users_list(CLIENT)
                current_user = st.session_state.get("logged_user") or ""
                candidates = [(uid, uname, adm) for uid, uname, adm, _ in users if uname != current_user]

                if not candidates:
                    st.info("No hay otros usuarios para resetear.")
                else:
                    pickr = st.selectbox(
                        "Usuario a resetear",
                        [f"{uname} (Admin: {'Sí' if int(adm)==1 else 'No'}) | ID {uid}" for uid, uname, adm in candidates],
                        key="pick_reset_user",
                    )
                    picked_idr = int(pickr.split("ID")[-1].strip())

                    rp1 = st.text_input("Nueva contraseña", type="password", key="reset_pw1")
                    rp2 = st.text_input("Confirmar nueva contraseña", type="password", key="reset_pw2")
                    confirm_reset = st.checkbox("Confirmo que deseo resetear la contraseña", value=False, key="confirm_reset")

                    if st.button("Resetear contraseña", type="primary", disabled=not confirm_reset, key="btn_reset_pw"):
                        try:
                            if rp1 != rp2:
                                st.error("Las contraseñas no coinciden.")
                            else:
                                user_update_password_by_id(CLIENT, picked_idr, rp1)
                                st.success("✅ Contraseña reseteada.")
                                st.session_state["reset_pw1"] = ""
                                st.session_state["reset_pw2"] = ""
                                st.session_state["confirm_reset"] = False
                                st.rerun()
                        except Exception as e:
                            st.error(f"No se pudo resetear: {e}")
