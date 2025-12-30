import streamlit as st
import streamlit.components.v1 as components
import base64
import os
import base64
import streamlit as st

# =====================================================
# GLOBAL SPOTIFY UI + SIDEBAR (SPOTIFY-STYLE)
# =====================================================
def apply_spotify_ui():
    # -----------------------------
    # Resolve absolute path safely
    # -----------------------------
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    logo_path = os.path.join(BASE_DIR, "assets", "spotify_logo.png")

    logo_base64 = base64.b64encode(
        open(logo_path, "rb").read()
    ).decode()

    st.markdown(
        f"""
        <style>
        /* =====================
           GLOBAL
        ===================== */
        html, body {{
            background-color: #121212;
            font-family: Inter, system-ui, sans-serif;
        }}

        /* =====================
           SIDEBAR BASE
        ===================== */
        section[data-testid="stSidebar"] {{
            background-color: #000000;
            border-right: 1px solid #1f1f1f;
            position: relative;
        }}

        /* =====================
           LOGO AT VERY TOP
        ===================== */
        section[data-testid="stSidebar"]::before {{
            content: "";
            display: block;
            height: 110px;
            background-image: url("data:image/png;base64,{logo_base64}");
            background-repeat: no-repeat;
            background-position: center 40px;
            background-size: 75px 75px;
        }}

        /* Push menu BELOW logo */
        section[data-testid="stSidebar"] .stSidebarContent {{
            padding-top: 20px !important;
        }}

        /* =====================
           SIDEBAR LINKS
        ===================== */
        section[data-testid="stSidebar"] a {{
            padding: 10px 14px;
            border-radius: 999px;
            margin: 2px 10px;
            display: flex;
            align-items: center;
            gap: 10px;
            color: #EAEAEA;
            font-weight: 500;
            text-decoration: none;
            position: relative;
        }}

        section[data-testid="stSidebar"] a:hover {{
            background-color: #1a1a1a;
            color: #1DB954;
        }}

        section[data-testid="stSidebar"] a[aria-current="page"] {{
            background-color: rgba(29, 185, 84, 0.12);
            color: #1DB954;
            font-weight: 700;
        }}

        section[data-testid="stSidebar"] a[aria-current="page"]::before {{
            content: "";
            position: absolute;
            left: -6px;
            width: 4px;
            height: 60%;
            background-color: #1DB954;
            border-radius: 4px;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )


# =====================================================
# SIDEBAR RENDER (HTML ONLY — NO CSS HERE)
# =====================================================
def render_sidebar():
    pass

# =====================================================
# SPOTIFY-STYLE NOW PLAYING FOOTER
# =====================================================
def add_now_playing_footer(
    track=" Now Viewing: Home",
    artist="Dashboard",
    progress=80
):
    components.html(
        f"""
        <style>
        .now-playing {{
            position: fixed;
            bottom: 0;
            left: 0;
            width: 100%;
            height: 90px;
            background: linear-gradient(to right, #181818, #121212);
            border-top: 1px solid #2A2A2A;
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0 30px;
            z-index: 9999;
            box-shadow: 0 -10px 30px rgba(0,0,0,0.7);
            font-family: Inter, sans-serif;
        }}

        .np-left {{
            display: flex;
            flex-direction: column;
        }}

        .np-track {{
            color: white;
            font-weight: 700;
            font-size: 1rem;
        }}

        .np-artist {{
            color: #B3B3B3;
            font-size: 0.85rem;
        }}

        .np-center {{
            display: flex;
            flex-direction: column;
            align-items: center;
            width: 40%;
        }}

        .np-controls {{
            display: flex;
            gap: 20px;
            font-size: 1.4rem;
            color: white;
            margin-bottom: 6px;
        }}

        .np-progress {{
            width: 100%;
            height: 4px;
            background: #2A2A2A;
            border-radius: 10px;
            overflow: hidden;
        }}

        .np-progress-fill {{
            width: {progress}%;
            height: 100%;
            background: #1DB954;
        }}

        .np-right {{
            color: #B3B3B3;
            font-size: 1.2rem;
        }}

        body {{
            padding-bottom: 110px;
        }}
        </style>

        <div class="now-playing">
            <div class="np-left">
                <div class="np-track">🎵 {track}</div>
                <div class="np-artist">{artist}</div>
            </div>

            <div class="np-center">
                <div class="np-controls">⏮️ ▶️ ⏭️</div>
                <div class="np-progress">
                    <div class="np-progress-fill"></div>
                </div>
            </div>

            <div class="np-right">🔊</div>
        </div>
        """,
        height=90
    )