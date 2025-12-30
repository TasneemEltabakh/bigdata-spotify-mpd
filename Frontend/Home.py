import streamlit as st
from ui import apply_spotify_ui, add_now_playing_footer, render_sidebar

st.set_page_config(
    page_title="Spotify Analytics",
    layout="wide"
)

apply_spotify_ui()
render_sidebar()


# ===============================
# HERO SECTION
# ===============================
st.markdown(
    """
    <div style="
        background:
            linear-gradient(
                160deg,
                rgba(29,185,84,0.95) 0%,
                rgba(29,185,84,0.55) 25%,
                rgba(18,18,18,0.85) 55%,
                #121212 75%
            );
        padding: 70px 60px 60px 60px;
        border-radius: 20px;
        margin-bottom: 50px;
    ">
        <h1 style="color:black;">
            🎧 Spotify Dashboard
        </h1>
    </div>
    """,
    unsafe_allow_html=True
)


# ===============================
# EXPLORE SECTION
# ===============================
st.markdown("## Explore the platform")

c1, c2, c3 = st.columns(3)

with c1:
    st.markdown(
        """
        <div style="background:#181818; padding:26px; border-radius:18px;">
            <h3>🎵 Content Portfolio</h3>
            <p style="color:#B3B3B3; font-size:0.9rem;">
                Tracks, playlists, and catalog growth over time.
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )

with c2:
    st.markdown(
        """
        <div style="background:#181818; padding:26px; border-radius:18px;">
            <h3>🎤 Creator Landscape</h3>
            <p style="color:#B3B3B3; font-size:0.9rem;">
                Artist concentration, dominance, and long-tail behavior.
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )

with c3:
    st.markdown(
        """
        <div style="background:#181818; padding:26px; border-radius:18px;">
            <h3>👥 Engagement Segments</h3>
            <p style="color:#B3B3B3; font-size:0.9rem;">
                Playlist popularity tiers and follower distribution.
            </p>
            <br>
        </div>
        """,
        unsafe_allow_html=True
    )

st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)

# ===============================
# FOOTER
# ===============================
add_now_playing_footer(
    track="Now Viewing: Home",
    artist="Spotify Analytics Dashboard",
    progress=80
)

