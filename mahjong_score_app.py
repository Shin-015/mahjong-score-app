import sqlite3
from contextlib import closing
from datetime import date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

DB_PATH = "mahjong_data.db"


def get_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    with closing(get_connection()) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_name TEXT NOT NULL UNIQUE,
                player_count INTEGER NOT NULL DEFAULT 4,
                starting_points INTEGER NOT NULL,
                return_points INTEGER NOT NULL,
                oka REAL NOT NULL DEFAULT 0,
                uma_1 REAL NOT NULL DEFAULT 0,
                uma_2 REAL NOT NULL DEFAULT 0,
                uma_3 REAL NOT NULL DEFAULT 0,
                uma_4 REAL NOT NULL DEFAULT 0,
                has_hakoshita INTEGER NOT NULL DEFAULT 0,
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_date TEXT NOT NULL,
                rule_id INTEGER NOT NULL,
                title TEXT,
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (rule_id) REFERENCES rules (id)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS session_players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL,
                seat_no INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (session_id) REFERENCES sessions (id),
                FOREIGN KEY (player_id) REFERENCES players (id),
                UNIQUE(session_id, seat_no)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hanchans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL,
                hanchan_no INTEGER NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (session_id) REFERENCES sessions (id)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hanchan_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hanchan_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                final_score INTEGER NOT NULL,
                rank INTEGER NOT NULL,
                settlement REAL NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (hanchan_id) REFERENCES hanchans (id),
                FOREIGN KEY (player_id) REFERENCES players (id)
            )
            """
        )

        conn.commit()


def fetch_dataframe(query, params=()):
    with closing(get_connection()) as conn:
        return pd.read_sql_query(query, conn, params=params)


def execute_query(query, params=()):
    with closing(get_connection()) as conn:
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()


def add_player(name):
    name = name.strip()
    if not name:
        raise ValueError("プレイヤー名を入力してください。")
    try:
        execute_query("INSERT INTO players (name) VALUES (?)", (name,))
    except sqlite3.IntegrityError:
        raise ValueError("同じ名前のプレイヤーはすでに登録されています。")


def delete_player(player_id):
    execute_query("DELETE FROM players WHERE id = ?", (player_id,))


def get_players():
    return fetch_dataframe(
        "SELECT id, name, created_at FROM players ORDER BY name COLLATE NOCASE ASC"
    )


def add_rule(
    rule_name,
    player_count,
    starting_points,
    return_points,
    oka,
    uma_1,
    uma_2,
    uma_3,
    uma_4,
    has_hakoshita,
    notes,
):
    rule_name = rule_name.strip()
    if not rule_name:
        raise ValueError("ルール名を入力してください。")
    try:
        execute_query(
            """
            INSERT INTO rules (
                rule_name, player_count, starting_points, return_points,
                oka, uma_1, uma_2, uma_3, uma_4, has_hakoshita, notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rule_name,
                player_count,
                starting_points,
                return_points,
                oka,
                uma_1,
                uma_2,
                uma_3,
                uma_4,
                int(has_hakoshita),
                notes.strip(),
            ),
        )
    except sqlite3.IntegrityError:
        raise ValueError("同じ名前のルールはすでに登録されています。")


def get_rules():
    return fetch_dataframe(
        """
        SELECT
            id, rule_name, player_count, starting_points, return_points,
            oka, uma_1, uma_2, uma_3, uma_4, has_hakoshita, notes, created_at
        FROM rules
        ORDER BY created_at DESC
        """
    )


def delete_rule(rule_id):
    session_count_df = fetch_dataframe(
        "SELECT COUNT(*) AS cnt FROM sessions WHERE rule_id = ?",
        (rule_id,),
    )
    if int(session_count_df.iloc[0]["cnt"]) > 0:
        raise ValueError("このルールを使っている対局データがあるため削除できません。")
    execute_query("DELETE FROM rules WHERE id = ?", (rule_id,))


def create_session(session_date, rule_id, title, notes, player_ids):
    if len(player_ids) != 4 or len(set(player_ids)) != 4:
        raise ValueError("参加者4人を重複なく選んでください。")

    with closing(get_connection()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO sessions (session_date, rule_id, title, notes)
            VALUES (?, ?, ?, ?)
            """,
            (session_date, rule_id, title.strip(), notes.strip()),
        )
        session_id = cur.lastrowid
        for i, player_id in enumerate(player_ids, start=1):
            cur.execute(
                "INSERT INTO session_players (session_id, seat_no, player_id) VALUES (?, ?, ?)",
                (session_id, i, int(player_id)),
            )
        conn.commit()
    return session_id


def update_session(session_id, session_date, rule_id, title, notes):
    execute_query(
        """
        UPDATE sessions
        SET session_date = ?, rule_id = ?, title = ?, notes = ?
        WHERE id = ?
        """,
        (session_date, rule_id, title.strip(), notes.strip(), session_id),
    )


def delete_session(session_id):
    with closing(get_connection()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM hanchans WHERE session_id = ?", (session_id,))
        hanchan_ids = [row[0] for row in cur.fetchall()]
        for hanchan_id in hanchan_ids:
            cur.execute("DELETE FROM hanchan_results WHERE hanchan_id = ?", (hanchan_id,))
        cur.execute("DELETE FROM hanchans WHERE session_id = ?", (session_id,))
        cur.execute("DELETE FROM session_players WHERE session_id = ?", (session_id,))
        cur.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
        conn.commit()


def get_sessions():
    return fetch_dataframe(
        """
        SELECT
            s.id, s.session_date, s.title, r.rule_name, s.notes, s.created_at
        FROM sessions s
        JOIN rules r ON s.rule_id = r.id
        ORDER BY s.id DESC
        """
    )


def get_session_detail(session_id):
    return fetch_dataframe(
        """
        SELECT
            s.id, s.session_date, s.title, s.notes,
            r.id AS rule_id, r.rule_name, r.player_count,
            r.starting_points, r.return_points, r.oka,
            r.uma_1, r.uma_2, r.uma_3, r.uma_4, r.has_hakoshita
        FROM sessions s
        JOIN rules r ON s.rule_id = r.id
        WHERE s.id = ?
        """,
        (session_id,),
    )


def get_session_players(session_id):
    return fetch_dataframe(
        """
        SELECT
            sp.seat_no, p.id AS player_id, p.name
        FROM session_players sp
        JOIN players p ON sp.player_id = p.id
        WHERE sp.session_id = ?
        ORDER BY sp.seat_no ASC
        """,
        (session_id,),
    )


def get_next_hanchan_no(session_id):
    df = fetch_dataframe(
        "SELECT COALESCE(MAX(hanchan_no), 0) AS max_no FROM hanchans WHERE session_id = ?",
        (session_id,),
    )
    return int(df.iloc[0]["max_no"]) + 1


def calculate_settlements_bottom_up(final_scores, rule_row):
    scores = [int(x) for x in final_scores]
    indexed = list(enumerate(scores))
    sorted_pairs = sorted(indexed, key=lambda x: x[1], reverse=True)

    rank_map = {}
    position_to_indices = {1: [], 2: [], 3: [], 4: []}
    current_position = 1

    for idx, (original_index, score) in enumerate(sorted_pairs):
        if idx > 0 and score < sorted_pairs[idx - 1][1]:
            current_position = idx + 1
        rank_map[original_index] = current_position
        position_to_indices[current_position].append(original_index)

    uma1 = float(rule_row["uma_1"])
    uma2 = float(rule_row["uma_2"])
    uma3 = float(rule_row["uma_3"])
    uma4 = float(rule_row["uma_4"])
    oka = float(rule_row["oka"])
    return_points = int(rule_row["return_points"])

    uma_values = [0.0, 0.0, 0.0, 0.0]

    # 通常
    if (
        len(position_to_indices[1]) == 1
        and len(position_to_indices[2]) == 1
        and len(position_to_indices[3]) == 1
        and len(position_to_indices[4]) == 1
    ):
        only_map = {1: uma1 + oka, 2: uma2, 3: uma3, 4: uma4}
        for pos in [1, 2, 3, 4]:
            i = position_to_indices[pos][0]
            uma_values[i] = only_map[pos]

    # 1位2位同着
    elif len(position_to_indices[1]) == 2 and len(position_to_indices[3]) == 1 and len(position_to_indices[4]) == 1:
        split_top = (uma1 + uma2 + oka) / 2.0
        for i in position_to_indices[1]:
            uma_values[i] = split_top
        uma_values[position_to_indices[3][0]] = uma3
        uma_values[position_to_indices[4][0]] = uma4

    # 2位3位同着
    elif len(position_to_indices[1]) == 1 and len(position_to_indices[2]) == 2 and len(position_to_indices[4]) == 1:
        uma_values[position_to_indices[1][0]] = uma1 + oka
        for i in position_to_indices[2]:
            uma_values[i] = 0.0
        uma_values[position_to_indices[4][0]] = uma4

    # 3位4位同着
    elif len(position_to_indices[1]) == 1 and len(position_to_indices[2]) == 1 and len(position_to_indices[3]) == 2:
        uma_values[position_to_indices[1][0]] = uma1 + oka
        uma_values[position_to_indices[2][0]] = uma2
        split_bottom = (uma3 + uma4) / 2.0
        for i in position_to_indices[3]:
            uma_values[i] = split_bottom

    # その他
    else:
        for pos in [4, 3, 2]:
            targets = position_to_indices[pos]
            if len(targets) == 1:
                if pos == 4:
                    uma_values[targets[0]] = uma4
                elif pos == 3:
                    uma_values[targets[0]] = uma3
                elif pos == 2:
                    uma_values[targets[0]] = uma2
            elif len(targets) > 1:
                if pos == 4:
                    split = uma4 / len(targets)
                elif pos == 3:
                    split = uma3 / len(targets)
                else:
                    split = uma2 / len(targets)
                for i in targets:
                    uma_values[i] = split

    settlements = []
    for i in range(4):
        base = (scores[i] - return_points) / 1000.0
        settlements.append(round(base + uma_values[i], 1))

    total_now = round(sum(settlements), 1)
    if total_now != 0.0:
        top_targets = position_to_indices[1]
        correction = round(-total_now, 1)
        if len(top_targets) == 1:
            settlements[top_targets[0]] = round(settlements[top_targets[0]] + correction, 1)
        else:
            split = round(correction / len(top_targets), 1)
            assigned = 0.0
            for i in top_targets[:-1]:
                settlements[i] = round(settlements[i] + split, 1)
                assigned = round(assigned + split, 1)
            settlements[top_targets[-1]] = round(settlements[top_targets[-1]] + (correction - assigned), 1)

    rank_list = [rank_map[i] for i in range(4)]
    return rank_list, settlements


def build_hanchan_preview(session_id, final_scores):
    session_df = get_session_detail(session_id)
    players_df = get_session_players(session_id)
    if session_df.empty or players_df.empty:
        return pd.DataFrame(), 0.0

    rule_row = session_df.iloc[0]
    names = players_df["name"].tolist()
    rank_list, settlements = calculate_settlements_bottom_up(final_scores, rule_row)

    rows = []
    for i, name in enumerate(names):
        rows.append(
            {
                "プレイヤー": name,
                "点数": int(final_scores[i]),
                "順位": int(rank_list[i]),
                "精算": round(float(settlements[i]), 1),
            }
        )

    preview_df = pd.DataFrame(rows).sort_values(["順位", "プレイヤー"]).reset_index(drop=True)
    total_settlement = round(float(preview_df["精算"].sum()), 1)
    return preview_df, total_settlement


def add_hanchan_result(session_id, final_scores):
    players_df = get_session_players(session_id)
    if len(players_df) != 4 or len(final_scores) != 4:
        raise ValueError("4人固定の対局データが必要です。")

    preview_df, _ = build_hanchan_preview(session_id, final_scores)
    hanchan_no = get_next_hanchan_no(session_id)

    with closing(get_connection()) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO hanchans (session_id, hanchan_no) VALUES (?, ?)",
            (session_id, hanchan_no),
        )
        hanchan_id = cur.lastrowid

        player_map = dict(zip(players_df["name"], players_df["player_id"]))
        for _, row in preview_df.iterrows():
            cur.execute(
                """
                INSERT INTO hanchan_results (hanchan_id, player_id, final_score, rank, settlement)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    hanchan_id,
                    int(player_map[row["プレイヤー"]]),
                    int(row["点数"]),
                    int(row["順位"]),
                    float(row["精算"]),
                ),
            )
        conn.commit()


def update_hanchan_result(session_id, hanchan_no, final_scores):
    players_df = get_session_players(session_id)
    if len(players_df) != 4 or len(final_scores) != 4:
        raise ValueError("4人固定の対局データが必要です。")

    preview_df, _ = build_hanchan_preview(session_id, final_scores)

    with closing(get_connection()) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM hanchans WHERE session_id = ? AND hanchan_no = ?",
            (session_id, hanchan_no),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError("修正する半荘が見つかりません。")
        hanchan_id = row[0]

        cur.execute("DELETE FROM hanchan_results WHERE hanchan_id = ?", (hanchan_id,))

        player_map = dict(zip(players_df["name"], players_df["player_id"]))
        for _, result_row in preview_df.iterrows():
            cur.execute(
                """
                INSERT INTO hanchan_results (hanchan_id, player_id, final_score, rank, settlement)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    hanchan_id,
                    int(player_map[result_row["プレイヤー"]]),
                    int(result_row["点数"]),
                    int(result_row["順位"]),
                    float(result_row["精算"]),
                ),
            )
        conn.commit()


def get_hanchan_results(session_id):
    return fetch_dataframe(
        """
        SELECT
            hr.id, h.hanchan_no, p.name, hr.final_score, hr.rank, hr.settlement
        FROM hanchan_results hr
        JOIN hanchans h ON hr.hanchan_id = h.id
        JOIN players p ON hr.player_id = p.id
        WHERE h.session_id = ?
        ORDER BY h.hanchan_no DESC, hr.rank ASC
        """,
        (session_id,),
    )


def get_session_player_totals(session_id):
    return fetch_dataframe(
        """
        SELECT
            p.name,
            COUNT(hr.id) AS games,
            SUM(CASE WHEN hr.rank = 1 THEN 1 ELSE 0 END) AS first_count,
            SUM(CASE WHEN hr.rank = 2 THEN 1 ELSE 0 END) AS second_count,
            SUM(CASE WHEN hr.rank = 3 THEN 1 ELSE 0 END) AS third_count,
            SUM(CASE WHEN hr.rank = 4 THEN 1 ELSE 0 END) AS fourth_count,
            ROUND(AVG(CAST(hr.rank AS REAL)), 2) AS avg_rank,
            ROUND(SUM(hr.settlement), 1) AS total_settlement
        FROM hanchan_results hr
        JOIN players p ON hr.player_id = p.id
        JOIN hanchans h ON hr.hanchan_id = h.id
        WHERE h.session_id = ?
        GROUP BY p.id, p.name
        ORDER BY total_settlement DESC, avg_rank ASC
        """,
        (session_id,),
    )


def get_player_stats(player_name=None, start_date=None, end_date=None):
    conditions = []
    params = []
    if player_name:
        conditions.append("p.name = ?")
        params.append(player_name)
    if start_date:
        conditions.append("s.session_date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("s.session_date <= ?")
        params.append(end_date)

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    query = f"""
        SELECT
            p.name,
            COUNT(hr.id) AS games,
            SUM(CASE WHEN hr.rank = 1 THEN 1 ELSE 0 END) AS first_count,
            SUM(CASE WHEN hr.rank = 2 THEN 1 ELSE 0 END) AS second_count,
            SUM(CASE WHEN hr.rank = 3 THEN 1 ELSE 0 END) AS third_count,
            SUM(CASE WHEN hr.rank = 4 THEN 1 ELSE 0 END) AS fourth_count,
            ROUND(AVG(CAST(hr.rank AS REAL)), 2) AS avg_rank,
            ROUND(SUM(hr.settlement), 1) AS total_settlement
        FROM players p
        LEFT JOIN hanchan_results hr ON p.id = hr.player_id
        LEFT JOIN hanchans h ON hr.hanchan_id = h.id
        LEFT JOIN sessions s ON h.session_id = s.id
        {where_clause}
        GROUP BY p.id, p.name
        ORDER BY total_settlement DESC, avg_rank ASC
    """
    return fetch_dataframe(query, tuple(params))


def get_session_rank_trend(session_id):
    df = fetch_dataframe(
        """
        SELECT
            h.hanchan_no, p.name, hr.rank
        FROM hanchan_results hr
        JOIN players p ON hr.player_id = p.id
        JOIN hanchans h ON hr.hanchan_id = h.id
        WHERE h.session_id = ?
        ORDER BY p.name ASC, h.hanchan_no ASC
        """,
        (session_id,),
    )
    if df.empty:
        return df

    frames = []
    for player, group in df.groupby("name"):
        g = group.copy().reset_index(drop=True)
        g["半荘数"] = range(1, len(g) + 1)
        g["平均順位"] = g["rank"].expanding().mean().round(3)
        g["プレイヤー"] = player
        frames.append(g[["半荘数", "平均順位", "プレイヤー"]])

    return pd.concat(frames, ignore_index=True)


def make_rank_line_chart(rank_trend_df, title):
    fig = go.Figure()

    for player in rank_trend_df["プレイヤー"].unique():
        player_df = rank_trend_df[rank_trend_df["プレイヤー"] == player]
        fig.add_trace(
            go.Scatter(
                x=player_df["半荘数"],
                y=player_df["平均順位"],
                mode="lines+markers",
                name=player,
            )
        )

    max_x = int(rank_trend_df["半荘数"].max()) if not rank_trend_df.empty else 1

    fig.update_layout(
        title=title,
        xaxis_title="半荘数",
        yaxis_title="順位",
        yaxis=dict(
            autorange="reversed",
            tickmode="array",
            tickvals=[1, 2, 3, 4],
            ticktext=["1", "2", "3", "4"],
            range=[4.1, 0.9],
        ),
        xaxis=dict(
            tickmode="array",
            tickvals=list(range(1, max_x + 1)),
        ),
        margin=dict(l=20, r=20, t=50, b=20),
        height=420,
    )
    return fig


def page_home():
    st.title("麻雀成績管理サイト")
    st.write("スマホから入力できる成績管理サイトです。")

    players_df = get_players()
    rules_df = get_rules()
    sessions_df = get_sessions()

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("プレイヤー数", len(players_df))
    with col2:
        st.metric("ルール数", len(rules_df))
    with col3:
        st.metric("対局データ数", len(sessions_df))


def page_players():
    st.title("プレイヤー登録")

    with st.form("add_player_form", clear_on_submit=True):
        name = st.text_input("プレイヤー名")
        submitted = st.form_submit_button("追加")
        if submitted:
            try:
                add_player(name)
                st.success("プレイヤーを追加しました。")
                st.rerun()
            except ValueError as e:
                st.error(str(e))

    players_df = get_players()
    if players_df.empty:
        st.info("まだプレイヤーが登録されていません。")
        return

    display_df = players_df.rename(
        columns={
            "id": "ID",
            "name": "プレイヤー名",
            "created_at": "登録日時",
        }
    )
    st.dataframe(display_df[["ID", "プレイヤー名", "登録日時"]], use_container_width=True, hide_index=True)

    with st.expander("プレイヤー削除"):
        selected_player = st.selectbox(
            "削除するプレイヤー",
            options=players_df["id"].tolist(),
            format_func=lambda x: players_df.loc[players_df["id"] == x, "name"].iloc[0],
        )
        if st.button("このプレイヤーを削除"):
            delete_player(int(selected_player))
            st.success("削除しました。")
            st.rerun()


def page_rules():
    st.title("ルール登録")

    with st.form("add_rule_form", clear_on_submit=True):
        rule_name = st.text_input("ルール名", placeholder="例: 25-30 10-20")

        col1, col2, col3 = st.columns(3)
        with col1:
            player_count = st.number_input("人数", min_value=3, max_value=4, value=4, step=1)
        with col2:
            starting_points = st.number_input("持ち点", min_value=0, value=25000, step=1000)
        with col3:
            return_points = st.number_input("返し点", min_value=0, value=30000, step=1000)

        col4, col5 = st.columns(2)
        with col4:
            oka = st.number_input("オカ", value=20.0, step=1.0)
        with col5:
            has_hakoshita = st.checkbox("箱下あり", value=True)

        st.markdown("#### ウマ")
        col6, col7, col8, col9 = st.columns(4)
        with col6:
            uma_1 = st.number_input("1着馬", value=20.0, step=1.0)
        with col7:
            uma_2 = st.number_input("2着馬", value=10.0, step=1.0)
        with col8:
            uma_3 = st.number_input("3着馬", value=-10.0, step=1.0)
        with col9:
            uma_4 = st.number_input("4着馬", value=-20.0, step=1.0)

        notes = st.text_area("メモ")
        submitted = st.form_submit_button("ルールを追加")
        if submitted:
            try:
                add_rule(
                    rule_name,
                    int(player_count),
                    int(starting_points),
                    int(return_points),
                    float(oka),
                    float(uma_1),
                    float(uma_2),
                    float(uma_3),
                    float(uma_4),
                    has_hakoshita,
                    notes,
                )
                st.success("ルールを追加しました。")
                st.rerun()
            except ValueError as e:
                st.error(str(e))

    rules_df = get_rules()
    if rules_df.empty:
        st.info("まだルールが登録されていません。")
        return

    display_df = rules_df.copy()
    display_df["has_hakoshita"] = display_df["has_hakoshita"].map({1: "あり", 0: "なし"})
    display_df = display_df.rename(
        columns={
            "id": "ID",
            "rule_name": "ルール名",
            "player_count": "人数",
            "starting_points": "持ち点",
            "return_points": "返し点",
            "oka": "オカ",
            "uma_1": "1着馬",
            "uma_2": "2着馬",
            "uma_3": "3着馬",
            "uma_4": "4着馬",
            "has_hakoshita": "箱下",
            "notes": "メモ",
            "created_at": "登録日時",
        }
    )
    st.dataframe(
        display_df[
            ["ID", "ルール名", "人数", "持ち点", "返し点", "オカ", "1着馬", "2着馬", "3着馬", "4着馬", "箱下", "メモ", "登録日時"]
        ],
        use_container_width=True,
        hide_index=True,
    )

    with st.expander("ルール削除"):
        delete_rule_id = st.selectbox(
            "削除するルール",
            options=rules_df["id"].tolist(),
            format_func=lambda x: rules_df.loc[rules_df["id"] == x, "rule_name"].iloc[0],
        )
        if st.button("このルールを削除"):
            try:
                delete_rule(int(delete_rule_id))
                st.success("ルールを削除しました。")
                st.rerun()
            except ValueError as e:
                st.error(str(e))


def page_session_input():
    st.title("対局データ入力")

    players_df = get_players()
    rules_df = get_rules()
    sessions_df = get_sessions()

    if len(players_df) < 4:
        st.warning("先にプレイヤーを4人以上登録してください。")
        return
    if rules_df.empty:
        st.warning("先にルールを1つ以上登録してください。")
        return

    if "input_mode" not in st.session_state:
        st.session_state["input_mode"] = "menu"
    if "active_session_id" not in st.session_state:
        st.session_state["active_session_id"] = None

    if st.session_state["input_mode"] == "menu":
        st.subheader("新しい対局データ")
        if st.button("新規作成"):
            st.session_state["input_mode"] = "create"
            st.rerun()

        if not sessions_df.empty:
            st.divider()
            st.subheader("既存の対局データを編集・削除")

            target_session_id = st.selectbox(
                "対局データ",
                options=sessions_df["id"].tolist(),
                format_func=lambda x: (
                    str(sessions_df.loc[sessions_df["id"] == x, "session_date"].iloc[0])
                    + " | "
                    + str(sessions_df.loc[sessions_df["id"] == x, "title"].fillna("タイトルなし").iloc[0])
                    + " | "
                    + str(sessions_df.loc[sessions_df["id"] == x, "rule_name"].iloc[0])
                ),
            )
            target_detail = get_session_detail(int(target_session_id)).iloc[0]

            st.markdown("#### この対局データに半荘を追加")
            st.caption("前に作った対局データを開いて、続きの半荘を追加できます。")
            if st.button("この対局データで半荘入力を再開"):
                st.session_state["active_session_id"] = int(target_session_id)
                st.session_state["input_mode"] = "input"
                st.rerun()

            with st.expander("対局データを編集"):
                with st.form("edit_session_form"):
                    edit_date = st.date_input("日付", value=pd.to_datetime(target_detail["session_date"]).date())
                    edit_title = st.text_input(
                        "タイトル",
                        value="" if pd.isna(target_detail["title"]) else str(target_detail["title"]),
                    )
                    edit_rule_id = st.selectbox(
                        "ルール",
                        options=rules_df["id"].tolist(),
                        index=rules_df[rules_df["id"] == int(target_detail["rule_id"])].index[0],
                        format_func=lambda x: rules_df.loc[rules_df["id"] == x, "rule_name"].iloc[0],
                    )
                    edit_notes = st.text_area(
                        "メモ",
                        value="" if pd.isna(target_detail["notes"]) else str(target_detail["notes"]),
                    )
                    if st.form_submit_button("編集を保存"):
                        update_session(int(target_session_id), str(edit_date), int(edit_rule_id), edit_title, edit_notes)
                        st.success("対局データを編集しました。")
                        st.rerun()

            with st.expander("途中の半荘を修正"):
                edit_results_df = get_hanchan_results(int(target_session_id))
                fixed_players_df = get_session_players(int(target_session_id))
                if edit_results_df.empty:
                    st.info("まだ半荘データがありません。")
                else:
                    edit_hanchan_no = st.selectbox(
                        "修正する半荘",
                        options=sorted(edit_results_df["hanchan_no"].unique().tolist()),
                        key="menu_edit_hanchan_no",
                    )
                    target_hanchan_df = edit_results_df[edit_results_df["hanchan_no"] == edit_hanchan_no].copy()
                    score_map = dict(zip(target_hanchan_df["name"], target_hanchan_df["final_score"]))

                    with st.form("menu_edit_hanchan_form"):
                        edit_scores = []
                        for i, fixed_row in fixed_players_df.iterrows():
                            col1, col2 = st.columns([1, 1])
                            with col1:
                                st.text_input(
                                    f"{int(fixed_row['seat_no'])}人目",
                                    value=fixed_row["name"],
                                    disabled=True,
                                    key=f"menu_fixed_name_{i}",
                                )
                            with col2:
                                score = st.number_input(
                                    "点数",
                                    value=int(score_map.get(fixed_row["name"], 25000)),
                                    step=100,
                                    key=f"menu_edit_score_{i}",
                                )
                                edit_scores.append(int(score))

                        menu_base_total = int(target_detail["starting_points"]) * 4
                        menu_score_total = sum(edit_scores)
                        menu_score_diff = menu_score_total - menu_base_total

                        col1, col2, col3 = st.columns(3)
                        with col1:
                            menu_check_submitted = st.form_submit_button("チェック")
                        with col2:
                            menu_save_submitted = st.form_submit_button("この半荘を修正")
                        with col3:
                            menu_close = st.form_submit_button("閉じる")

                        if menu_check_submitted or menu_save_submitted:
                            st.markdown("#### 点数合計チェック")
                            col_a, col_b, col_c = st.columns(3)
                            with col_a:
                                st.metric("点数合計", menu_score_total)
                            with col_b:
                                st.metric("基準合計", menu_base_total)
                            with col_c:
                                st.metric("差分", menu_score_diff)

                            if menu_score_diff == 0:
                                st.success("点数合計の差分は 0 です。")
                            else:
                                st.warning(f"点数合計の差分が {menu_score_diff} です。0 ではありません。")

                            preview_df, total_settlement = build_hanchan_preview(int(target_session_id), edit_scores)
                            if not preview_df.empty:
                                st.markdown("#### 修正後の結果")
                                st.dataframe(preview_df, use_container_width=True, hide_index=True)
                                if total_settlement != 0.0:
                                    st.warning(f"精算合計が {total_settlement:.1f} です。0.0 ではありません。")
                                else:
                                    st.success("精算合計は 0.0 です。")

                        if menu_save_submitted:
                            update_hanchan_result(int(target_session_id), int(edit_hanchan_no), edit_scores)
                            st.success("半荘結果を修正しました。")
                            st.rerun()

                        if menu_close:
                            st.rerun()

            with st.expander("対局データを削除"):
                st.warning("この対局データを削除すると、その中の半荘結果もすべて消えます。")
                if st.button("この対局データを削除"):
                    delete_session(int(target_session_id))
                    st.success("対局データを削除しました。")
                    st.rerun()
        return

    if st.session_state["input_mode"] == "create":
        st.subheader("① 日付・ルール・名前を選択")
        with st.form("create_session_flow"):
            session_date = st.date_input("日付", value=date.today())
            title = st.text_input("名前", placeholder="例: 研究室麻雀")
            rule_id = st.selectbox(
                "ルール",
                options=rules_df["id"].tolist(),
                format_func=lambda x: rules_df.loc[rules_df["id"] == x, "rule_name"].iloc[0],
            )
            notes = st.text_area("メモ")

            st.markdown("#### 参加者4人")
            selected_player_ids = []
            for i in range(4):
                pid = st.selectbox(
                    f"{i+1}人目",
                    options=players_df["id"].tolist(),
                    format_func=lambda x: players_df.loc[players_df["id"] == x, "name"].iloc[0],
                    key=f"create_player_{i}",
                )
                selected_player_ids.append(int(pid))

            col1, col2 = st.columns(2)
            with col1:
                create_submitted = st.form_submit_button("入力開始")
            with col2:
                cancel_submitted = st.form_submit_button("戻る")

            if cancel_submitted:
                st.session_state["input_mode"] = "menu"
                st.rerun()

            if create_submitted:
                try:
                    session_id = create_session(
                        str(session_date), int(rule_id), title, notes, selected_player_ids
                    )
                    st.session_state["active_session_id"] = int(session_id)
                    st.session_state["input_mode"] = "input"
                    st.rerun()
                except ValueError as e:
                    st.error(str(e))
        return

    if st.session_state["input_mode"] == "input":
        session_id = st.session_state["active_session_id"]
        if not session_id:
            st.session_state["input_mode"] = "menu"
            st.rerun()

        session_detail = get_session_detail(int(session_id)).iloc[0]
        session_players = get_session_players(int(session_id))

        st.subheader("② 半荘結果を追加")
        st.markdown(
            "**日付:** {}  \n**名前:** {}  \n**ルール:** {}".format(
                session_detail["session_date"],
                "タイトルなし" if pd.isna(session_detail["title"]) or session_detail["title"] == "" else session_detail["title"],
                session_detail["rule_name"],
            )
        )
        st.caption("参加者は固定です。変える場合は新しい対局データを作ってください。")
        st.caption("次は第{}半荘です。".format(get_next_hanchan_no(int(session_id))))

        with st.form("add_hanchan_flow", clear_on_submit=True):
            scores = []
            for i, row in session_players.iterrows():
                col1, col2 = st.columns([1, 1])
                with col1:
                    st.text_input(
                        f"{int(row['seat_no'])}人目",
                        value=row["name"],
                        disabled=True,
                        key=f"fixed_name_{i}",
                    )
                with col2:
                    score = st.number_input(
                        "点数",
                        value=25000,
                        step=100,
                        key=f"fixed_score_{i}",
                    )
                    scores.append(int(score))

            base_total = int(session_detail["starting_points"]) * 4
            score_total = sum(scores)
            score_diff = score_total - base_total

            col1, col2, col3 = st.columns(3)
            with col1:
                check_submitted = st.form_submit_button("チェック")
            with col2:
                add_submitted = st.form_submit_button("追加")
            with col3:
                finish_submitted = st.form_submit_button("終了")

            if check_submitted or add_submitted:
                st.markdown("#### 点数合計チェック")
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    st.metric("点数合計", score_total)
                with col_b:
                    st.metric("基準合計", base_total)
                with col_c:
                    st.metric("差分", score_diff)

                if score_diff == 0:
                    st.success("点数合計の差分は 0 です。")
                else:
                    st.warning(f"点数合計の差分が {score_diff} です。0 ではありません。")

                preview_df, total_settlement = build_hanchan_preview(int(session_id), scores)
                if not preview_df.empty:
                    st.markdown("#### 今回の結果")
                    st.dataframe(preview_df, use_container_width=True, hide_index=True)
                    if total_settlement != 0.0:
                        st.warning(f"精算合計が {total_settlement:.1f} です。0.0 ではありません。")
                    else:
                        st.success("精算合計は 0.0 です。")

            if add_submitted:
                if score_diff != 0:
                    st.warning(f"点数合計の差分が {score_diff} のままです。")
                add_hanchan_result(int(session_id), scores)
                st.success("半荘結果を追加しました。")
                st.rerun()

            if finish_submitted:
                st.session_state["day_stats_session_id"] = int(session_id)
                st.session_state["input_mode"] = "menu"
                st.session_state["active_session_id"] = None
                st.rerun()

        results_df = get_hanchan_results(int(session_id))
        if not results_df.empty:
            st.divider()
            st.subheader("登録済み半荘")
            display_results = results_df[["hanchan_no", "name", "final_score", "rank", "settlement"]].copy()
            display_results.columns = ["半荘", "プレイヤー", "点数", "順位", "精算"]
            st.dataframe(display_results, use_container_width=True, hide_index=True)
        return


def page_day_stats():
    st.title("その日の成績")

    sessions_df = get_sessions()
    if sessions_df.empty:
        st.info("まだ対局データがありません。")
        return

    if "day_stats_session_id" not in st.session_state:
        st.session_state["day_stats_session_id"] = int(sessions_df.iloc[0]["id"])

    st.subheader("対局データ一覧")
    for _, session_row in sessions_df.iterrows():
        title_text = "タイトルなし" if pd.isna(session_row["title"]) or session_row["title"] == "" else str(session_row["title"])
        button_label = "{} | {} | {}".format(
            session_row["session_date"],
            title_text,
            session_row["rule_name"],
        )
        if st.button(button_label, key="day_session_btn_{}".format(int(session_row["id"]))):
            st.session_state["day_stats_session_id"] = int(session_row["id"])
            st.rerun()

    session_id = int(st.session_state["day_stats_session_id"])
    session_detail_df = get_session_detail(session_id)

    st.divider()
    if not session_detail_df.empty:
        row = session_detail_df.iloc[0]
        st.markdown(
            "**日付:** {}  \n**タイトル:** {}  \n**ルール:** {}".format(
                row["session_date"],
                "タイトルなし" if pd.isna(row["title"]) or row["title"] == "" else row["title"],
                row["rule_name"],
            )
        )

    totals_df = get_session_player_totals(session_id)
    st.subheader("その日の個人成績")
    if totals_df.empty:
        st.info("まだ半荘データがありません。")
    else:
        for src, dst in [
            ("first_count", "1着率"),
            ("second_count", "2着率"),
            ("third_count", "3着率"),
            ("fourth_count", "4着率"),
        ]:
            totals_df[dst] = totals_df.apply(
                lambda row: round((row[src] / row["games"] * 100), 1) if row["games"] else 0,
                axis=1,
            )

        display_totals = totals_df.rename(
            columns={
                "name": "プレイヤー",
                "games": "半荘数",
                "first_count": "1着",
                "second_count": "2着",
                "third_count": "3着",
                "fourth_count": "4着",
                "avg_rank": "平均順位",
                "total_settlement": "収支",
            }
        )
        st.dataframe(
            display_totals[
                ["プレイヤー", "半荘数", "1着", "2着", "3着", "4着", "1着率", "2着率", "3着率", "4着率", "平均順位", "収支"]
            ],
            use_container_width=True,
            hide_index=True,
        )
        st.caption("その日の精算合計: {:.1f}".format(round(display_totals["収支"].sum(), 1)))

    st.subheader("その日の順位グラフ")
    rank_trend_df = get_session_rank_trend(session_id)
    if rank_trend_df.empty:
        st.info("グラフにするデータがありません。")
    else:
        fig = make_rank_line_chart(rank_trend_df, "その日の順位グラフ")
        st.plotly_chart(fig, use_container_width=True)

    results_df = get_hanchan_results(session_id)
    st.subheader("半荘ごとの結果")
    if results_df.empty:
        st.info("まだ半荘データがありません。")
    else:
        display_results = results_df[["hanchan_no", "name", "final_score", "rank", "settlement"]].copy()
        display_results.columns = ["半荘", "プレイヤー", "点数", "順位", "精算"]
        st.dataframe(display_results, use_container_width=True, hide_index=True)


def page_stats_total():
    st.title("トータル成績")

    players_df = get_players()
    col1, col2, col3 = st.columns(3)
    with col1:
        selected_player = st.selectbox(
            "プレイヤー",
            options=["全員"] + players_df["name"].tolist() if not players_df.empty else ["全員"],
        )
    with col2:
        start_date = st.date_input("開始日", value=None)
    with col3:
        end_date = st.date_input("終了日", value=None)

    player_filter = None if selected_player == "全員" else selected_player
    start_filter = str(start_date) if start_date else None
    end_filter = str(end_date) if end_date else None

    stats_df = get_player_stats(player_filter, start_filter, end_filter)
    if stats_df.empty:
        st.info("まだ成績データがありません。")
        return

    for src, dst in [
        ("first_count", "1着率"),
        ("second_count", "2着率"),
        ("third_count", "3着率"),
        ("fourth_count", "4着率"),
    ]:
        stats_df[dst] = stats_df.apply(
            lambda row: round((row[src] / row["games"] * 100), 1) if row["games"] else 0,
            axis=1,
        )

    display_stats = stats_df.rename(
        columns={
            "name": "プレイヤー",
            "games": "半荘数",
            "first_count": "1着",
            "second_count": "2着",
            "third_count": "3着",
            "fourth_count": "4着",
            "avg_rank": "平均順位",
            "total_settlement": "収支",
        }
    )
    st.dataframe(
        display_stats[
            ["プレイヤー", "半荘数", "1着", "2着", "3着", "4着", "1着率", "2着率", "3着率", "4着率", "平均順位", "収支"]
        ],
        use_container_width=True,
        hide_index=True,
    )


def main():
    st.set_page_config(
        page_title="麻雀成績管理サイト",
        page_icon="🀄",
        layout="centered",
        initial_sidebar_state="collapsed",
    )

    init_db()

    menu = st.sidebar.radio(
        "メニュー",
        ["ホーム", "プレイヤー登録", "ルール登録", "対局データ入力", "その日の成績", "トータル成績"],
    )

    if menu == "ホーム":
        page_home()
    elif menu == "プレイヤー登録":
        page_players()
    elif menu == "ルール登録":
        page_rules()
    elif menu == "対局データ入力":
        page_session_input()
    elif menu == "その日の成績":
        page_day_stats()
    elif menu == "トータル成績":
        page_stats_total()


if __name__ == "__main__":
    main()