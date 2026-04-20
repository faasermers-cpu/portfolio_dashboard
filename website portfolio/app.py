"""
Portfolio Dashboard – publieke versie met DEGIRO CSV-upload.

Gebruik:
    pip install -r requirements.txt
    python app.py
    Open http://127.0.0.1:8050

Deployment (Render.com):
    Zie render.yaml in dezelfde map.
"""

import json
import sys
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

try:
    import yfinance as yf
except ImportError:
    print("pip install yfinance"); sys.exit(1)

try:
    import dash
    from dash import dcc, html, ctx
    from dash.dependencies import Input, Output, State
    import plotly.graph_objects as go
    import pandas as pd
except ImportError:
    print("pip install dash plotly pandas"); sys.exit(1)

from degiro_parser import parse_degiro_csv

# ──────────────────────────────────────────────────────────────────────────────
# Constanten
# ──────────────────────────────────────────────────────────────────────────────
KLEUREN = [
    "#2196F3", "#4CAF50", "#FF9800", "#E91E63", "#9C27B0",
    "#00BCD4", "#FF5722", "#607D8B", "#8BC34A", "#FFC107",
]
KOOP_KLEUR, VERKOOP_KLEUR = "#2E7D32", "#C62828"

BENCHMARKS = {
    "IWDA.AS": {"naam": "MSCI World",         "kleur": "#888888"},
    "VUSA.AS": {"naam": "S&P 500 (Vanguard)", "kleur": "#FF9800"},
    "EQQQ.DE": {"naam": "NASDAQ-100 (EQQQ)", "kleur": "#9C27B0"},
    "VWRL.AS": {"naam": "FTSE All-World",     "kleur": "#00BCD4"},
    "CSPX.AS": {"naam": "S&P 500 (iShares)", "kleur": "#FF5722"},
}

SECTION = {
    "backgroundColor": "white",
    "borderRadius": "8px",
    "padding": "5px",
    "boxShadow": "0 1px 3px rgba(0,0,0,0.08)",
    "marginBottom": "12px",
}


# ──────────────────────────────────────────────────────────────────────────────
# Dataophaling
# ──────────────────────────────────────────────────────────────────────────────

def haal_koersen_op(posities: list) -> dict:
    resultaat = {}
    eind = datetime.now()
    bench_posities = [
        {"ticker": t, "eerste_aankoop": "2020-01-01"} for t in BENCHMARKS
    ]
    for p in posities + bench_posities:
        ticker = p["ticker"]
        if ticker in resultaat:
            continue
        eerste = p.get("eerste_aankoop")
        start = (
            datetime.strptime(eerste, "%Y-%m-%d") - timedelta(days=7)
            if eerste
            else eind - timedelta(days=90)
        )
        try:
            hist = yf.Ticker(ticker).history(
                start=start.strftime("%Y-%m-%d"),
                end=eind.strftime("%Y-%m-%d"),
            )
            if hist.empty:
                resultaat[ticker] = {"huidige_koers": 0, "df": pd.DataFrame(),
                                     "dividenden": pd.Series(dtype=float)}
                continue
            df = hist[["Close"]].copy()
            df.index = df.index.tz_localize(None)
            df = df.rename(columns={"Close": "koers"})
            divs = hist["Dividends"] if "Dividends" in hist.columns else pd.Series(dtype=float)
            resultaat[ticker] = {
                "huidige_koers": round(float(df["koers"].iloc[-1]), 2),
                "df": df,
                "dividenden": divs[divs > 0] if len(divs) > 0 else pd.Series(dtype=float),
            }
        except Exception as e:
            print(f"  FOUT {ticker}: {e}")
            resultaat[ticker] = {"huidige_koers": 0, "df": pd.DataFrame(),
                                 "dividenden": pd.Series(dtype=float)}
    return resultaat


def bereken_resultaten(posities: list, koersen: dict) -> list:
    res = []
    for p in posities:
        kd = koersen.get(p["ticker"], {})
        h  = kd.get("huidige_koers", 0)
        w  = p["aantal"] * h
        inv = p["aantal"] * p["aankoopprijs"]
        wv  = w - inv
        r   = ((h / p["aankoopprijs"]) - 1) * 100 if p["aankoopprijs"] > 0 else 0
        res.append({
            **p,
            "huidige_koers":  h,
            "huidige_waarde": round(w, 2),
            "investering":    round(inv, 2),
            "winst_verlies":  round(wv, 2),
            "rendement_pct":  round(r, 2),
            "df":             kd.get("df", pd.DataFrame()),
            "dividenden":     kd.get("dividenden", pd.Series(dtype=float)),
        })
    return res


def filter_periode(df, periode):
    if df.empty or periode == "MAX":
        return df
    dagen = {"1M": 30, "3M": 90, "6M": 180, "1J": 365, "2J": 730}.get(periode, 9999)
    return df[df.index >= df.index.max() - timedelta(days=dagen)]


# ──────────────────────────────────────────────────────────────────────────────
# Serialisatie (dcc.Store slaat alleen JSON op)
# ──────────────────────────────────────────────────────────────────────────────

def serialiseer(koersen: dict) -> dict:
    serial = {}
    for t, kd in koersen.items():
        df   = kd.get("df", pd.DataFrame())
        divs = kd.get("dividenden", pd.Series(dtype=float))
        div_data = (
            {"datums": divs.index.strftime("%Y-%m-%d").tolist(), "bedragen": divs.tolist()}
            if len(divs) > 0 else {"datums": [], "bedragen": []}
        )
        if not df.empty:
            serial[t] = {
                "huidige_koers": kd["huidige_koers"],
                "datums":  df.index.strftime("%Y-%m-%d").tolist(),
                "koersen": df["koers"].tolist(),
                "dividenden": div_data,
            }
        else:
            serial[t] = {"huidige_koers": 0, "datums": [], "koersen": [], "dividenden": div_data}
    return serial


def deserialiseer(serial: dict) -> dict:
    koersen = {}
    for t, kd in serial.items():
        if kd["datums"]:
            df = pd.DataFrame({"koers": kd["koersen"]}, index=pd.to_datetime(kd["datums"]))
            div_d = kd.get("dividenden", {"datums": [], "bedragen": []})
            divs  = (
                pd.Series(div_d["bedragen"], index=pd.to_datetime(div_d["datums"]))
                if div_d["datums"] else pd.Series(dtype=float)
            )
            koersen[t] = {"huidige_koers": kd["huidige_koers"], "df": df, "dividenden": divs}
        else:
            koersen[t] = {"huidige_koers": 0, "df": pd.DataFrame(), "dividenden": pd.Series(dtype=float)}
    return koersen


# ──────────────────────────────────────────────────────────────────────────────
# Grafiek-bouwers
# ──────────────────────────────────────────────────────────────────────────────

def _layout(fig, titel, hoogte=350, leg_onder=True, y_titel=None):
    leg = (
        dict(orientation="h", y=-0.18, font=dict(size=10))
        if leg_onder else dict(font=dict(size=10))
    )
    layout = dict(
        title=titel, template="plotly_white", height=hoogte,
        margin=dict(l=50, r=20, t=50, b=40), legend=leg,
    )
    if y_titel:
        layout["yaxis"] = dict(title=y_titel, tickformat=".2f")
    fig.update_layout(**layout)
    return fig


def maak_koersgrafiek(resultaten, titel, periode="MAX"):
    fig = go.Figure()
    for i, r in enumerate(resultaten):
        df = filter_periode(r["df"], periode)
        if df.empty:
            continue
        basis = df["koers"].iloc[0]
        if basis == 0:
            continue
        genorm = ((df["koers"] / basis) - 1) * 100
        fig.add_trace(go.Scatter(
            x=df.index, y=genorm.round(2), mode="lines", name=r["naam"],
            line=dict(color=KLEUREN[i % len(KLEUREN)], width=2),
            hovertemplate=(
                "<b>%{fullData.name}</b><br>Datum: %{x|%d-%m-%Y}<br>"
                "Koers: €%{customdata:.2f}<br>Verandering: %{y:+.2f}%<extra></extra>"
            ),
            customdata=df["koers"].round(2),
        ))
        for tx in r.get("transacties", []):
            tx_d = pd.Timestamp(tx["datum"])
            if tx_d < df.index.min() or tx_d > df.index.max():
                continue
            idx = df.index.get_indexer([tx_d], method="nearest")[0]
            if idx < 0 or idx >= len(df):
                continue
            y   = (df["koers"].iloc[idx] / basis - 1) * 100
            koop = tx["type"] == "koop"
            fig.add_trace(go.Scatter(
                x=[df.index[idx]], y=[round(y, 2)], mode="markers",
                marker=dict(
                    symbol="triangle-up" if koop else "triangle-down",
                    size=12,
                    color=KOOP_KLEUR if koop else VERKOOP_KLEUR,
                    line=dict(width=1, color="white"),
                ),
                hovertemplate=(
                    f"<b>{'Koop' if koop else 'Verkoop'} {tx['aantal']}x</b><br>"
                    f"Datum: {tx['datum']}<br>Koers: €{tx['koers']:.2f}<br>"
                    f"Totaal: €{tx['aantal'] * tx['koers']:,.2f}<extra></extra>"
                ),
                showlegend=False,
            ))
    fig.add_hline(y=0, line_dash="dash", line_color="#888", line_width=0.5)
    return _layout(fig, titel, y_titel="Rendement (%)")


def maak_taartdiagram(resultaten):
    namen   = [r["naam"]          for r in resultaten]
    waarden = [r["huidige_waarde"] for r in resultaten]
    totaal  = sum(waarden)
    fig = go.Figure(go.Pie(
        labels=namen, values=waarden,
        marker=dict(colors=KLEUREN[: len(resultaten)]),
        textinfo="percent", textposition="inside", hole=0.35,
        hovertemplate="<b>%{label}</b><br>Waarde: €%{value:,.2f}<br>Aandeel: %{percent}<extra></extra>",
    ))
    fig.update_layout(
        title=f"ETF-verdeling (totaal: €{totaal:,.2f})",
        template="plotly_white", height=350,
        margin=dict(l=20, r=20, t=50, b=20),
        legend=dict(orientation="v", x=1.02, y=0.5, font=dict(size=11)),
    )
    return fig


def maak_rendement_staaf(resultaten, titel="Rendement per positie"):
    # Posities zonder live koers weglaten (huidige_koers = 0)
    resultaten = [r for r in resultaten if r.get("huidige_koers", 0) > 0]
    if not resultaten:
        fig = go.Figure()
        fig.add_annotation(text="Geen koersdata beschikbaar", xref="paper", yref="paper",
                           x=0.5, y=0.5, showarrow=False, font=dict(size=14, color="#aaa"))
        return _layout(fig, titel)
    namen = [r["naam"]          for r in resultaten]
    rends = [r["rendement_pct"] for r in resultaten]
    wvs   = [r["winst_verlies"] for r in resultaten]
    fig = go.Figure(go.Bar(
        y=namen, x=rends, orientation="h",
        marker=dict(color=[KOOP_KLEUR if r >= 0 else VERKOOP_KLEUR for r in rends]),
        text=[f"{r:+.1f}% (€{w:+,.2f})" for r, w in zip(rends, wvs)],
        textposition="outside", textfont=dict(size=10),
        hovertemplate="<b>%{y}</b><br>Rendement: %{x:+.1f}%<extra></extra>",
    ))
    fig.add_vline(x=0, line_color="#888", line_width=0.8)
    fig.update_layout(
        title=titel, template="plotly_white", height=350,
        margin=dict(l=10, r=80, t=50, b=40),
        xaxis=dict(title="Rendement (%)"), yaxis=dict(automargin=True),
    )
    return fig


def maak_gesloten_posities(gesloten):
    if not gesloten:
        fig = go.Figure()
        fig.add_annotation(text="Geen gesloten posities", xref="paper", yref="paper",
                           x=0.5, y=0.5, showarrow=False, font=dict(size=14, color="#888"))
        return _layout(fig, "Gesloten posities")
    namen   = [g["naam"] for g in gesloten]
    winsten = [(g["verkoop_koers"] - g["koop_koers"]) * g["aantal"] for g in gesloten]
    rends   = [((g["verkoop_koers"] / g["koop_koers"]) - 1) * 100 for g in gesloten]
    periodes = [f"{g['koop_datum'][:7]} → {g['verkoop_datum'][:7]}" for g in gesloten]
    fig = go.Figure(go.Bar(
        y=namen, x=winsten, orientation="h",
        marker=dict(color=[KOOP_KLEUR if w >= 0 else VERKOOP_KLEUR for w in winsten]),
        text=[f"€{w:+,.2f} ({r:+.1f}%)" for w, r in zip(winsten, rends)],
        textposition="outside", textfont=dict(size=10),
        hovertemplate=[
            f"<b>{n}</b><br>Winst: €{w:+,.2f}<br>Rendement: {r:+.1f}%<br>"
            f"Periode: {p}<extra></extra>"
            for n, w, r, p in zip(namen, winsten, rends, periodes)
        ],
    ))
    fig.add_vline(x=0, line_color="#888", line_width=0.8)
    fig.update_layout(
        title="Gesloten posities", template="plotly_white", height=350,
        margin=dict(l=10, r=80, t=50, b=40),
        xaxis=dict(title="Winst/Verlies (€)"), yaxis=dict(automargin=True),
    )
    return fig


def maak_portfolio_historie(alle_posities, koersen, periode="MAX"):
    ticker_txs = {}
    koers_dfs  = {}
    for p in alle_posities:
        ticker = p["ticker"]
        df = koersen.get(ticker, {}).get("df", pd.DataFrame())
        if not df.empty:
            koers_dfs[ticker] = df
        txs = sorted(p.get("transacties", []), key=lambda t: t["datum"])
        cumul = 0
        tijdlijn = []
        for tx in txs:
            cumul += tx["aantal"] if tx["type"] == "koop" else -tx["aantal"]
            tijdlijn.append((pd.Timestamp(tx["datum"]).date(), cumul))
        ticker_txs[ticker] = tijdlijn

    if not koers_dfs:
        fig = go.Figure()
        fig.add_annotation(text="Geen data", xref="paper", yref="paper",
                           x=0.5, y=0.5, showarrow=False)
        return fig

    def aantal_op_dag(ticker, dag):
        n = 0
        for tx_datum, tx_cumul in ticker_txs.get(ticker, []):
            if tx_datum <= dag:
                n = tx_cumul
            else:
                break
        return n

    alle_datums = sorted(set().union(*[set(df.index.date) for df in koers_dfs.values()]))
    totale_waarde, inleg_list = [], []

    alle_tx = []
    for p in alle_posities:
        for tx in p.get("transacties", []):
            b = tx["aantal"] * tx["koers"]
            if b > 0:
                alle_tx.append({"datum": pd.Timestamp(tx["datum"]).date(), "bedrag": b})
    alle_tx.sort(key=lambda x: x["datum"])

    cumul_inleg, ti = 0.0, 0
    for dag in alle_datums:
        while ti < len(alle_tx) and alle_tx[ti]["datum"] <= dag:
            cumul_inleg += alle_tx[ti]["bedrag"]
            ti += 1
        inleg_list.append(cumul_inleg)
        dw = sum(
            koers_dfs[t].loc[koers_dfs[t].index.date <= dag, "koers"].iloc[-1]
            * aantal_op_dag(t, dag)
            for t in koers_dfs
            if (koers_dfs[t].index.date <= dag).any()
        )
        totale_waarde.append(dw)

    wv = [w - i for w, i in zip(totale_waarde, inleg_list)]
    df_h = pd.DataFrame(
        {"waarde": totale_waarde, "inleg": inleg_list, "wv": wv},
        index=pd.to_datetime(alle_datums),
    )
    df_h = filter_periode(df_h, periode)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_h.index, y=df_h["waarde"], mode="lines", name="Marktwaarde posities",
        line=dict(color="#2196F3", width=2.5), fill="tozeroy",
        fillcolor="rgba(33,150,243,0.15)",
        hovertemplate="<b>Marktwaarde posities</b><br>%{x|%d-%m-%Y}<br>€%{y:,.2f}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=df_h.index, y=df_h["inleg"], mode="lines",
        name=f"Geïnvesteerd (€{df_h['inleg'].iloc[-1]:,.2f})",
        line=dict(color="#E91E63", width=2, dash="dash"),
        hovertemplate="<b>Geïnvesteerd</b><br>%{x|%d-%m-%Y}<br>€%{y:,.2f}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=df_h.index, y=df_h["wv"], mode="lines",
        name=f"Winst/verlies (€{df_h['wv'].iloc[-1]:+,.2f})",
        line=dict(color="#4CAF50", width=1.8),
        hovertemplate="<b>Winst/verlies</b><br>%{x|%d-%m-%Y}<br>€%{y:+,.2f}<extra></extra>",
    ))
    fig.add_hline(y=0, line_color="#888", line_width=0.5)
    return _layout(fig, "Portfoliowaarde over tijd", 400, y_titel="Waarde (€)")


def maak_benchmark(alle_posities, koersen, periode="MAX", benchmarks=None):
    if benchmarks is None:
        benchmarks = ["IWDA.AS"]
    fig = go.Figure()

    pos_data = [
        {"df": koersen[p["ticker"]]["df"],
         "waarde": p["aantal"] * koersen[p["ticker"]]["huidige_koers"],
         "ticker": p["ticker"]}
        for p in alle_posities
        if p["ticker"] in koersen and not koersen[p["ticker"]]["df"].empty
    ]

    if pos_data:
        totaal = sum(pd["waarde"] for pd in pos_data)
        if totaal > 0:
            start = max(pd["df"].index.min() for pd in pos_data)
            gewogen = []
            for pd_item in pos_data:
                df_f = pd_item["df"][pd_item["df"].index >= start]
                if df_f.empty:
                    continue
                w = pd_item["waarde"] / totaal
                basis = df_f["koers"].iloc[0]
                if basis > 0:
                    gewogen.append(((df_f["koers"] / basis) - 1) * 100 * w)
            if gewogen:
                combined = pd.concat(gewogen, axis=1).ffill().fillna(0)
                port_idx  = filter_periode(combined.sum(axis=1).round(2).to_frame("r"), periode)["r"]
                fig.add_trace(go.Scatter(
                    x=port_idx.index, y=port_idx, mode="lines",
                    name="Jouw portfolio (gewogen)", line=dict(color="#2196F3", width=3),
                    hovertemplate="<b>Jouw portfolio</b><br>%{x|%d-%m-%Y}<br>%{y:+.2f}%<extra></extra>",
                ))
                for bench_t in benchmarks:
                    if bench_t not in BENCHMARKS:
                        continue
                    bdf = koersen.get(bench_t, {}).get("df", pd.DataFrame())
                    if bdf.empty:
                        continue
                    bf = filter_periode(bdf, periode)
                    bf = bf[bf.index >= port_idx.index.min()]
                    if bf.empty:
                        continue
                    bas = bf["koers"].iloc[0]
                    if bas > 0:
                        rend = (((bf["koers"] / bas) - 1) * 100).round(2)
                        fig.add_trace(go.Scatter(
                            x=bf.index, y=rend, mode="lines",
                            name=BENCHMARKS[bench_t]["naam"],
                            line=dict(color=BENCHMARKS[bench_t]["kleur"], width=2, dash="dot"),
                            hovertemplate=f"<b>{BENCHMARKS[bench_t]['naam']}</b><br>%{{x|%d-%m-%Y}}<br>%{{y:+.2f}}%<extra></extra>",
                        ))

    fig.add_hline(y=0, line_dash="dash", line_color="#888", line_width=0.5)
    fig.update_layout(yaxis=dict(tickformat=".2f"))
    return _layout(fig, "Jouw portfolio vs benchmarks", 400, y_titel="Rendement (%)")


def maak_dividend_tracker(alle_resultaten):
    alle_divs = []
    for r in alle_resultaten:
        divs = r.get("dividenden", pd.Series(dtype=float))
        if isinstance(divs, pd.Series) and len(divs) > 0:
            for datum, bedrag in divs.items():
                dc = datum.tz_localize(None) if datum.tzinfo else datum
                alle_divs.append({
                    "maand": dc.strftime("%Y-%m"),
                    "bedrag": bedrag * r["aantal"],
                    "naam":  r["naam"],
                })
    if not alle_divs:
        fig = go.Figure()
        fig.add_annotation(text="Geen dividenddata gevonden", xref="paper", yref="paper",
                           x=0.5, y=0.5, showarrow=False, font=dict(size=14, color="#888"))
        return _layout(fig, "Ontvangen dividenden")
    df_div = pd.DataFrame(alle_divs)
    mt = df_div.groupby("maand")["bedrag"].sum().reset_index().sort_values("maand")
    fig = go.Figure()
    for i, naam in enumerate(df_div["naam"].unique()):
        sub = df_div[df_div["naam"] == naam].groupby("maand")["bedrag"].sum().reset_index()
        fig.add_trace(go.Bar(
            x=sub["maand"], y=sub["bedrag"], name=naam,
            marker=dict(color=KLEUREN[i % len(KLEUREN)]),
            hovertemplate=f"<b>{naam}</b><br>Maand: %{{x}}<br>€%{{y:.2f}}<extra></extra>",
        ))
    cum = mt["bedrag"].cumsum()
    fig.add_trace(go.Scatter(
        x=mt["maand"], y=cum, mode="lines+markers",
        name=f"Cumulatief (€{cum.iloc[-1]:.2f})", yaxis="y2",
        line=dict(color="#333", width=2),
        hovertemplate="<b>Cumulatief</b><br>%{x}<br>€%{y:.2f}<extra></extra>",
    ))
    fig.update_layout(
        title="Ontvangen dividenden per maand", template="plotly_white",
        height=380, margin=dict(l=50, r=50, t=50, b=40), barmode="stack",
        legend=dict(orientation="h", y=-0.22, font=dict(size=10)),
        xaxis=dict(title=""), yaxis=dict(title="Dividend (€)"),
        yaxis2=dict(title="Cumulatief (€)", overlaying="y", side="right"),
    )
    return fig


def maak_concentratie(alle_resultaten):
    gesorteerd = sorted(alle_resultaten, key=lambda x: x["huidige_waarde"], reverse=True)
    namen   = [r["naam"]          for r in gesorteerd]
    waarden = [r["huidige_waarde"] for r in gesorteerd]
    totaal  = sum(waarden)
    if totaal == 0:
        fig = go.Figure()
        fig.add_annotation(text="Geen data", xref="paper", yref="paper",
                           x=0.5, y=0.5, showarrow=False)
        return _layout(fig, "Concentratierisico")
    pcts     = [w / totaal * 100 for w in waarden]
    cum_pcts = [sum(pcts[: i + 1]) for i in range(len(pcts))]
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=namen, y=pcts, name="Aandeel (%)",
        marker=dict(color=KLEUREN[: len(namen)]),
        text=[f"{p:.1f}%" for p in pcts], textposition="outside", textfont=dict(size=10),
        hovertemplate="<b>%{x}</b><br>Aandeel: %{y:.1f}%<br>Waarde: €%{customdata:,.2f}<extra></extra>",
        customdata=waarden,
    ))
    fig.add_trace(go.Scatter(
        x=namen, y=cum_pcts, mode="lines+markers", name="Cumulatief",
        line=dict(color="#333", width=2), marker=dict(size=6), yaxis="y2",
        hovertemplate="<b>Cumulatief</b><br>%{x}<br>%{y:.1f}%<extra></extra>",
    ))
    fig.add_hline(y=50, line_dash="dash", line_color="#FF9800", line_width=1,
                  annotation_text="50%", annotation_position="top right", yref="y2")
    fig.update_layout(
        title="Concentratierisico", template="plotly_white", height=380,
        margin=dict(l=50, r=50, t=50, b=80),
        legend=dict(orientation="h", y=-0.28, font=dict(size=10)),
        xaxis=dict(tickangle=-30),
        yaxis=dict(title="Aandeel (%)"),
        yaxis2=dict(title="Cumulatief (%)", overlaying="y", side="right", range=[0, 105]),
    )
    return fig


def maak_heatmap(alle_posities, koersen):
    ticker_txs = {}
    koers_dfs  = {}
    for p in alle_posities:
        ticker = p["ticker"]
        df = koersen.get(ticker, {}).get("df", pd.DataFrame())
        if not df.empty:
            koers_dfs[ticker] = df
        txs = sorted(p.get("transacties", []), key=lambda t: t["datum"])
        cumul, tijdlijn = 0, []
        for tx in txs:
            cumul += tx["aantal"] if tx["type"] == "koop" else -tx["aantal"]
            tijdlijn.append((pd.Timestamp(tx["datum"]).date(), cumul))
        ticker_txs[ticker] = tijdlijn

    if not koers_dfs:
        return go.Figure()

    def n_op_dag(ticker, dag):
        n = 0
        for d, c in ticker_txs.get(ticker, []):
            if d <= dag:
                n = c
            else:
                break
        return n

    alle_datums = sorted(set().union(*[set(df.index.date) for df in koers_dfs.values()]))
    alle_tx = []
    for p in alle_posities:
        for tx in p.get("transacties", []):
            b = tx["aantal"] * tx["koers"]
            if b > 0:
                alle_tx.append({"datum": pd.Timestamp(tx["datum"]).date(), "bedrag": b})
    alle_tx.sort(key=lambda x: x["datum"])

    waarden, inleg_l, cumul_inleg, ti = [], [], 0.0, 0
    for dag in alle_datums:
        while ti < len(alle_tx) and alle_tx[ti]["datum"] <= dag:
            cumul_inleg += alle_tx[ti]["bedrag"]
            ti += 1
        inleg_l.append(cumul_inleg)
        dw = sum(
            koers_dfs[t].loc[koers_dfs[t].index.date <= dag, "koers"].iloc[-1]
            * n_op_dag(t, dag)
            for t in koers_dfs
            if (koers_dfs[t].index.date <= dag).any()
        )
        waarden.append(dw)

    df_w = pd.DataFrame({"waarde": waarden, "inleg": inleg_l}, index=pd.to_datetime(alle_datums))
    df_w["netto"] = df_w["waarde"] - df_w["inleg"]
    maand_eind = df_w.resample("ME").last()
    rend_maand = []
    for i in range(1, len(maand_eind)):
        dn = maand_eind["netto"].iloc[i] - maand_eind["netto"].iloc[i - 1]
        il = maand_eind["inleg"].iloc[i]
        rend_maand.append(dn / il * 100 if il > 0 else 0)

    series = pd.Series(rend_maand, index=maand_eind.index[1:]).dropna()
    if len(series) == 0:
        fig = go.Figure()
        fig.add_annotation(text="Onvoldoende data", xref="paper", yref="paper",
                           x=0.5, y=0.5, showarrow=False)
        return _layout(fig, "Maandelijks rendement")

    df_r = pd.DataFrame({"r": series})
    df_r["jaar"]  = df_r.index.year
    df_r["maand"] = df_r.index.month
    jaren  = sorted(df_r["jaar"].unique())
    mnamen = ["Jan","Feb","Mrt","Apr","Mei","Jun","Jul","Aug","Sep","Okt","Nov","Dec"]

    z, text = [], []
    for jaar in jaren:
        rij, rtxt = [], []
        for m in range(1, 13):
            val = df_r[(df_r["jaar"] == jaar) & (df_r["maand"] == m)]["r"]
            if len(val):
                v = round(val.iloc[0], 1)
                rij.append(v)
                rtxt.append(f"{v:+.1f}%")
            else:
                rij.append(None)
                rtxt.append("")
        z.append(rij)
        text.append(rtxt)

    fig = go.Figure(go.Heatmap(
        z=z, x=mnamen, y=[str(j) for j in jaren],
        text=text, texttemplate="%{text}", textfont=dict(size=11),
        colorscale=[[0, "#E91E63"], [0.5, "#FFFFFF"], [1, "#4CAF50"]],
        zmid=0, zmin=-15, zmax=15, showscale=True,
        colorbar=dict(title=dict(text="Rendement (%)", side="right")),
        hovertemplate="<b>%{y} %{x}</b><br>Rendement: %{text}<extra></extra>",
    ))
    fig.update_layout(
        title="Maandelijks portfoliorendement (%)", template="plotly_white",
        height=300, margin=dict(l=50, r=20, t=50, b=40),
        yaxis=dict(autorange="reversed"),
    )
    return fig


# ──────────────────────────────────────────────────────────────────────────────
# Lege figuur (vóór data)
# ──────────────────────────────────────────────────────────────────────────────

def leeg_fig(tekst="Upload je DEGIRO-export om te beginnen", hoogte=350):
    fig = go.Figure()
    fig.add_annotation(
        text=tekst, xref="paper", yref="paper",
        x=0.5, y=0.5, showarrow=False, font=dict(size=14, color="#aaa"),
    )
    fig.update_layout(template="plotly_white", height=hoogte,
                      margin=dict(l=20, r=20, t=50, b=40))
    return fig


# ──────────────────────────────────────────────────────────────────────────────
# App-layout
# ──────────────────────────────────────────────────────────────────────────────

# ── Google Analytics ──────────────────────────────────────────────────────────
# Vervang 'G-XXXXXXXXXX' door jouw eigen GA4 Measurement ID
# (te vinden op analytics.google.com → Admin → Data Streams → jouw stream)
GA4_ID = G-5EN7C1LQ6B

GA4_SCRIPT = f"""
window.dataLayer = window.dataLayer || [];
function gtag(){{dataLayer.push(arguments);}}
gtag('js', new Date());
gtag('config', '{GA4_ID}');
"""

app = dash.Dash(
    __name__,
    title="Portfolio Dashboard",
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
    external_scripts=[
        {"src": f"https://www.googletagmanager.com/gtag/js?id={GA4_ID}", "async": True},
    ],
    index_string="""<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
        <script>""" + GA4_SCRIPT + """</script>
    </body>
</html>""",
)
server = app.server  # Voor deployment (Gunicorn / Render)

UPLOAD_STIJL = {
    "width": "100%", "maxWidth": "560px", "margin": "0 auto",
    "height": "140px", "lineHeight": "140px",
    "borderWidth": "2px", "borderStyle": "dashed",
    "borderRadius": "10px", "borderColor": "#aaa",
    "textAlign": "center", "color": "#888",
    "cursor": "pointer", "backgroundColor": "#fafafa",
    "fontSize": "15px",
}

app.layout = html.Div(
    style={"fontFamily": "'Segoe UI', Arial, sans-serif",
           "maxWidth": "1400px", "margin": "0 auto",
           "padding": "20px", "backgroundColor": "#F5F5F5"},
    children=[
        # ── Header ────────────────────────────────────────────────────────────
        html.Div(style={"textAlign": "center", "marginBottom": "20px"}, children=[
            html.H1("📈 Portfolio Dashboard",
                    style={"color": "#333", "marginBottom": "6px", "fontSize": "28px"}),
            html.P("Upload je DEGIRO-transactie-export en bekijk je volledige portfolioanalyse.",
                   style={"color": "#888", "fontSize": "14px"}),
        ]),

        # ── Upload-sectie ──────────────────────────────────────────────────────
        html.Div(id="upload-sectie", style={**SECTION, "padding": "24px"}, children=[
            html.H3("Stap 1 – Upload je DEGIRO-transacties",
                    style={"textAlign": "center", "color": "#555", "marginBottom": "12px"}),
            html.P(
                "Download via DEGIRO → Activiteit → Exporteren → Transacties (CSV). "
                "Sleep het bestand hieronder of klik om te selecteren.",
                style={"textAlign": "center", "color": "#888", "fontSize": "13px", "marginBottom": "16px"},
            ),
            dcc.Upload(
                id="upload-csv",
                children=html.Div(["📂  Sleep CSV hier naartoe  —  of  ", html.U("klik om te selecteren")]),
                style=UPLOAD_STIJL,
                accept=".csv",
            ),
            html.Div(id="upload-status",
                     style={"textAlign": "center", "marginTop": "12px",
                            "fontSize": "13px", "color": "#555"}),
        ]),

        # ── Controls (verborgen tot data geladen) ──────────────────────────────
        html.Div(id="controls-sectie",
                 style={"display": "none", "textAlign": "center",
                        "marginBottom": "15px", "flexWrap": "wrap",
                        "justifyContent": "center", "gap": "10px",
                        "alignItems": "center"},
                 children=[
            html.Button(
                "🔄  Ververs koersen", id="ververs-btn",
                style={"padding": "10px 24px", "backgroundColor": "#2196F3",
                       "color": "white", "border": "none", "borderRadius": "6px",
                       "fontSize": "14px", "cursor": "pointer", "fontWeight": "bold"},
            ),
            dcc.Loading(id="loading", type="circle", children=html.Div(id="loading-output")),
            html.Span("Periode:", style={"marginLeft": "20px", "color": "#666"}),
            dcc.RadioItems(
                id="periode-keuze",
                options=[{"label": p, "value": p} for p in ["1M", "3M", "6M", "1J", "2J", "MAX"]],
                value="MAX", inline=True,
                style={"display": "inline-flex", "gap": "4px"},
                inputStyle={"marginRight": "3px"},
                labelStyle={"padding": "5px 10px", "backgroundColor": "#E0E0E0",
                            "borderRadius": "4px", "fontSize": "12px", "cursor": "pointer"},
            ),
            html.P(id="status-tekst",
                   style={"color": "#aaa", "fontSize": "12px", "margin": "4px 0 0"}),
        ]),

        # ── Overzichtskaarten ──────────────────────────────────────────────────
        html.Div(id="totaal-overzicht",
                 style={"display": "flex", "justifyContent": "center",
                        "gap": "20px", "marginBottom": "15px", "flexWrap": "wrap"}),

        # ── Ticket-waarschuwingen ──────────────────────────────────────────────
        html.Div(id="ticker-waarschuwing"),

        # ── Grafieken ──────────────────────────────────────────────────────────
        html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr 1fr",
                        "gap": "10px", **SECTION}, children=[
            dcc.Graph(id="taart-etf",        config={"displayModeBar": False}),
            dcc.Graph(id="rendement-etf",    config={"displayModeBar": False}),
            dcc.Graph(id="gesloten-posities",config={"displayModeBar": False}),
        ]),
        html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr",
                        "gap": "10px", **SECTION}, children=[
            dcc.Graph(id="koers-etf",      config={"scrollZoom": True}),
            dcc.Graph(id="koers-aandelen", config={"scrollZoom": True}),
        ]),
        html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr",
                        "gap": "10px", **SECTION}, children=[
            dcc.Graph(id="portfolio-historie", config={"scrollZoom": True}),
            html.Div(children=[
                html.Div(style={"padding": "5px 10px", "display": "flex",
                                "flexWrap": "wrap", "gap": "5px", "alignItems": "center"}, children=[
                    html.Span("Vergelijk met:", style={"fontSize": "12px", "color": "#666"}),
                    dcc.Checklist(
                        id="benchmark-keuze",
                        options=[{"label": v["naam"], "value": k} for k, v in BENCHMARKS.items()],
                        value=["IWDA.AS"], inline=True,
                        style={"display": "inline-flex", "gap": "3px", "flexWrap": "wrap"},
                        inputStyle={"marginRight": "3px"},
                        labelStyle={"padding": "3px 8px", "backgroundColor": "#F0F0F0",
                                    "borderRadius": "4px", "fontSize": "11px"},
                    ),
                ]),
                dcc.Graph(id="benchmark", config={"scrollZoom": True}),
            ]),
        ]),
        html.Div(style={"display": "grid", "gridTemplateColumns": "1fr 1fr",
                        "gap": "10px", **SECTION}, children=[
            dcc.Graph(id="dividenden", config={"displayModeBar": False}),
            dcc.Graph(id="heatmap",    config={"displayModeBar": False}),
        ]),
        html.Div(style={**SECTION}, children=[
            dcc.Graph(id="concentratie", config={"displayModeBar": False}),
        ]),

        # ── Opslag ────────────────────────────────────────────────────────────
        dcc.Store(id="portfolio-store"),   # parsed portfolio dict
        dcc.Store(id="koersen-store"),     # fetched price data
    ],
)


# ──────────────────────────────────────────────────────────────────────────────
# Callbacks
# ──────────────────────────────────────────────────────────────────────────────

@app.callback(
    Output("portfolio-store", "data"),
    Output("upload-status", "children"),
    Output("controls-sectie", "style"),
    Input("upload-csv", "contents"),
    State("upload-csv", "filename"),
    prevent_initial_call=True,
)
def upload_verwerken(contents, filename):
    if contents is None:
        return dash.no_update, "", {"display": "none"}

    try:
        portfolio = parse_degiro_csv(contents)
        n_etf   = len(portfolio["etfs"])
        n_aand  = len(portfolio["aandelen_actief"])
        n_ges   = len(portfolio["aandelen_gesloten"])
        issues  = portfolio.get("_ticker_issues", [])
        bericht = (
            f"✅  {filename} verwerkt  —  "
            f"{n_etf} ETF{'s' if n_etf != 1 else ''}, "
            f"{n_aand} aandeel/aandelen, "
            f"{n_ges} gesloten positie(s).  "
            f"Klik nu op 'Ververs koersen'."
        )
        if issues:
            bericht += f"  ⚠️  {len(issues)} instrument(en) met onbekende ticker (zie waarschuwing)."

        controls_stijl = {
            "display": "flex", "textAlign": "center", "marginBottom": "15px",
            "flexWrap": "wrap", "justifyContent": "center", "gap": "10px",
            "alignItems": "center",
        }
        return portfolio, bericht, controls_stijl

    except Exception as e:
        return None, f"❌  Fout: {str(e)}", {"display": "none"}


@app.callback(
    Output("koersen-store",  "data"),
    Output("status-tekst",   "children"),
    Output("loading-output", "children"),
    Input("ververs-btn", "n_clicks"),
    State("portfolio-store", "data"),
    prevent_initial_call=True,
)
def ververs(n, portfolio):
    if not portfolio:
        return dash.no_update, "Laad eerst een portfolio.", ""
    etfs   = portfolio.get("etfs", [])
    actief = portfolio.get("aandelen_actief", [])
    koersen = haal_koersen_op(etfs + actief)
    serial  = serialiseer(koersen)
    return serial, f"Ververst op {datetime.now().strftime('%d-%m-%Y %H:%M')}", ""


@app.callback(
    Output("taart-etf",         "figure"),
    Output("rendement-etf",     "figure"),
    Output("gesloten-posities", "figure"),
    Output("koers-etf",         "figure"),
    Output("koers-aandelen",    "figure"),
    Output("portfolio-historie","figure"),
    Output("benchmark",         "figure"),
    Output("dividenden",        "figure"),
    Output("heatmap",           "figure"),
    Output("concentratie",      "figure"),
    Output("totaal-overzicht",  "children"),
    Output("ticker-waarschuwing","children"),
    Input("koersen-store",  "data"),
    Input("periode-keuze",  "value"),
    Input("benchmark-keuze","value"),
    State("portfolio-store","data"),
)
def update(serial, periode, benchmarks, portfolio):
    leeg = [leeg_fig()] * 10

    if not serial or not portfolio:
        return *leeg, [], []

    koersen = deserialiseer(serial)
    etfs    = portfolio.get("etfs", [])
    actief  = portfolio.get("aandelen_actief", [])
    gesloten = portfolio.get("aandelen_gesloten", [])
    issues   = portfolio.get("_ticker_issues", [])

    etf_r  = bereken_resultaten(etfs,   koersen)
    aand_r = bereken_resultaten(actief, koersen)
    alle_r = etf_r + aand_r
    alle_p = etfs + actief

    f1  = maak_taartdiagram(etf_r)           if etf_r  else leeg_fig("Geen ETF-data")
    f2  = maak_rendement_staaf(etf_r, "Rendement per ETF") if etf_r else leeg_fig()
    f3  = maak_gesloten_posities(gesloten)
    f4  = maak_koersgrafiek(etf_r,  "ETF Koersverloop (%)", periode) if etf_r  else leeg_fig()
    f5  = maak_koersgrafiek(aand_r, "Aandelen Koersverloop (%)", periode) if aand_r else leeg_fig()
    f6  = maak_portfolio_historie(alle_p, koersen, periode)
    f7  = maak_benchmark(alle_p, koersen, periode, benchmarks or ["IWDA.AS"])
    f8  = maak_dividend_tracker(alle_r)
    f9  = maak_heatmap(alle_p, koersen)
    f10 = maak_concentratie(alle_r)

    # ── Overzichtskaarten (alleen posities met geldige koers) ─────────────────
    alle_r_geldig = [r for r in alle_r if r.get("huidige_koers", 0) > 0]
    tw = sum(r["huidige_waarde"] for r in alle_r_geldig)
    ti = sum(r["investering"]    for r in alle_r_geldig)
    wv = tw - ti
    rend = ((tw / ti) - 1) * 100 if ti > 0 else 0
    kleur = KOOP_KLEUR if wv >= 0 else VERKOOP_KLEUR

    def kaart(lbl, val, kl="#333"):
        return html.Div(
            style={"textAlign": "center", "padding": "10px 20px",
                   "backgroundColor": "white", "borderRadius": "8px",
                   "boxShadow": "0 1px 3px rgba(0,0,0,0.1)"},
            children=[
                html.Div(lbl, style={"fontSize": "11px", "color": "#888", "marginBottom": "3px"}),
                html.Div(val, style={"fontSize": "18px", "fontWeight": "bold", "color": kl}),
            ],
        )

    overzicht = [
        kaart("Marktwaarde",  f"€{tw:,.2f}"),
        kaart("Geïnvesteerd", f"€{ti:,.2f}"),
        kaart("Winst/verlies", f"€{wv:+,.2f}", kleur),
        kaart("Rendement",    f"{rend:+.1f}%",  kleur),
    ]

    # ── Ticker-waarschuwing ───────────────────────────────────────────────────
    waarschuwing = []
    if issues:
        rijen = [
            html.Tr([
                html.Td(i["product"], style={"padding": "4px 8px"}),
                html.Td(i["isin"],    style={"padding": "4px 8px", "fontFamily": "monospace"}),
                html.Td(i.get("gebruikte_ticker", "–") or "–",
                        style={"padding": "4px 8px", "fontFamily": "monospace"}),
                html.Td(i["opmerking"], style={"padding": "4px 8px", "color": "#888"}),
            ])
            for i in issues
        ]
        waarschuwing = [html.Div(style={**SECTION, "padding": "12px 16px", "marginBottom": "12px"}, children=[
            html.P(f"⚠️  {len(issues)} instrument(en) met onzekere ticker — "
                   "koersen worden mogelijk niet correct geladen. "
                   "Voeg het instrument toe aan ISIN_TICKER in degiro_parser.py als je dit wilt corrigeren.",
                   style={"color": "#a06000", "fontSize": "13px", "marginBottom": "8px"}),
            html.Table(
                [html.Thead(html.Tr([html.Th(h, style={"padding": "4px 8px", "textAlign": "left"})
                                     for h in ["Product", "ISIN", "Gebruikte ticker", "Opmerking"]]))]
                + [html.Tbody(rijen)],
                style={"fontSize": "12px", "borderCollapse": "collapse", "width": "100%"},
            ),
        ])]

    return f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, overzicht, waarschuwing


# ──────────────────────────────────────────────────────────────────────────────
# Start
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8050))
    print(f"\nDashboard: http://127.0.0.1:{port}")
    print("Ctrl+C om te stoppen.\n")
    app.run(debug=False, host="0.0.0.0", port=port)
