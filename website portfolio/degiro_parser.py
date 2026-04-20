"""
degiro_parser.py  –  Verwerkt een DEGIRO transactie-export (CSV) naar
een portfolio-dict die de dashboard-app begrijpt.

Ondersteunde DEGIRO-exports:
  • Activiteit → Transacties  (klassieke export, komma of puntkomma)
  • UTF-8 / UTF-8-BOM / Latin-1

Resulterende dict-sleutels:
  etfs              – open ETF-posities
  aandelen_actief   – open aandelenposities
  aandelen_gesloten – volledig verkochte posities
  obligaties        – altijd []  (niet in DEGIRO-export)
  cash_saldo_eur    – altijd 0   (niet in transactie-export)
  _ticker_issues    – lijst van instrumenten waarvan de ticker onbekend is
"""

import io, base64, re
from collections import defaultdict
from datetime import datetime

import pandas as pd

# ──────────────────────────────────────────────────────────────────────────────
# ISIN → Yahoo Finance ticker  (meest gangbare ETFs op DEGIRO)
# ──────────────────────────────────────────────────────────────────────────────
ISIN_TICKER = {
    # Vanguard
    "IE00B3XXRP09": "VUSA.AS",
    "IE00B3RBWM25": "VWRL.AS",
    "IE00BKX55T58": "VEUR.AS",
    "IE00B1FZS350": "IBTA.AS",
    "IE00B3VVMM84": "VUSA.AS",
    "IE0031786142": "VUSA.AS",
    # iShares
    "IE00B4L5Y983": "IWDA.AS",
    "IE00B4L5YC18": "SWDA.AS",
    "IE00B52MJY50": "CSPX.AS",
    "IE00B0M62Q58": "IEMG.AS",
    "IE00B14X4T88": "SPYD.DE",
    "IE00B4WXJJ64": "EMIM.AS",
    "IE00B5BMR087": "CSPX.AS",
    "IE00BYX2JD69": "IUSQ.DE",
    # Invesco / PowerShares
    "IE00BFMXXD54": "EQQQ.DE",
    "IE00B23LNQ02": "EQQQ.DE",
    "IE0032077012": "EQQQ.DE",   # distributing variant
    # Xtrackers / DWS
    "LU1900195949": "XCS6.DE",
    "LU0514695690": "XCS6.DE",   # oudere ISIN zelfde ETF
    "LU0274211480": "DBPK.DE",
    "LU0490618542": "XDWD.DE",
    # SPDR
    "IE00B3YTMJ21": "SPYD.DE",
    "IE00B6YX5D40": "SPYY.DE",
    "IE00B6YX5C98": "SPYD.DE",
    # Amundi
    "LU1681043599": "PANX.PA",
    "FR0010315770": "CW8.PA",
    # Diversen ETFs
    "IE00B27YCK28": "IUSA.AS",
    "IE00B52SF786": "SMEA.AS",
    # ── US aandelen (ISIN → Yahoo ticker) ─────────────────────────────────────
    "US3453708600": "F",          # Ford Motor Co
    "US6245801062": "MOV",        # Movado Group
    "US4062161017": "HAL",        # Halliburton
    "US92556H2067": "PSKY",       # Paramount/Skydance (nu PSKY)
    "US9245241037": "VALE",       # Vale SA (ADR) – primaire ISIN
    "US91912E1055": "VALE",       # Vale SA (ADR) – alternatieve ISIN
    "US38141G1040": "GS",         # Goldman Sachs (voorbeeld)
    # ── Europese aandelen ─────────────────────────────────────────────────────
    "NL0013654783": "PRX.AS",     # Prosus NV
    "NL0009538784": "HEIA.AS",    # Heineken
    "NL0010273215": "ASML.AS",    # ASML
    # ── Obligaties: worden overgeslagen (geen yfinance-data) ──────────────────
    # ISINs die beginnen met FR + cijfers zijn vaak Franse staatsobligaties of
    # bedrijfsobligaties — worden apart verwerkt als _obligatie_isin
}

# Beurs-code → Yahoo Finance suffix
BEURS_SUFFIX = {
    "AMS":    ".AS",   # Amsterdam (Euronext)
    "EPA":    ".PA",   # Parijs (Euronext)
    "XETRA":  ".DE",   # Frankfurt XETRA
    "EAM":    ".MC",   # Madrid
    "LSE":    ".L",    # Londen
    "BVME":   ".MI",   # Milaan
    "XHEL":   ".HE",   # Helsinki
    "XOSL":   ".OL",   # Oslo
    "STO":    ".ST",   # Stockholm
    "VIE":    ".VI",   # Wenen
    "NASDAQ": "",
    "NDQ":    "",
    "NYSE":   "",
    "ARCX":   "",
    "BATS":   "",
}


# ──────────────────────────────────────────────────────────────────────────────
# Hulpfuncties
# ──────────────────────────────────────────────────────────────────────────────

def _decode(content_string: str) -> str:
    """Decodeer base64-string van dcc.Upload naar tekst."""
    if "," in content_string:
        content_string = content_string.split(",", 1)[1]
    raw = base64.b64decode(content_string)
    for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            pass
    return raw.decode("utf-8", errors="replace")


def _read_csv(text: str) -> pd.DataFrame:
    """Probeer CSV te lezen met komma of puntkomma als scheidingsteken."""
    for sep in (",", ";", "\t"):
        try:
            df = pd.read_csv(io.StringIO(text), sep=sep, dtype=str, keep_default_na=False)
            if len(df.columns) >= 7:
                return df
        except Exception:
            pass
    raise ValueError(
        "Kon het bestand niet als CSV lezen. Controleer of je de DEGIRO "
        "transactie-export hebt gedownload via Activiteit → Exporteren."
    )


def _to_float(val) -> float:
    """Zet een DEGIRO-getal-string om naar float.

    DEGIRO Nederland gebruikt Europese notatie:
      • Komma = decimaalscheiding  (69,714 → 69.714)
      • Punt  = duizendtalsscheiding  (1.234,56 → 1234.56)

    Als er alleen een komma aanwezig is (geen punt), is die komma ALTIJD
    een decimaalscheiding — ook met 3 decimalen zoals 69,714 of 101,383.
    """
    if val is None:
        return 0.0
    s = str(val).strip()
    # Verwijder valutasymbolen en spaties
    s = re.sub(r"[€$£A-Za-z\s]", "", s)
    if not s or s == "-":
        return 0.0
    # Europese notatie: 1.234,56  →  1234.56
    if "," in s and "." in s:
        if s.rindex(".") < s.rindex(","):
            # Europees: punt = duizendtal, komma = decimaal
            s = s.replace(".", "").replace(",", ".")
        else:
            # Amerikaans: komma = duizendtal, punt = decimaal
            s = s.replace(",", "")
    elif "," in s:
        # Alleen komma → altijd decimaalscheiding in Europese notatie
        # (bijv. "69,714" = 69.714, "101,383" = 101.383, "532,00" = 532.00)
        s = s.replace(",", ".")
    # Alleen punt → gewone decimaal of Amerikaanse notatie, laat staan
    try:
        return float(s)
    except ValueError:
        return 0.0


def _parse_date(s: str) -> str | None:
    """Probeer verschillende datumformaten; geeft 'YYYY-MM-DD' terug."""
    s = str(s).strip()
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def _find_col(cols: list[str], *keywords) -> int | None:
    """Zoek kolomindex op (hoofdletterongevoelig)."""
    for kw in keywords:
        for i, c in enumerate(cols):
            if kw in c.lower():
                return i
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Hoofdparser
# ──────────────────────────────────────────────────────────────────────────────

def parse_degiro_csv(content_string: str) -> dict:
    """
    Verwerk DEGIRO transactie-CSV naar een portfolio-dict.

    Parameters
    ----------
    content_string : str
        Base64-gecodeerde bestandsinhoud van dcc.Upload (inclusief data-URI prefix).

    Returns
    -------
    dict met sleutels: etfs, aandelen_actief, aandelen_gesloten,
                       obligaties, cash_saldo_eur, _ticker_issues
    """
    text = _decode(content_string)
    df = _read_csv(text)
    cols_raw = df.columns.tolist()
    cols = [c.lower().strip() for c in cols_raw]

    # ── Kolommen identificeren ────────────────────────────────────────────────
    i_datum   = _find_col(cols, "datum")
    i_product = _find_col(cols, "product")
    i_isin    = _find_col(cols, "isin")
    i_beurs   = _find_col(cols, "beurs")
    i_aantal  = _find_col(cols, "aantal")
    i_koers   = _find_col(cols, "koers")

    if i_datum is None or i_product is None:
        raise ValueError(
            "Vereiste kolommen (Datum, Product) niet gevonden. "
            "Upload de transactie-export via DEGIRO → Activiteit → Exporteren."
        )

    # ── Rijen inlezen ─────────────────────────────────────────────────────────
    raw_txs = []
    for _, row in df.iterrows():
        vals = row.tolist()

        datum = _parse_date(vals[i_datum]) if i_datum is not None else None
        if datum is None:
            continue  # overgeslagen kopregel of lege rij

        product = str(vals[i_product]).strip() if i_product is not None else "Onbekend"
        isin    = str(vals[i_isin]).strip()    if i_isin   is not None else ""
        beurs   = str(vals[i_beurs]).strip()   if i_beurs  is not None else ""
        aantal  = _to_float(vals[i_aantal])    if i_aantal is not None else 0.0
        koers   = _to_float(vals[i_koers])     if i_koers  is not None else 0.0

        # Lege/ongeldige rijen overslaan
        if product in ("nan", "") or aantal == 0 or koers == 0:
            continue

        isin  = isin  if isin  not in ("nan", "")  else ""
        beurs = beurs if beurs not in ("nan", "NaN") else ""

        raw_txs.append({
            "datum":   datum,
            "product": product,
            "isin":    isin,
            "beurs":   beurs,
            "type":    "koop" if aantal > 0 else "verkoop",
            "aantal":  abs(aantal),
            "koers":   abs(koers),
        })

    if not raw_txs:
        raise ValueError(
            "Geen transacties gevonden. Controleer of je de juiste export hebt geüpload "
            "(DEGIRO → Activiteit → Exporteren → CSV)."
        )

    # ── Groeperen per instrument ──────────────────────────────────────────────
    instrument_txs:  dict[str, list] = defaultdict(list)
    instrument_meta: dict[str, dict] = {}

    for tx in raw_txs:
        key = tx["isin"] if tx["isin"] else tx["product"]
        instrument_txs[key].append(tx)
        if key not in instrument_meta:
            instrument_meta[key] = {
                "product": tx["product"],
                "isin":    tx["isin"],
                "beurs":   tx["beurs"],
            }
        elif not instrument_meta[key]["beurs"] and tx["beurs"]:
            instrument_meta[key]["beurs"] = tx["beurs"]

    # ── Per instrument: positie berekenen ────────────────────────────────────
    etfs:              list = []
    aandelen_actief:   list = []
    aandelen_gesloten: list = []
    ticker_issues:     list = []

    ETF_KEYWORDS = {
        "ETF", "INDEX", "FUND", "MSCI", "S&P", "S&P500", "NASDAQ", "FTSE",
        "DAX", "VANGUARD", "ISHARES", "AMUNDI", "XTRACKERS", "INVESCO",
        "SPDR", "UCITS", "LYXOR", "VANECK", "WISDOMTREE",
    }

    for key, txs in instrument_txs.items():
        meta = instrument_meta[key]
        txs_sorted = sorted(txs, key=lambda x: x["datum"])

        # Cumul positie & kostprijs bijhouden
        cumul       = 0.0
        cost_basis  = 0.0
        buy_txs     = []
        sell_txs    = []

        for tx in txs_sorted:
            if tx["type"] == "koop":
                cumul      += tx["aantal"]
                cost_basis += tx["aantal"] * tx["koers"]
                buy_txs.append(tx)
            else:
                # Proportioneel kostprijs verminderen
                prev = cumul
                cumul -= tx["aantal"]
                cumul  = max(cumul, 0)
                if prev > 0:
                    cost_basis *= max(cumul / prev, 0)
                else:
                    cost_basis = 0.0
                sell_txs.append(tx)

        cumul = round(cumul, 6)

        # ── Ticker opzoeken ───────────────────────────────────────────────────
        isin  = meta["isin"]
        beurs = meta["beurs"]
        ticker: str | None = None

        # Obligaties herkennen aan ISIN-prefix (FR, XS, DE, etc. voor bonds)
        # of productnamen met % (coupon) → overslaan, geen yfinance-data
        naam_upper_check = meta["product"].upper()
        is_bond = (
            re.search(r"\d+[.,]\d+\s*%", naam_upper_check) is not None  # bijv. "3.7%"
            or "BOND" in naam_upper_check
            or "OBLIGAT" in naam_upper_check
        )
        if is_bond:
            ticker_issues.append({
                "product": meta["product"],
                "isin":    isin,
                "beurs":   beurs,
                "gebruikte_ticker": None,
                "opmerking": "Obligatie overgeslagen (geen koersdata via yfinance)",
            })
            continue

        if isin and isin in ISIN_TICKER:
            ticker = ISIN_TICKER[isin]
        elif isin:
            # Probeer ISIN rechtstreeks — yfinance ondersteunt sommige ISINs
            ticker = isin
            ticker_issues.append({
                "product":          meta["product"],
                "isin":             isin,
                "beurs":            beurs,
                "gebruikte_ticker": ticker,
                "opmerking":        "ISIN gebruikt als ticker (niet in bekende mapping)",
            })
        else:
            ticker_issues.append({
                "product":          meta["product"],
                "isin":             "",
                "beurs":            beurs,
                "gebruikte_ticker": None,
                "opmerking":        "Geen ISIN — positie overgeslagen",
            })
            continue  # Geen ticker → overslaan

        eerste_aankoop = buy_txs[0]["datum"] if buy_txs else txs_sorted[0]["datum"]
        gem_aankoop    = (cost_basis / cumul) if cumul > 0.001 else (
            sum(t["koers"] for t in buy_txs) / len(buy_txs) if buy_txs else 0
        )

        positie = {
            "ticker":        ticker,
            "naam":          meta["product"],
            "aantal":        round(cumul, 6),
            "aankoopprijs":  round(gem_aankoop, 4),
            "eerste_aankoop": eerste_aankoop,
            "transacties": [
                {
                    "datum": tx["datum"],
                    "type":  tx["type"],
                    "aantal": tx["aantal"],
                    "koers":  tx["koers"],
                }
                for tx in txs_sorted
            ],
        }

        if cumul > 0.001:
            # ETF of aandeel?
            naam_upper = meta["product"].upper()
            is_etf = any(kw in naam_upper for kw in ETF_KEYWORDS)
            (etfs if is_etf else aandelen_actief).append(positie)
        else:
            # Gesloten positie
            if buy_txs and sell_txs:
                totaal_koop    = sum(t["aantal"] * t["koers"] for t in buy_txs)
                totaal_verkoop = sum(t["aantal"] * t["koers"] for t in sell_txs)
                n_koop         = sum(t["aantal"] for t in buy_txs)
                n_verkoop      = sum(t["aantal"] for t in sell_txs)
                aandelen_gesloten.append({
                    "naam":           meta["product"],
                    "koop_koers":     round(totaal_koop  / n_koop,    4),
                    "verkoop_koers":  round(totaal_verkoop / n_verkoop, 4),
                    "aantal":         round(n_koop, 6),
                    "valuta":         "EUR",
                    "koop_datum":     buy_txs[0]["datum"],
                    "verkoop_datum":  sell_txs[-1]["datum"],
                })

    return {
        "etfs":               etfs,
        "aandelen_actief":    aandelen_actief,
        "aandelen_gesloten":  aandelen_gesloten,
        "obligaties":         [],
        "cash_saldo_eur":     0,
        "_ticker_issues":     ticker_issues,
    }
