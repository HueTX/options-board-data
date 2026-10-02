"""Curated universe of liquid, optionable tickers for the daily options-volume scan.

Covers index/leveraged/sector ETFs plus the most actively option-traded single
names across tech, semis, AI, quantum, space, crypto-adjacent, meme, pharma,
financials, energy, consumer, industrials and China ADRs. Edit freely — the
scanner reads this list every run.
"""

TICKERS = [
    # --- Index & leveraged ETFs ---
    "SPY", "QQQ", "IWM", "DIA", "TQQQ", "SQQQ", "SOXL", "SOXS",
    "UVXY", "VXX", "SPXL", "SPXU", "TNA", "TZA", "FAS", "FAZ",
    "TECL", "TECS", "FNGU", "FNGD", "LABU", "LABD", "BOIL", "KOLD",
    "YINN", "YANG", "EDC", "EDZ", "NUGT", "DUST", "JNUG", "JDST",
    "GUSH", "DRIP", "NRGU", "NRGD",
    # --- Sector / thematic ETFs ---
    "XLF", "XLK", "XLE", "XLV", "XLI", "XLP", "XLU", "XLY", "XLB",
    "XLRE", "XLC", "SMH", "SOXX", "ARKK", "TLT", "HYG", "GLD",
    "SLV", "USO", "UNG", "KRE", "XBI", "XOP", "GDX", "GDXJ",
    "URA", "TAN", "LIT", "XRT", "ITA", "JETS", "IGV", "FDN",
    # --- Mega-cap tech ---
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "GOOG", "META",
    "TSLA", "AVGO", "ORCL", "CRM", "AMD", "NFLX", "ADBE",
    "INTC", "IBM", "CSCO", "TXN", "QCOM", "AMAT", "LRCX",
    "MU", "KLAC", "SNPS", "CDNS", "MRVL", "ARM", "ASML",
    "TSM", "ANET", "NXPI", "MCHP", "ON", "CRDO", "TSEM",
    "WDC", "STX", "DELL", "SMCI", "HPQ", "HPE", "P",
    # --- Software / cloud / cyber ---
    "PLTR", "PANW", "CRWD", "FTNT", "SNOW", "DDOG", "NET",
    "ZS", "OKTA", "MDB", "TEAM", "WDAY", "NOW", "SHOP",
    "APP", "PATH", "AI", "SOUN", "BBAI", "UPST",
    # --- Fintech / crypto-adjacent ---
    "COIN", "HOOD", "MSTR", "MARA", "RIOT", "CLSK",
    "PYPL", "XYZ", "AFRM", "SOFI", "NU",
    # --- Social / consumer tech ---
    "RDDT", "SPOT", "ROKU", "PINS", "SNAP", "ZM", "PTON",
    "DKNG", "RBLX", "U", "TTWO", "UBER", "LYFT",
    "DASH", "ABNB", "BKNG", "EXPE",
    # --- Meme / retail favorites ---
    "GME", "AMC", "DJT", "OPEN",
    # --- Space / quantum / speculative tech ---
    "RKLB", "LUNR", "ASTS", "JOBY", "ACHR",
    "IONQ", "RGTI", "QBTS",
    # --- EVs / autos ---
    "RIVN", "LCID", "NIO", "XPEV", "LI", "F", "GM",
    # --- Pharma / biotech / health ---
    "LLY", "NVO", "UNH", "JNJ", "PFE", "MRK", "ABBV",
    "AMGN", "GILD", "REGN", "VRTX", "ISRG", "BIIB",
    "DXCM", "ZBH", "MDT", "SYK", "BSX", "EW", "TMO",
    "MRNA", "BNTX", "HIMS", "CRSP", "BEAM", "NTLA",
    # --- Financials ---
    "JPM", "BAC", "GS", "MS", "C", "WFC", "AXP", "V",
    "MA", "COF", "SCHW", "BLK", "BX", "KKR", "APO",
    "ARES", "CME", "ICE", "PS",
    # --- Energy ---
    "XOM", "CVX", "COP", "EOG", "SLB", "OXY", "MPC",
    "VLO", "PSX", "EQT", "KMI", "WMB", "BE", "ENPH",
    "FSLR", "NEE", "DUK", "CEG", "VST",
    # --- Consumer / staples ---
    "WMT", "COST", "TGT", "HD", "LOW", "MCD", "SBUX",
    "CMG", "DIS", "NKE", "LULU", "DECK", "CROX",
    "PG", "KO", "PEP", "MDLZ", "KHC", "GIS", "CL",
    "MNST", "STZ", "EL", "TPR", "RL",
    # --- Travel / leisure ---
    "RCL", "CCL", "NCLH", "MAR", "HLT",
    # --- Industrials / defense ---
    "CAT", "DE", "BA", "LMT", "RTX", "GD", "NOC",
    "HON", "GE", "MMM", "EMR", "ETN", "PH", "ROK",
    "CARR", "OTIS", "JCI", "UNP", "NSC", "CSX",
    "UPS", "FDX", "DAL", "UAL", "AAL", "LUV",
    # --- Telecom / media ---
    "T", "VZ", "TMUS", "CHTR", "CMCSA", "WBD", "PSKY",
    # --- China ADRs ---
    "BABA", "JD", "PDD", "BIDU", "BILI", "FUTU",
]

# de-duplicated, order-preserving
TICKERS = list(dict.fromkeys(TICKERS))

# Broad-market index products (plain + leveraged/inverse + volatility).
# Excluded from the headline ranking — they always swamp single names.
INDEX_PRODUCTS = {
    "SPY", "QQQ", "IWM", "DIA",
    "TQQQ", "SQQQ", "SPXL", "SPXU", "TNA", "TZA",
    "TECL", "TECS", "FNGU", "FNGD", "LABU", "LABD",
    "FAS", "FAZ", "EDC", "EDZ", "YINN", "YANG",
    "NUGT", "DUST", "JNUG", "JDST", "GUSH", "DRIP",
    "NRGU", "NRGD", "BOIL", "KOLD", "UVXY", "VXX",
}
