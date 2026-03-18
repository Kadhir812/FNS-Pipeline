import redis
import json

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

# ============================================================
# NEW KEYWORDS — high signal, category-specific
# No overlaps with other categories
# ============================================================
new_keywords = {

    # ── A: Earnings & Quarterly Results ─────────────────────
    # Focus: financial metrics, reporting events, performance
    "A": [
        # Reporting events
        "earnings release", "earnings report", "results announcement",
        "quarterly report", "half year results", "full year results",
        "interim results", "preliminary results", "final results",
        "trading update", "financial update", "investor update",
        "earnings season", "reporting season", "results season",

        # Performance metrics
        "revenue per share", "sales per share", "profit per share",
        "return on equity", "return on assets", "return on capital",
        "gross margin expansion", "margin compression", "margin pressure",
        "operating leverage", "cost leverage", "fixed cost leverage",
        "revenue acceleration", "revenue deceleration", "growth acceleration",
        "volume growth", "price growth", "mix effect", "fx impact",
        "currency headwind", "currency tailwind", "constant currency",

        # Guidance & estimates
        "raised outlook", "lowered outlook", "maintained outlook",
        "full year guidance raised", "full year guidance cut",
        "consensus beat", "consensus miss", "above expectations",
        "below expectations", "in line with expectations",
        "earnings surprise", "positive surprise", "negative surprise",
        "guidance midpoint", "guidance range", "guidance assumption",

        # Cash & balance sheet
        "net cash position", "net debt position", "cash generation",
        "cash conversion", "working capital improvement",
        "balance sheet strength", "leverage ratio", "gearing ratio",
        "dividend coverage", "interest coverage ratio",
    ],

    # ── B: Analyst Ratings & Recommendations ────────────────
    # Focus: broker actions, valuation, research
    "B": [
        # Broker actions
        "broker upgrade", "broker downgrade", "broker note",
        "analyst note", "analyst report", "research report",
        "initiation of coverage", "coverage initiation",
        "double upgrade", "double downgrade", "triple upgrade",
        "upgrade to overweight", "downgrade to underweight",
        "upgrade to outperform", "downgrade to underperform",
        "added to buy list", "removed from buy list",
        "added to conviction list", "price target increase",
        "price target decrease", "price target raised",
        "price target cut", "price target maintained",

        # Valuation
        "valuation attractive", "valuation stretched",
        "trading at discount", "trading at premium",
        "pe ratio", "price earnings ratio", "forward pe",
        "ev to ebitda", "price to cash flow", "dividend yield",
        "total return potential", "risk reward attractive",
        "risk reward unfavorable", "margin of safety",

        # Consensus
        "consensus buy", "consensus sell", "consensus hold",
        "majority buy", "majority sell", "split consensus",
        "analyst consensus", "street view", "wall street view",
        "sell side view", "buy side view", "institutional view",
    ],

    # ── C: Mergers, Acquisitions & Deals ────────────────────
    # Focus: deal events, structure, approval
    "C": [
        # Deal events
        "deal announced", "deal agreed", "deal signed",
        "deal collapsed", "deal withdrawn", "deal terminated",
        "deal extended", "deal revised", "deal sweetened",
        "bid raised", "bid rejected", "bid accepted",
        "offer accepted", "offer rejected", "offer lapsed",
        "merger completed", "acquisition completed", "deal completed",
        "merger cancelled", "acquisition cancelled", "deal cancelled",

        # Deal structure
        "all cash acquisition", "all stock acquisition",
        "cash and stock deal", "mixed consideration",
        "enterprise value", "equity value", "deal premium",
        "acquisition premium", "takeover premium", "control premium",
        "purchase price", "transaction value", "implied valuation",

        # Process
        "strategic review", "sale process", "formal sale process",
        "strategic alternatives", "potential acquirer", "potential buyer",
        "shortlisted bidder", "preferred bidder", "exclusive talks",
        "advanced talks", "preliminary talks", "exploratory talks",
        "non binding offer", "binding offer", "indicative offer",
        "letter of intent", "memorandum of understanding",
    ],

    # ── D: Regulatory & Legal ────────────────────────────────
    # Focus: enforcement, legal proceedings, compliance
    "D": [
        # Enforcement
        "regulatory action", "enforcement notice", "warning letter",
        "show cause notice", "notice of violation", "notice of default",
        "regulatory sanction", "regulatory censure", "regulatory ban",
        "license suspension", "license revocation", "license cancellation",
        "trading suspension", "operating suspension", "business suspension",

        # Legal proceedings
        "class action filed", "lawsuit filed", "legal proceedings",
        "court proceedings", "arbitration proceedings", "tribunal hearing",
        "legal challenge", "legal appeal", "court appeal",
        "injunction filed", "injunction granted", "injunction denied",
        "preliminary injunction", "temporary restraining order",

        # Compliance
        "compliance failure", "compliance breach", "compliance gap",
        "audit finding", "audit report", "internal audit",
        "external audit", "regulatory audit", "compliance review",
        "material weakness", "significant deficiency", "control failure",
        "restatement risk", "accounting irregularity", "fraud allegation",
    ],

    # ── E: Economic Data & Central Banks ────────────────────
    # Focus: macro indicators, central bank actions, data releases
    "E": [
        # Data releases
        "jobs data", "payroll data", "employment report",
        "inflation report", "price data", "consumer prices",
        "producer prices", "import prices", "export prices",
        "retail data", "consumer data", "spending data",
        "housing data", "construction data", "building permits",
        "factory orders", "durable goods", "capital goods orders",
        "trade data", "current account data", "balance of payments",
        "money supply", "credit data", "lending data",

        # Central bank
        "rate decision", "policy decision", "meeting minutes",
        "fed minutes", "fomc minutes", "ecb minutes",
        "policy statement", "forward guidance", "dot plot",
        "neutral rate", "terminal rate", "peak rate",
        "rate pause", "rate hold", "skip meeting",
        "quantitative easing announcement", "asset purchase program",
        "balance sheet expansion", "balance sheet reduction",
        "tapering announcement", "taper timeline",

        # Economic conditions
        "soft landing", "hard landing", "no landing",
        "recession probability", "recession risk", "recession signal",
        "growth slowdown", "growth pickup", "growth resilience",
        "consumer confidence", "business confidence", "sentiment index",
        "purchasing managers index", "pmi data", "composite pmi",
        "manufacturing pmi", "services pmi", "construction pmi",
    ],

    # ── F: Corporate Actions & Leadership ───────────────────
    # Focus: executive changes, shareholder returns, governance
    "F": [
        # Executive changes
        "chief executive officer", "president and ceo",
        "executive vice president", "managing director appointed",
        "group chief executive", "division head appointed",
        "regional head appointed", "country head appointed",
        "coo appointed", "cto appointed", "cmo appointed",
        "cro appointed", "ciso appointed", "chief risk officer",
        "management buyout", "management team change",
        "c suite change", "c level executive", "top executive",

        # Shareholder returns
        "capital return", "capital return program",
        "shareholder return program", "return of capital",
        "enhanced dividend", "progressive dividend",
        "special cash dividend", "variable dividend",
        "buyback program launched", "buyback program extended",
        "buyback program completed", "share cancellation",
        "tender offer buyback", "dutch auction buyback",

        # Governance
        "board composition", "board independence",
        "governance improvement", "governance concern",
        "governance failure", "governance risk",
        "esg governance", "board accountability",
        "shareholder rights", "minority shareholder",
        "controlling shareholder", "related party",
    ],

    # ── G: Market Trends & Sector Analysis ──────────────────
    # Focus: market dynamics, flows, sector moves
    "G": [
        # Market dynamics
        "market breadth improving", "market breadth deteriorating",
        "advance decline ratio", "new highs new lows",
        "market internals", "market structure", "market regime",
        "volatility regime", "low vol regime", "high vol regime",
        "risk appetite", "investor appetite", "market appetite",
        "fear greed index", "vix spike", "volatility spike",
        "volatility compression", "implied volatility",
        "realized volatility", "historical volatility",

        # Flows
        "fund inflows", "fund outflows", "net inflows", "net outflows",
        "equity inflows", "equity outflows", "bond inflows", "bond outflows",
        "etf inflows", "etf outflows", "passive flows", "active flows",
        "institutional flows", "retail flows", "foreign flows",
        "emerging market flows", "developed market flows",

        # Sector moves
        "sector leadership", "sector laggard", "sector outperform",
        "sector underperform", "sector upgrade", "sector downgrade",
        "sector rotation trade", "defensive rotation", "cyclical rotation",
        "growth to value rotation", "value to growth rotation",
        "quality rotation", "factor shift", "style shift",
    ],

    # ── H: IPO & New Listings ────────────────────────────────
    # Focus: listing events, pre-ipo, post-ipo
    "H": [
        # Pre-IPO
        "ipo preparation", "ipo readiness", "ipo timeline",
        "ipo candidate", "ipo pipeline", "ipo queue",
        "ipo backlog", "ipo calendar", "upcoming ipo",
        "planned ipo", "potential ipo", "rumored ipo",
        "ipo confidentially filed", "draft prospectus",
        "ipo mandate", "ipo bank mandate", "ipo adviser",

        # IPO events
        "ipo application", "ipo subscription opens",
        "ipo subscription closes", "ipo basis of allotment",
        "ipo refund", "ipo listing date", "ipo listing price",
        "grey market premium", "gmp", "kosdaq listing",
        "nasdaq listing", "nyse listing", "lse listing",
        "asx listing", "hkex listing", "sgx listing",

        # Post-IPO
        "post ipo performance", "post ipo lockup",
        "ipo lockup period", "ipo lockup expiry",
        "founder lockup", "employee lockup", "insider lockup",
        "post ipo selloff", "post ipo rally",
        "ipo quiet period", "quiet period expiry",
    ],

    # ── I: Product Launches & Innovation ────────────────────
    # Focus: product events, R&D milestones, approvals
    "I": [
        # Product events
        "product announcement", "product reveal", "product unveiling",
        "product demonstration", "product preview", "product teaser",
        "limited release", "soft launch", "hard launch",
        "regional launch", "global rollout", "phased rollout",
        "commercial launch", "commercial release", "market release",
        "product discontinuation", "product end of life",
        "product refresh", "product redesign", "product rebrand",

        # R&D milestones
        "research milestone", "development milestone",
        "clinical milestone", "regulatory milestone",
        "proof of concept achieved", "prototype completed",
        "pilot program", "pilot results", "field trial",
        "technology demonstration", "technology readiness",
        "innovation award", "research grant", "r&d investment",
        "r&d spending", "r&d budget", "r&d pipeline",

        # Approvals
        "regulatory clearance", "product approval",
        "marketing approval", "commercial approval",
        "ce marking", "fcc approval", "ul certification",
        "iso certification", "quality certification",
        "safety approval", "safety certification",
    ],

    # ── J: General Business & Industry News ─────────────────
    # Focus: truly generic business events only
    "J": [
        # Generic business events
        "business results", "company results", "corporate results",
        "company performance", "business performance",
        "company announcement", "corporate announcement",
        "company filing", "regulatory filing update",
        "company profile", "business profile", "industry profile",
        "market position update", "competitive update",
        "business model change", "revenue model change",
        "pricing strategy", "go to market strategy",

        # Operations
        "operational update", "operations review",
        "capacity expansion", "capacity reduction",
        "headcount increase", "headcount reduction", "layoffs",
        "redundancies", "workforce reduction", "job cuts",
        "hiring freeze", "recruitment drive", "talent acquisition",
        "office expansion", "office closure", "plant closure",
        "factory closure", "facility closure", "site closure",
        "new office", "new facility", "new headquarters",
    ],

    # ── K: Commodities & Energy ──────────────────────────────
    # Focus: price moves, supply/demand, production events
    "K": [
        # Price events
        "oil price surge", "oil price slump", "oil price crash",
        "gas price spike", "gas price drop", "energy price rally",
        "commodity price surge", "commodity price crash",
        "metal price rally", "metal price selloff",
        "gold rally", "gold selloff", "silver rally",
        "copper rally", "copper selloff", "iron ore rally",
        "agricultural price spike", "food price inflation",

        # Supply & demand
        "supply disruption", "supply outage", "supply shortage",
        "demand destruction", "demand recovery", "demand pickup",
        "inventory build", "inventory draw", "stock build",
        "stock draw", "supply glut", "supply deficit",
        "market surplus", "market deficit", "market balance",
        "production cut", "production increase", "output cut",
        "output increase", "capacity cut", "capacity addition",

        # Industry events
        "opec decision", "opec cut", "opec increase",
        "opec meeting outcome", "cartel decision",
        "pipeline outage", "pipeline disruption", "pipeline restart",
        "refinery outage", "refinery restart", "refinery maintenance",
        "mine closure", "mine opening", "mine expansion",
        "smelter closure", "smelter restart", "smelter curtailment",
    ],

    # ── L: ESG & Sustainability ──────────────────────────────
    # Focus: ESG ratings, commitments, reporting
    "L": [
        # Ratings & rankings
        "esg upgrade", "esg downgrade", "esg score improvement",
        "esg score decline", "sustainability rating",
        "msci esg rating", "sustainalytics rating",
        "cdp score", "dow jones sustainability index",
        "ftse4good index", "added to esg index",
        "removed from esg index", "esg inclusion",

        # Commitments & targets
        "net zero pledge", "carbon zero commitment",
        "climate commitment", "emissions target",
        "renewable energy target", "clean energy commitment",
        "sustainability commitment", "esg commitment",
        "climate action plan", "transition plan",
        "just transition", "energy transition plan",
        "decarbonization plan", "decarbonization target",
        "carbon reduction target", "emissions reduction target",

        # Reporting & disclosure
        "esg disclosure", "sustainability disclosure",
        "climate disclosure", "tcfd disclosure",
        "tcfd report", "gri report", "sasb report",
        "integrated report", "sustainability report published",
        "climate report", "carbon report", "emissions report",
        "scope emissions reported", "carbon accounting",
        "greenwashing allegation", "greenwashing claim",
        "esg controversy", "sustainability controversy",
    ],

    # ── M: Macro & Geopolitics ───────────────────────────────
    # Focus: geopolitical events, trade, macro risks
    "M": [
        # Regional conflicts
        "middle east conflict", "middle east tension",
        "israel hamas", "israel iran", "iran nuclear",
        "russia ukraine", "ukraine war", "ukraine ceasefire",
        "taiwan tension", "taiwan strait", "south china sea tension",
        "north korea missile", "korean peninsula",
        "india pakistan", "india china border",
        "africa conflict", "africa instability",

        # Trade & sanctions
        "trade restriction", "trade barrier", "trade dispute",
        "trade negotiation", "trade deal", "trade agreement",
        "tariff announcement", "tariff increase", "tariff reduction",
        "tariff exemption", "tariff retaliation", "counter tariff",
        "sanctions imposed", "sanctions lifted", "sanctions extended",
        "new sanctions", "sanctions relief", "sanctions waiver",
        "export ban", "import restriction", "technology ban",

        # Political events
        "election result", "election outcome", "election upset",
        "government change", "regime change", "leadership change",
        "prime minister", "presidential election", "parliamentary election",
        "political crisis", "political turmoil", "political uncertainty",
        "government collapse", "coalition breakdown", "snap election",
        "referendum result", "independence vote", "secession",
    ],

    # ── N: Technology Trends & Disruption ───────────────────
    # Focus: tech breakthroughs, platform shifts, digital economy
    "N": [
        # AI & ML
        "ai model launch", "ai model release", "llm release",
        "ai breakthrough", "ai capability", "ai benchmark",
        "ai safety", "ai regulation", "ai governance",
        "ai investment", "ai funding", "ai startup",
        "ai chip", "ai accelerator", "gpu demand",
        "inference chip", "training chip", "ai infrastructure",
        "openai release", "anthropic release", "google deepmind",

        # Platforms & cloud
        "cloud migration", "cloud adoption", "cloud revenue",
        "aws growth", "azure growth", "google cloud growth",
        "platform shift", "platform adoption", "saas growth",
        "paas growth", "iaas growth", "multicloud strategy",
        "hybrid cloud", "private cloud", "cloud security",

        # Cybersecurity
        "data breach", "cyber attack", "ransomware attack",
        "cyber incident", "security breach", "network breach",
        "zero day exploit", "vulnerability disclosed",
        "patch released", "security update", "cyber threat",
        "nation state attack", "critical infrastructure attack",

        # Emerging tech
        "quantum breakthrough", "quantum computer",
        "augmented reality launch", "virtual reality launch",
        "spatial computing", "wearable technology",
        "autonomous driving update", "self driving update",
        "ev battery breakthrough", "solid state battery",
        "semiconductor advance", "chip breakthrough",
        "next gen chip", "new processor", "new gpu",
    ],
}

# ============================================================
# EXECUTE — add new keywords avoiding duplicates
# ============================================================
def add_new_keywords(dry_run=True):
    print("🔍 Adding new keywords to Redis categories...")
    print(f"   Mode: {'DRY RUN' if dry_run else 'LIVE'}\n")

    total_added      = 0
    total_duplicates = 0

    for category, keywords in new_keywords.items():
        redis_key    = f"category_keywords_{category}"
        existing     = {kw.lower() for kw in r.smembers(redis_key)}
        truly_new    = [kw for kw in keywords if kw.lower() not in existing]
        duplicates   = [kw for kw in keywords if kw.lower() in existing]

        print(f"Category {category}:")
        print(f"  Existing : {len(existing)}")
        print(f"  New      : {len(truly_new)}")
        print(f"  Duplicate: {len(duplicates)}")

        if truly_new and not dry_run:
            r.sadd(redis_key, *truly_new)
            print(f"  ✅ Added  : {len(truly_new)} keywords")

        total_added      += len(truly_new)
        total_duplicates += len(duplicates)

    print(f"\n{'DRY RUN — ' if dry_run else ''}Summary:")
    print(f"  Total new keywords : {total_added}")
    print(f"  Total duplicates   : {total_duplicates}")

    if dry_run:
        print("\n  Run with dry_run=False to apply changes")

# ============================================================
# BACKUP — export all keywords to JSON
# ============================================================
def backup_keywords(filename="redis_keywords_backup.json"):
    all_keywords = {}
    for cat in CATEGORY_LETTERS:
        key      = f"category_keywords_{cat}"
        keywords = sorted(r.smembers(key))
        all_keywords[cat] = keywords
        print(f"  {cat}: {len(keywords)} keywords")

    with open(filename, "w") as f:
        json.dump(all_keywords, f, indent=2)

    total = sum(len(v) for v in all_keywords.values())
    print(f"\n✅ Backed up {total} keywords to {filename}")

CATEGORY_LETTERS = "ABCDEFGHIJKLMN"

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    try:
        r.ping()
        print("✅ Connected to Redis\n")
    except redis.ConnectionError:
        print("❌ Cannot connect to Redis")
        exit(1)

    # Step 1 — dry run first
    add_new_keywords(dry_run=True)

    # Step 2 — confirm
    print()
    confirm = input("Apply changes? Type 'yes' to confirm: ").strip().lower()
    if confirm == "yes":
        add_new_keywords(dry_run=False)
        print("\nBacking up...")
        backup_keywords()
    else:
        print("Cancelled")