#!/usr/bin/env python3
"""
Script to add additional keywords to Redis categories while avoiding duplicates
"""

import redis
import json
from collections import defaultdict

# Redis connection
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Additional keywords to add (avoiding duplicates from your existing set)
additional_keywords = {
    'A': [
        # Financial Performance & Metrics
        'fcf', 'free cash flow', 'working capital', 'debt to equity', 'roe', 'roa', 'roic',
        'book value', 'tangible book value', 'price to book', 'price to sales', 'ev/ebitda',
        'debt service', 'interest coverage', 'current ratio', 'quick ratio', 'cash ratio',
        'inventory turnover', 'receivables turnover', 'asset turnover', 'equity ratio',
        'gross profit', 'operating profit', 'pre-tax income', 'after-tax income',
        'diluted eps', 'adjusted eps', 'non-gaap earnings', 'normalized earnings',
        'recurring revenue', 'subscription revenue', 'same store sales', 'comparable sales',
        'organic growth', 'inorganic growth', 'revenue mix', 'margin expansion',
        'cost reduction', 'efficiency gains', 'operational leverage', 'economies of scale',
        'write-down', 'impairment', 'goodwill impairment', 'restructuring charges',
        'one-time charges', 'extraordinary items', 'special items', 'pro forma',
        'restatement', 'accounting change', 'revenue recognition', 'deferred revenue'
    ],
    
    'B': [
        # Analyst Coverage & Research
        'street consensus', 'estimate revision', 'earnings whisper', 'guidance revision',
        'sum of parts', 'dcf model', 'comparable analysis', 'precedent transactions',
        'peer multiples', 'sector comparison', 'relative valuation', 'intrinsic value',
        'fair value', 'target multiple', 'upside potential', 'downside risk',
        'investment case', 'investment rationale', 'key risks', 'risk factors',
        'catalysts', 'value drivers', 'growth drivers', 'margin drivers',
        'thesis points', 'key assumptions', 'sensitivity analysis', 'scenario analysis',
        'bull case', 'bear case', 'base case', 'blue sky scenario',
        'conviction buy', 'conviction sell', 'top pick', 'focus list',
        'watchlist', 'restricted list', 'under review', 'rating under review',
        'coverage suspended', 'not rated', 'sector weight', 'equal weight',
        'market weight', 'tactical outperform', 'tactical underperform',
        'long term buy', 'speculative buy', 'momentum buy'
    ],
    
    'C': [
        # M&A & Corporate Actions
        'strategic buyer', 'financial buyer', 'white knight', 'poison pill',
        'golden parachute', 'breakup fee', 'reverse termination fee', 'mac clause',
        'material adverse change', 'due diligence', 'data room', 'management presentation',
        'synergies', 'cost synergies', 'revenue synergies', 'deal rationale',
        'accretion', 'dilution', 'eps accretive', 'eps dilutive', 'cash accretive',
        'value creation', 'strategic fit', 'cultural fit', 'integration risk',
        'execution risk', 'regulatory risk', 'antitrust clearance', 'competition approval',
        'shareholder approval', 'board recommendation', 'proxy fight', 'activist investor',
        'unsolicited bid', 'competing bid', 'auction process', 'go shop period',
        'standstill agreement', 'exclusivity period', 'collar structure', 'contingent value',
        'earnout', 'escrow', 'representations and warranties', 'indemnification',
        'closing conditions', 'regulatory filing', 'hart scott rodino', 'committee on foreign investment'
    ],
    
    'D': [
        # Regulatory & Legal
        'cease and desist', 'consent decree', 'settlement agreement', 'fine imposition',
        'penalty assessment', 'regulatory guidance', 'rule proposal', 'comment period',
        'final rule', 'interim rule', 'emergency rule', 'supervisory guidance',
        'enforcement action', 'civil penalty', 'criminal charges', 'plea agreement',
        'deferred prosecution', 'non prosecution agreement', 'compliance monitor',
        'remediation plan', 'corrective action', 'regulatory capital', 'stress test',
        'ccar', 'dfast', 'liquidity coverage ratio', 'net stable funding ratio',
        'basel iii', 'dodd frank', 'volcker rule', 'fiduciary rule',
        'best interest standard', 'suitability rule', 'know your customer',
        'anti money laundering', 'suspicious activity report', 'currency transaction report',
        'bank secrecy act', 'patriot act', 'foreign corrupt practices act',
        'sanctions compliance', 'export controls', 'cfius review', 'national security review',
        'data privacy', 'gdpr compliance', 'ccpa compliance', 'cybersecurity regulation'
    ],
    
    'E': [
        # Economic Data & Indicators
        'leading indicators', 'lagging indicators', 'coincident indicators', 'yield curve',
        'term structure', 'credit spread', 'risk premium', 'equity risk premium',
        'real interest rate', 'nominal interest rate', 'effective rate', 'target rate',
        'overnight rate', 'repo rate', 'reverse repo', 'discount rate',
        'prime rate', 'libor', 'sofr', 'treasury yield', 'bond yield',
        'yield spread', 'duration risk', 'convexity', 'credit risk',
        'default risk', 'sovereign risk', 'currency risk', 'exchange rate',
        'purchasing power parity', 'real exchange rate', 'trade weighted dollar',
        'dollar index', 'commodity index', 'inflation expectation', 'breakeven inflation',
        'core inflation', 'headline inflation', 'deflation', 'disinflation',
        'stagflation', 'hyperinflation', 'phillips curve', 'output gap',
        'potential gdp', 'productivity growth', 'labor force participation', 'wage growth',
        'unit labor cost', 'profit margin', 'capacity utilization', 'inventory to sales'
    ],
    
    'F': [
        # Leadership & Corporate Actions
        'activist campaign', 'proxy contest', 'shareholder proposal', 'say on pay',
        'compensation committee', 'nominating committee', 'audit committee', 'risk committee',
        'independent director', 'lead director', 'presiding director', 'executive session',
        'board evaluation', 'director tenure', 'board diversity', 'skill matrix',
        'succession planning', 'talent pipeline', 'retention program', 'incentive plan',
        'stock option', 'restricted stock', 'performance share', 'phantom stock',
        'stock appreciation right', 'clawback provision', 'golden handcuff', 'retention bonus',
        'severance package', 'change in control', 'double trigger', 'single trigger',
        'employment agreement', 'non compete clause', 'non solicitation', 'confidentiality agreement',
        'fiduciary duty', 'duty of care', 'duty of loyalty', 'business judgment rule',
        'entire fairness', 'conflict of interest', 'related party transaction', 'arm\'s length',
        'whistleblower complaint', 'sec investigation', 'internal investigation', 'forensic audit'
    ],
    
    'G': [
        # Market Trends & Sentiment
        'risk on', 'risk off', 'flight to quality', 'flight to safety',
        'safe haven', 'risk asset', 'defensive asset', 'cyclical asset',
        'momentum factor', 'value factor', 'quality factor', 'size factor',
        'low volatility', 'high dividend', 'growth at reasonable price', 'garp',
        'contrarian investing', 'momentum investing', 'mean reversion', 'trend following',
        'technical breakout', 'support level', 'resistance level', 'moving average',
        'relative strength', 'price momentum', 'earnings momentum', 'estimate revision',
        'short interest', 'days to cover', 'short squeeze', 'gamma squeeze',
        'options flow', 'unusual activity', 'block trade', 'dark pool',
        'algorithmic trading', 'high frequency trading', 'market making', 'liquidity provision',
        'market microstructure', 'bid ask spread', 'market impact', 'price discovery',
        'correlation breakdown', 'dispersion trade', 'pairs trade', 'statistical arbitrage',
        'factor rotation', 'style rotation', 'geographic rotation', 'currency hedging'
    ],
    
    'H': [
        # IPO & Listings
        'pre ipo', 'unicorn', 'decacorn', 'venture round', 'series a', 'series b',
        'growth equity', 'late stage', 'crossover fund', 'sovereign wealth fund',
        'cornerstone investor', 'anchor investor', 'institutional allocation', 'retail allocation',
        'employee allocation', 'friends and family', 'directed share program', 'loyalty program',
        'greenshoe option', 'over allotment', 'stabilization', 'aftermarket support',
        'lockup expiry', 'insider selling', 'secondary offering', 'follow on offering',
        'at the market offering', 'shelf registration', 'universal shelf', 'well known seasoned issuer',
        'registration statement', 'preliminary prospectus', 'red herring', 'final prospectus',
        'pricing call', 'book building', 'price discovery', 'allocation process',
        'institutional book', 'retail book', 'subscription ratio', 'listing ceremony',
        'first day pop', 'first day close', 'reference price', 'opening price',
        'closing price', 'trading halt', 'circuit breaker', 'volatility auction'
    ],
    
    'I': [
        # Innovation & Technology
        'proof of concept', 'minimum viable product', 'mvp', 'alpha version',
        'beta version', 'release candidate', 'general availability', 'end of life',
        'legacy system', 'next generation', 'disruptive technology', 'breakthrough innovation',
        'competitive advantage', 'moat', 'network effect', 'platform effect',
        'ecosystem', 'api integration', 'interoperability', 'scalability',
        'cloud native', 'microservices', 'containerization', 'devops',
        'agile development', 'continuous integration', 'continuous deployment', 'version control',
        'open source', 'proprietary technology', 'trade secret', 'know how',
        'intellectual property portfolio', 'patent thicket', 'patent cliff', 'generic competition',
        'biosimilar', 'me too drug', 'first in class', 'best in class',
        'regulatory pathway', 'clinical trial', 'phase i', 'phase ii', 'phase iii',
        'new drug application', 'biologics license application', 'orphan drug', 'fast track'
    ],
    
    'J': [
        # General Business
        'value proposition', 'competitive positioning', 'market position', 'market leader',
        'market challenger', 'market follower', 'niche player', 'first mover advantage',
        'late mover advantage', 'switching cost', 'customer loyalty', 'brand equity',
        'customer acquisition cost', 'customer lifetime value', 'retention rate', 'churn rate',
        'net promoter score', 'customer satisfaction', 'market penetration', 'market development',
        'product development', 'diversification', 'vertical integration', 'horizontal integration',
        'outsourcing', 'offshoring', 'nearshoring', 'reshoring', 'supply chain resilience',
        'just in time', 'lean manufacturing', 'six sigma', 'total quality management',
        'continuous improvement', 'best practice', 'operational excellence', 'process optimization',
        'digital transformation', 'automation', 'artificial intelligence', 'machine learning',
        'data analytics', 'business intelligence', 'predictive analytics', 'prescriptive analytics',
        'key performance indicator', 'balanced scorecard', 'dashboard', 'real time monitoring'
    ],
    
    'K': [
        # Commodities & Energy
        'spot price', 'forward price', 'futures contract', 'contango', 'backwardation',
        'storage cost', 'convenience yield', 'basis risk', 'crack spread',
        'spark spread', 'refining margin', 'processing margin', 'crush spread',
        'calendar spread', 'location spread', 'quality spread', 'transport cost',
        'pipeline capacity', 'storage capacity', 'refining capacity', 'production capacity',
        'spare capacity', 'utilization rate', 'breakeven price', 'cash cost',
        'all in sustaining cost', 'marginal cost', 'replacement cost', 'depletion',
        'reserve replacement', 'resource development', 'exploration success', 'discovery',
        'field development', 'production ramp', 'decline curve', 'enhanced recovery',
        'unconventional', 'shale oil', 'tight oil', 'oil sands', 'heavy oil',
        'lng terminal', 'gas pipeline', 'gas storage', 'peak demand', 'base load',
        'renewable portfolio standard', 'carbon tax', 'cap and trade', 'carbon border adjustment'
    ],
    
    'L': [
        # ESG & Sustainability
        'scope 1 emissions', 'scope 2 emissions', 'scope 3 emissions', 'carbon intensity',
        'emission factor', 'greenhouse gas', 'co2 equivalent', 'carbon dioxide',
        'methane', 'nitrous oxide', 'fluorinated gas', 'carbon sink',
        'carbon offset', 'carbon credit', 'verified carbon standard', 'gold standard',
        'additionality', 'permanence', 'leakage', 'baseline scenario',
        'science based target', 'net zero commitment', 'carbon neutral', 'climate neutral',
        'renewable energy certificate', 'power purchase agreement', 'virtual ppa',
        'green building', 'leed certification', 'breeam rating', 'energy star',
        'circular economy', 'waste reduction', 'recycling rate', 'material recovery',
        'water stewardship', 'water stress', 'water scarcity', 'water footprint',
        'biodiversity', 'ecosystem service', 'natural capital', 'deforestation',
        'sustainable supply chain', 'responsible sourcing', 'conflict mineral', 'fair trade',
        'living wage', 'human rights', 'labor standard', 'workplace safety'
    ],
    
    'M': [
        # Geopolitics & Events
        'strategic competition', 'great power rivalry', 'sphere of influence', 'balance of power',
        'alliance system', 'bilateral relation', 'multilateral agreement', 'trade bloc',
        'economic partnership', 'free trade agreement', 'customs union', 'common market',
        'most favored nation', 'national treatment', 'dispute settlement', 'wto panel',
        'investment treaty', 'bilateral investment treaty', 'investor state dispute', 'arbitration',
        'economic espionage', 'technology transfer', 'forced technology transfer', 'dual use technology',
        'export restriction', 'import ban', 'quota system', 'licensing requirement',
        'national security exception', 'essential security interest', 'critical infrastructure', 'strategic asset',
        'foreign ownership restriction', 'sectoral cap', 'screening mechanism', 'approval process',
        'reciprocity principle', 'mirror measure', 'retaliation', 'escalation',
        'de escalation', 'confidence building', 'track two diplomacy', 'back channel',
        'mediation', 'arbitration', 'good office', 'peacekeeping', 'humanitarian intervention'
    ],
    
    'N': [
        # Technology & Innovation
        'generative ai', 'large language model', 'foundation model', 'transformer architecture',
        'neural network', 'deep learning', 'reinforcement learning', 'supervised learning',
        'unsupervised learning', 'computer vision', 'natural language processing', 'speech recognition',
        'edge computing', 'fog computing', 'distributed computing', 'parallel processing',
        'quantum supremacy', 'quantum advantage', 'quantum algorithm', 'quantum cryptography',
        'post quantum cryptography', 'homomorphic encryption', 'zero knowledge proof', 'differential privacy',
        'federated learning', 'transfer learning', 'few shot learning', 'meta learning',
        'explainable ai', 'responsible ai', 'ai ethics', 'algorithmic bias',
        'digital twin', 'simulation', 'modeling', 'virtual reality', 'augmented reality',
        'mixed reality', 'metaverse', 'web3', 'decentralized autonomous organization',
        'smart contract', 'decentralized finance', 'non fungible token', 'central bank digital currency',
        'stablecoin', 'cryptocurrency', 'digital asset', 'tokenization', 'programmable money'
    ]
}

def check_existing_keywords():
    """Check what keywords already exist in Redis"""
    existing_keywords = {}
    categories = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N']
    
    for category in categories:
        redis_key = f'category_keywords_{category}'
        existing_keywords[category] = set(redis_client.smembers(redis_key))
    
    return existing_keywords

def add_new_keywords(dry_run=True):
    """Add new keywords to Redis, avoiding duplicates"""
    
    print("🔍 Checking existing keywords in Redis...")
    existing_keywords = check_existing_keywords()
    
    stats = {
        'total_existing': 0,
        'total_new': 0,
        'total_duplicates': 0,
        'category_stats': {}
    }
    
    for category, new_keywords in additional_keywords.items():
        existing_set = existing_keywords.get(category, set())
        stats['total_existing'] += len(existing_set)
        
        # Convert to lowercase for comparison (your Redis seems to store lowercase)
        existing_lower = {kw.lower() for kw in existing_set}
        new_keywords_lower = {kw.lower() for kw in new_keywords}
        
        # Find truly new keywords
        truly_new = new_keywords_lower - existing_lower
        duplicates = new_keywords_lower & existing_lower
        
        stats['category_stats'][category] = {
            'existing': len(existing_set),
            'proposed': len(new_keywords),
            'new': len(truly_new),
            'duplicates': len(duplicates)
        }
        
        stats['total_new'] += len(truly_new)
        stats['total_duplicates'] += len(duplicates)
        
        print(f"\n📂 Category {category}:")
        print(f"   Existing: {len(existing_set)} keywords")
        print(f"   Proposed: {len(new_keywords)} keywords")
        print(f"   New: {len(truly_new)} keywords")
        print(f"   Duplicates: {len(duplicates)} keywords")
        
        if duplicates and len(duplicates) <= 5:
            print(f"   🔄 Some duplicates: {', '.join(list(duplicates)[:5])}")
        
        if truly_new:
            print(f"   ✨ New keywords to add: {', '.join(list(truly_new)[:5])}...")
            
            if not dry_run:
                redis_key = f'category_keywords_{category}'
                for keyword in truly_new:
                    redis_client.sadd(redis_key, keyword)
                print(f"   ✅ Added {len(truly_new)} new keywords to Redis")
    
    print(f"\n📊 Overall Statistics:")
    print(f"   Total existing keywords: {stats['total_existing']}")
    print(f"   Total new keywords to add: {stats['total_new']}")
    print(f"   Total duplicates avoided: {stats['total_duplicates']}")
    
    if dry_run:
        print(f"\n🔍 DRY RUN MODE - No changes made to Redis")
        print(f"   Run with dry_run=False to actually add keywords")
    else:
        print(f"\n✅ Successfully added {stats['total_new']} new keywords to Redis!")
    
    return stats

def export_all_keywords():
    """Export all keywords from Redis to a JSON file for backup"""
    all_keywords = {}
    categories = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N']
    
    for category in categories:
        redis_key = f'category_keywords_{category}'
        keywords = list(redis_client.smembers(redis_key))
        all_keywords[category] = sorted(keywords)
    
    with open('redis_keywords_backup.json', 'w') as f:
        json.dump(all_keywords, f, indent=2)
    
    total_count = sum(len(keywords) for keywords in all_keywords.values())
    print(f"📦 Exported {total_count} keywords to redis_keywords_backup.json")
    
    return all_keywords

def main():
    print("🚀 Redis Keywords Management Script")
    print("=" * 50)
    
    try:
        # Test Redis connection
        redis_client.ping()
        print("✅ Connected to Redis successfully")
        
        # First, run in dry-run mode to see what would be added
        print(f"\n1️⃣ Running analysis (dry-run mode)...")
        stats = add_new_keywords(dry_run=True)
        
        # Ask user for confirmation
        if stats['total_new'] > 0:
            print(f"\n❓ Do you want to add {stats['total_new']} new keywords to Redis?")
            response = input("   Type 'yes' to confirm: ").lower().strip()
            
            if response == 'yes':
                print(f"\n2️⃣ Adding keywords to Redis...")
                add_new_keywords(dry_run=False)
                
                print(f"\n3️⃣ Creating backup...")
                export_all_keywords()
            else:
                print("❌ Operation cancelled by user")
        else:
            print("ℹ️ No new keywords to add")
            
    except redis.ConnectionError:
        print("❌ Failed to connect to Redis. Make sure Redis is running on localhost:6379")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
