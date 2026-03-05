"""
╔══════════════════════════════════════════════════════════════════╗
║          EARTH'S ROCKS  –  SQLite Database Generator             ║
║                                                                  ║
║  Run:  python earth_rocks_database.py                            ║
║  Requires only Python standard library (no pip installs)         ║
║                                                                  ║
║  Creates:  earth_rocks.db  (SQLite, 6 tables, 1 200+ rows)       ║
║                                                                  ║
║  DELIBERATELY INJECTED DATA QUALITY ISSUES                       ║
║  ─────────────────────────────────────────                       ║
║  MISSING DATA  (NULLs)  – realistic field-collection gaps        ║
║    • rock_samples.delta_o18              ~8%  NULL               ║
║      (isotope analysis not always performed in the field)        ║
║    • rock_samples.magnetic_susceptibility ~6%  NULL              ║
║      (instrument unavailable at remote sites)                    ║
║    • rock_samples.depth_collected_m      ~12% NULL               ║
║      (surface outcrop finds — depth not applicable)              ║
║    • rock_samples.porosity_pct            ~5%  NULL              ║
║      (lab measurement failed / sample too fragile)               ║
║    • lab_analyses.temperature_c          ~10% NULL               ║
║      (temperature not recorded for ambient analyses)             ║
║    • lab_analyses.pressure_mpa           ~10% NULL               ║
║      (atmospheric-pressure runs — value not logged)              ║
║                                                                  ║
║  DUPLICATE DATA  – realistic data-entry / import errors          ║
║    • 30 rock_samples rows duplicated with new sample_id          ║
║      but identical content (re-entry after logging error)        ║
║    • ~20 lab_analyses rows duplicated                            ║
║      (instrument software submitted result twice)                ║
╚══════════════════════════════════════════════════════════════════╝
"""

import sqlite3, random, math
from datetime import date, timedelta

# ── colours / formatting helpers ────────────────────────────────────────────
RESET   = "\033[0m";  BOLD = "\033[1m";  DIM  = "\033[2m"
CYAN    = "\033[96m"; GREEN= "\033[92m"; YELLOW="\033[93m"
BLUE    = "\033[94m"; MAGENTA="\033[95m";WHITE="\033[97m"
RED     = "\033[91m"

def header(text, colour=CYAN):
    w = 66
    print(f"\n{colour}{BOLD}{'═'*w}{RESET}\n{colour}{BOLD}  {text}{RESET}\n{colour}{BOLD}{'═'*w}{RESET}")

def sub(text, colour=YELLOW):
    print(f"\n{colour}{BOLD}  ▶  {text}{RESET}")

def note(text):
    print(f"  {DIM}ℹ  {text}{RESET}")

def table_print(rows, col_names, col_widths):
    sep  = "  " + "┼".join("─" * w for w in col_widths)
    head = "  " + "│".join(f"{BOLD}{CYAN}{n[:w]:<{w}}{RESET}" for n, w in zip(col_names, col_widths))
    print(head); print(sep)
    for row in rows:
        parts = []
        for val, w in zip(row, col_widths):
            if val is None:
                parts.append(f"{RED}{'NULL':<{w}}{RESET}")
            else:
                parts.append(f"{str(val)[:w]:<{w}}")
        print("  " + "│".join(parts))

# ════════════════════════════════════════════════════════════════════════════
#  1.  BUILD DATABASE
# ════════════════════════════════════════════════════════════════════════════
DB_PATH = "earth_rocks.db"
random.seed(42)
conn = sqlite3.connect(DB_PATH)
cur  = conn.cursor()
cur.execute("PRAGMA foreign_keys = ON;")

for _t in ["lab_analyses","mineral_composition","rock_samples",
           "collection_sites","geological_periods","rock_types"]:
    cur.execute(f"DROP TABLE IF EXISTS {_t};")

# ── TABLE 1 – rock_types ─────────────────────────────────────────────────────
cur.execute("""
CREATE TABLE rock_types (
    type_id           INTEGER PRIMARY KEY,
    type_name         TEXT NOT NULL UNIQUE,  -- nominal
    rock_class        TEXT NOT NULL,          -- nominal
    formation_process TEXT NOT NULL           -- nominal
)""")

rock_type_data = [
    (1,"Granite","Igneous","Intrusive cooling of magma"),
    (2,"Basalt","Igneous","Extrusive volcanic lava cooling"),
    (3,"Obsidian","Igneous","Rapid cooling of volcanic glass"),
    (4,"Pumice","Igneous","Frothy lava rapid solidification"),
    (5,"Rhyolite","Igneous","Extrusive felsic lava cooling"),
    (6,"Gabbro","Igneous","Slow intrusive mafic magma cooling"),
    (7,"Diorite","Igneous","Intermediate intrusive magma"),
    (8,"Sandstone","Sedimentary","Compaction of sand grains"),
    (9,"Limestone","Sedimentary","Accumulation of marine organisms"),
    (10,"Shale","Sedimentary","Compaction of clay and silt"),
    (11,"Conglomerate","Sedimentary","Cementation of rounded gravel"),
    (12,"Chalk","Sedimentary","Accumulation of microscopic shells"),
    (13,"Coal","Sedimentary","Compression of organic plant matter"),
    (14,"Chert","Sedimentary","Precipitation of silica"),
    (15,"Marble","Metamorphic","Metamorphism of limestone"),
    (16,"Quartzite","Metamorphic","Metamorphism of sandstone"),
    (17,"Slate","Metamorphic","Low-grade metamorphism of shale"),
    (18,"Schist","Metamorphic","Medium-grade metamorphism"),
    (19,"Gneiss","Metamorphic","High-grade metamorphism"),
    (20,"Phyllite","Metamorphic","Low-medium grade metamorphism of shale"),
]
cur.executemany("INSERT INTO rock_types VALUES (?,?,?,?)", rock_type_data)

# ── TABLE 2 – geological_periods ─────────────────────────────────────────────
cur.execute("""
CREATE TABLE geological_periods (
    period_id   INTEGER PRIMARY KEY,
    period_name TEXT NOT NULL UNIQUE,  -- nominal
    era         TEXT NOT NULL,         -- nominal
    start_mya   REAL NOT NULL,         -- ratio (millions of years ago)
    end_mya     REAL NOT NULL          -- ratio
)""")
cur.executemany("INSERT INTO geological_periods VALUES (?,?,?,?,?)", [
    (1,"Hadean","Precambrian",4600.0,4000.0),(2,"Archean","Precambrian",4000.0,2500.0),
    (3,"Proterozoic","Precambrian",2500.0,541.0),(4,"Cambrian","Paleozoic",541.0,485.4),
    (5,"Ordovician","Paleozoic",485.4,443.8),(6,"Silurian","Paleozoic",443.8,419.2),
    (7,"Devonian","Paleozoic",419.2,358.9),(8,"Carboniferous","Paleozoic",358.9,298.9),
    (9,"Permian","Paleozoic",298.9,251.9),(10,"Triassic","Mesozoic",251.9,201.3),
    (11,"Jurassic","Mesozoic",201.3,145.0),(12,"Cretaceous","Mesozoic",145.0,66.0),
    (13,"Paleogene","Cenozoic",66.0,23.0),(14,"Neogene","Cenozoic",23.0,2.6),
    (15,"Quaternary","Cenozoic",2.6,0.0),
])

# ── TABLE 3 – collection_sites ───────────────────────────────────────────────
cur.execute("""
CREATE TABLE collection_sites (
    site_id     INTEGER PRIMARY KEY,
    site_name   TEXT NOT NULL,  -- nominal
    country     TEXT NOT NULL,  -- nominal
    continent   TEXT NOT NULL,  -- nominal
    latitude    REAL NOT NULL,  -- interval (arbitrary zero, can be negative)
    longitude   REAL NOT NULL,  -- interval
    elevation_m REAL NOT NULL   -- ratio (true zero = sea level)
)""")
sites_raw = [
    ("Yosemite Valley","USA","North America",37.74,-119.59,1214.0),
    ("Grand Canyon","USA","North America",36.10,-112.10,760.0),
    ("Yellowstone Caldera","USA","North America",44.43,-110.67,2357.0),
    ("Kilauea Volcano","USA","North America",19.41,-155.28,1222.0),
    ("Appalachian Highlands","USA","North America",37.00,-80.00,800.0),
    ("Canadian Shield","Canada","North America",55.00,-85.00,300.0),
    ("Andes Mountains","Peru","South America",-9.19,-75.01,4200.0),
    ("Patagonian Plateau","Argentina","South America",-45.00,-69.00,600.0),
    ("Amazon Basin","Brazil","South America",-3.00,-60.00,80.0),
    ("Alps","Switzerland","Europe",46.50,8.00,2500.0),
    ("Scottish Highlands","UK","Europe",57.12,-4.20,750.0),
    ("Massif Central","France","Europe",45.50,3.00,900.0),
    ("Sahara Desert","Algeria","Africa",23.00,5.00,400.0),
    ("East African Rift","Kenya","Africa",-1.00,37.00,1500.0),
    ("Drakensberg","South Africa","Africa",-29.60,29.40,2500.0),
    ("Himalayas","Nepal","Asia",27.98,86.92,5500.0),
    ("Siberian Traps","Russia","Asia",60.00,90.00,300.0),
    ("Deccan Plateau","India","Asia",18.00,76.00,550.0),
    ("Mount Fuji","Japan","Asia",35.36,138.73,3776.0),
    ("Pilbara Craton","Australia","Australia",-22.00,118.00,500.0),
    ("Snowy Mountains","Australia","Australia",-36.45,148.26,1800.0),
    ("Antarctica Ice Sheet","Antarctica","Antarctica",-82.00,35.00,2300.0),
    ("Transantarctic Mountains","Antarctica","Antarctica",-85.00,175.00,3000.0),
    ("Iceland Rift Zone","Iceland","Europe",64.96,-19.02,800.0),
    ("Hawaiian Ridge","USA","North America",20.80,-156.30,0.0),
]
cur.executemany("INSERT INTO collection_sites VALUES (?,?,?,?,?,?,?)",
                [(i+1,)+t for i,t in enumerate(sites_raw)])

# ── TABLE 4 – rock_samples  (main fact table, 1 200 base rows) ───────────────
#
# NULLABLE columns — deliberate missing data:
#   delta_o18               NULL ~8%  Isotope lab analysis not always performed
#   magnetic_susceptibility NULL ~6%  Field magnetometer unavailable at remote sites
#   depth_collected_m       NULL ~12% Surface outcrop finds; depth not applicable
#   porosity_pct            NULL ~5%  Sample too fragile for mercury porosimetry
#
cur.execute("""
CREATE TABLE rock_samples (
    sample_id               INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_code             TEXT NOT NULL UNIQUE,    -- nominal
    type_id                 INTEGER NOT NULL REFERENCES rock_types(type_id),
    site_id                 INTEGER NOT NULL REFERENCES collection_sites(site_id),
    period_id               INTEGER NOT NULL REFERENCES geological_periods(period_id),
    color                   TEXT NOT NULL,            -- nominal
    texture                 TEXT NOT NULL,            -- nominal
    luster                  TEXT NOT NULL,            -- nominal
    hardness_mohs           REAL NOT NULL,            -- ordinal  (Mohs 1–10)
    weathering_grade        TEXT NOT NULL,            -- ordinal  (5 ordered levels)
    grain_size_class        TEXT NOT NULL,            -- ordinal  (4 ordered levels)
    collection_year         INTEGER NOT NULL,         -- interval (no true zero)
    delta_o18               REAL,                     -- interval (NULL ~8%: lab unavailable)
    magnetic_susceptibility REAL,                     -- interval (NULL ~6%: instrument absent)
    mass_kg                 REAL NOT NULL,            -- ratio
    length_cm               REAL NOT NULL,            -- ratio
    width_cm                REAL NOT NULL,            -- ratio
    height_cm               REAL NOT NULL,            -- ratio
    silica_pct              REAL NOT NULL,            -- ratio
    feldspar_pct            REAL NOT NULL,            -- ratio
    quartz_pct              REAL NOT NULL,            -- ratio
    porosity_pct            REAL,                     -- ratio   (NULL ~5%: sample fragile)
    density_gcc             REAL NOT NULL,            -- ratio
    depth_collected_m       REAL,                     -- ratio   (NULL ~12%: surface outcrop)
    radioactivity_bq        REAL NOT NULL             -- ratio
)""")

colors_by_class = {
    "Igneous":    ["Black","Dark Grey","Pink","Grey","White","Speckled Black-White","Dark Green","Reddish Brown","Light Grey"],
    "Sedimentary":["Tan","Brown","Yellow","Grey","White","Cream","Reddish Brown","Dark Grey","Black","Blue-Grey"],
    "Metamorphic":["Silver-Grey","White","Pink","Dark Grey","Banded Grey-White","Green","Blue-Grey","Black","Mottled White"],
}
textures_by_class = {
    "Igneous":    ["Porphyritic","Glassy","Vesicular","Coarse-grained","Fine-grained","Pegmatitic","Aphanitic"],
    "Sedimentary":["Clastic","Crystalline","Fossiliferous","Oolitic","Massive","Laminated","Cross-bedded"],
    "Metamorphic":["Foliated","Non-foliated","Lineated","Schistose","Gneissic","Slaty","Granoblastic"],
}
lusters         = ["Vitreous","Resinous","Pearly","Silky","Adamantine","Metallic","Earthy","Waxy"]
weathering_opts = ["Fresh","Slightly Weathered","Moderately Weathered","Highly Weathered","Completely Weathered"]
weathering_wts  = [0.20,0.30,0.25,0.15,0.10]
grain_opts      = ["Fine","Medium","Coarse","Very Coarse"]
hardness_range  = {"Igneous":(5.0,7.5),"Sedimentary":(1.5,6.5),"Metamorphic":(3.0,8.0)}
silica_range    = {"Igneous":(45,78),"Sedimentary":(2,85),"Metamorphic":(20,80)}
type_class_map  = {r[0]:r[2] for r in rock_type_data}

# ── Missing-data probability constants (named for clarity in the code) ───────
PROB_NULL_DELTA_O18  = 0.08   # 8%  – isotope analysis not performed
PROB_NULL_MAG_SUSC   = 0.06   # 6%  – magnetometer unavailable at site
PROB_NULL_DEPTH      = 0.12   # 12% – surface outcrop, depth not applicable
PROB_NULL_POROSITY   = 0.05   # 5%  – sample too fragile for porosimetry

sample_rows = []
for i in range(1, 1201):
    tid = random.randint(1,20); sid = random.randint(1,25); pid = random.randint(1,15)
    rc  = type_class_map[tid]
    sio2 = round(random.uniform(*silica_range[rc]),2)
    sample_rows.append((
        f"{rc[:3].upper()}-{i:05d}", tid, sid, pid,
        random.choice(colors_by_class[rc]),
        random.choice(textures_by_class[rc]),
        random.choice(lusters),
        round(random.uniform(*hardness_range[rc])*2)/2,
        random.choices(weathering_opts,weathering_wts)[0],
        random.choice(grain_opts),
        random.randint(1960,2024),
        None if random.random()<PROB_NULL_DELTA_O18  else round(random.gauss(-8.0,6.0),3),
        None if random.random()<PROB_NULL_MAG_SUSC   else round(random.gauss(50,200),4),
        round(random.lognormvariate(math.log(0.8),0.8),3),
        round(random.uniform(3,45),1), round(random.uniform(2,30),1), round(random.uniform(1,20),1),
        sio2,
        round(random.uniform(0,max(0,100-sio2-5)),2),
        round(min(sio2*0.6,random.uniform(0,50)),2),
        None if random.random()<PROB_NULL_POROSITY   else round(random.uniform(0.1,30.0),2),
        round(random.uniform(1.8,3.5),3),
        None if random.random()<PROB_NULL_DEPTH      else round(random.uniform(0.0,500.0),1),
        round(abs(random.gauss(15,20)),2),
    ))

cur.executemany("""
INSERT INTO rock_samples
  (sample_code,type_id,site_id,period_id,color,texture,luster,
   hardness_mohs,weathering_grade,grain_size_class,
   collection_year,delta_o18,magnetic_susceptibility,
   mass_kg,length_cm,width_cm,height_cm,
   silica_pct,feldspar_pct,quartz_pct,porosity_pct,
   density_gcc,depth_collected_m,radioactivity_bq)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", sample_rows)

# ── DUPLICATE rock_samples rows ──────────────────────────────────────────────
# Scenario: field technician re-entered 30 records after logging software
# wiped them; the originals were NOT deleted, leaving identical duplicates.
# Duplicates are flagged with a "COPY_" prefix on the sample_code.
NUM_SAMPLE_DUPES = 30
for orig_id in random.sample(range(1,1201), NUM_SAMPLE_DUPES):
    cur.execute("""SELECT type_id,site_id,period_id,color,texture,luster,
        hardness_mohs,weathering_grade,grain_size_class,collection_year,
        delta_o18,magnetic_susceptibility,mass_kg,length_cm,width_cm,height_cm,
        silica_pct,feldspar_pct,quartz_pct,porosity_pct,density_gcc,
        depth_collected_m,radioactivity_bq,sample_code
        FROM rock_samples WHERE sample_id=?""", (orig_id,))
    row = cur.fetchone()
    cur.execute("""
        INSERT INTO rock_samples
          (sample_code,type_id,site_id,period_id,color,texture,luster,
           hardness_mohs,weathering_grade,grain_size_class,collection_year,
           delta_o18,magnetic_susceptibility,mass_kg,length_cm,width_cm,height_cm,
           silica_pct,feldspar_pct,quartz_pct,porosity_pct,density_gcc,
           depth_collected_m,radioactivity_bq)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        ("COPY_"+row[-1],)+row[:-1])

# ── TABLE 5 – mineral_composition  (compound primary key) ────────────────────
cur.execute("""
CREATE TABLE mineral_composition (
    sample_id   INTEGER NOT NULL REFERENCES rock_samples(sample_id),
    mineral     TEXT    NOT NULL,  -- nominal
    abundance   TEXT    NOT NULL,  -- ordinal: Trace/Minor/Moderate/Major/Dominant
    weight_pct  REAL    NOT NULL,  -- ratio
    PRIMARY KEY (sample_id, mineral)
)""")
minerals_by_class = {
    "Igneous":    ["Quartz","Feldspar","Mica","Pyroxene","Olivine","Amphibole","Magnetite","Biotite","Hornblende","Plagioclase"],
    "Sedimentary":["Calcite","Quartz","Clay","Dolomite","Gypsum","Halite","Feldspar","Chert","Pyrite","Limonite"],
    "Metamorphic":["Garnet","Mica","Feldspar","Quartz","Chlorite","Biotite","Hornblende","Kyanite","Staurolite","Sillimanite"],
}
def abund(p):
    return "Trace" if p<5 else "Minor" if p<15 else "Moderate" if p<30 else "Major" if p<50 else "Dominant"

cur.execute("SELECT sample_id, type_id FROM rock_samples")
all_samples = cur.fetchall()
min_rows = []
for (sid,tid) in all_samples:
    rc = type_class_map.get(tid,"Igneous")
    chosen = random.sample(minerals_by_class[rc], random.randint(2,5))
    ws = [random.uniform(1,10) for _ in chosen]; tot=sum(ws)
    ws = [round(w/tot*100,2) for w in ws]; ws[-1]=round(100-sum(ws[:-1]),2)
    for m,w in zip(chosen,ws): min_rows.append((sid,m,abund(max(0.01,w)),max(0.01,w)))
cur.executemany("INSERT OR IGNORE INTO mineral_composition VALUES (?,?,?,?)", min_rows)

# ── TABLE 6 – lab_analyses  (FK → rock_samples) ──────────────────────────────
#
# NULLABLE columns — deliberate missing data:
#   temperature_c  NULL ~10% – ambient-condition analyses; temperature not recorded
#   pressure_mpa   NULL ~10% – atmospheric-pressure runs; value not logged
#
cur.execute("""
CREATE TABLE lab_analyses (
    analysis_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id      INTEGER NOT NULL REFERENCES rock_samples(sample_id),
    analysis_type  TEXT NOT NULL,   -- nominal
    lab_name       TEXT NOT NULL,   -- nominal
    analyst_grade  TEXT NOT NULL,   -- ordinal: Junior/Mid/Senior/Principal
    analysis_date  TEXT NOT NULL,   -- ISO-8601 date
    temperature_c  REAL,            -- interval (NULL ~10%: ambient run, not recorded)
    pressure_mpa   REAL,            -- ratio    (NULL ~10%: atmospheric run, not logged)
    result_value   REAL NOT NULL,   -- ratio
    result_unit    TEXT NOT NULL    -- nominal
)""")

analysis_types=[("XRF Spectroscopy","wt%"),("ICP-MS Trace Elements","ppm"),
                ("Thin Section Petrography","index"),("Raman Spectroscopy","cm-1"),
                ("Radiometric Dating","Ma"),("Neutron Activation","ppm"),("SEM-EDS","at%")]
labs=["GeoLab Oxford","MIT Earth Sciences","USGS Denver Lab","CSIRO Geoscience","ETH Zürich Earth","BGS Keyworth"]
analyst_grades=["Junior","Mid","Senior","Principal"]; analyst_wts=[0.25,0.35,0.25,0.15]
base_date=date(1990,1,1); total_days=(date(2024,12,31)-base_date).days

# Missing-data probability constants for lab_analyses
PROB_NULL_TEMP     = 0.10   # 10% – ambient analysis, temperature not recorded
PROB_NULL_PRESSURE = 0.10   # 10% – atmospheric-pressure run, value not logged

lab_rows=[]
for (sid,_) in all_samples:
    for _ in range(random.randint(1,3)):
        atype,unit=random.choice(analysis_types)
        lab_rows.append((sid,atype,random.choice(labs),
            random.choices(analyst_grades,analyst_wts)[0],
            (base_date+timedelta(days=random.randint(0,total_days))).isoformat(),
            None if random.random()<PROB_NULL_TEMP     else round(random.uniform(18,25),1),
            None if random.random()<PROB_NULL_PRESSURE else round(random.uniform(0.1,800),2),
            round(abs(random.gauss(30,25)),4), unit))
cur.executemany("""
INSERT INTO lab_analyses
  (sample_id,analysis_type,lab_name,analyst_grade,analysis_date,
   temperature_c,pressure_mpa,result_value,result_unit)
VALUES (?,?,?,?,?,?,?,?,?)""", lab_rows)

# ── DUPLICATE lab_analyses rows ───────────────────────────────────────────────
# Scenario: instrument firmware bug on 20 occasions submitted the same
# result record twice before a patch was applied.
NUM_LAB_DUPES = 20
cur.execute("SELECT analysis_id FROM lab_analyses ORDER BY RANDOM() LIMIT ?", (NUM_LAB_DUPES,))
for (aid,) in cur.fetchall():
    cur.execute("""SELECT sample_id,analysis_type,lab_name,analyst_grade,
        analysis_date,temperature_c,pressure_mpa,result_value,result_unit
        FROM lab_analyses WHERE analysis_id=?""", (aid,))
    cur.execute("""INSERT INTO lab_analyses
        (sample_id,analysis_type,lab_name,analyst_grade,analysis_date,
         temperature_c,pressure_mpa,result_value,result_unit)
        VALUES (?,?,?,?,?,?,?,?,?)""", cur.fetchone())

conn.commit()

# ════════════════════════════════════════════════════════════════════════════
#  2.  DISPLAY REPORT
# ════════════════════════════════════════════════════════════════════════════
header("🪨  EARTH'S ROCKS DATABASE  —  Summary Report", CYAN)

sub("Table Overview", MAGENTA)
for t in ["rock_types","geological_periods","collection_sites","rock_samples","mineral_composition","lab_analyses"]:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    cnt=cur.fetchone()[0]
    print(f"  {GREEN}{t:<28}{RESET}  {WHITE}{cnt:>6,} rows  {DIM}{'█'*min(cnt//100,40)}{RESET}")

sub("Rock Types (all 20)", YELLOW)
cur.execute("SELECT type_id,type_name,rock_class,formation_process FROM rock_types ORDER BY rock_class,type_name")
table_print(cur.fetchall(),["ID","Rock Name","Class","Formation Process"],[4,16,14,40])

sub("Geological Periods", YELLOW)
cur.execute("SELECT period_id,period_name,era,start_mya,end_mya FROM geological_periods ORDER BY start_mya DESC")
table_print(cur.fetchall(),["ID","Period","Era","Start (Mya)","End (Mya)"],[4,16,14,12,12])

sub("Collection Sites", YELLOW)
cur.execute("SELECT site_id,site_name,country,continent,latitude,longitude,elevation_m FROM collection_sites ORDER BY continent,site_name")
table_print(cur.fetchall(),["ID","Site Name","Country","Continent","Lat","Lon","Elev(m)"],[4,26,14,14,8,9,8])

sub("Rock Samples — 15 clean rows  (NULLs shown in red)", YELLOW)
note("Columns with deliberate NULLs: delta_o18, magnetic_susceptibility, porosity_pct, depth_collected_m")
cur.execute("""SELECT rs.sample_code,rt.type_name,rt.rock_class,cs.site_name,rs.color,
    rs.hardness_mohs,rs.weathering_grade,rs.collection_year,
    rs.delta_o18,rs.magnetic_susceptibility,rs.porosity_pct,rs.depth_collected_m
FROM rock_samples rs
JOIN rock_types rt ON rs.type_id=rt.type_id
JOIN collection_sites cs ON rs.site_id=cs.site_id
WHERE rs.sample_code NOT LIKE 'COPY_%' LIMIT 15""")
table_print(cur.fetchall(),
    ["Code","Type","Class","Site","Color","Mohs","Weathering","Year","δ18O","MagSus","Poros%","Depth_m"],
    [12,14,12,22,18,5,22,5,8,9,7,8])

sub(f"DUPLICATE Rock Samples — {NUM_SAMPLE_DUPES} re-entered rows (COPY_ prefix)", RED)
note("Original row still exists; duplicate has new sample_id but all data columns are identical")
cur.execute("""SELECT sample_id,sample_code,type_id,site_id,collection_year,mass_kg,density_gcc
FROM rock_samples WHERE sample_code LIKE 'COPY_%' LIMIT 12""")
table_print(cur.fetchall(),["ID","Sample Code","TypeID","SiteID","Year","kg","g/cc"],[6,16,7,7,6,7,7])

sub("Lab Analyses — 15 rows  (NULLs shown in red)", YELLOW)
note("Columns with deliberate NULLs: temperature_c, pressure_mpa")
cur.execute("""SELECT la.analysis_id,rs.sample_code,la.analysis_type,la.lab_name,
    la.analyst_grade,la.analysis_date,la.temperature_c,la.pressure_mpa,la.result_value,la.result_unit
FROM lab_analyses la JOIN rock_samples rs ON la.sample_id=rs.sample_id LIMIT 15""")
table_print(cur.fetchall(),
    ["AID","Sample","Analysis Type","Lab","Grade","Date","Temp°C","Pres MPa","Value","Unit"],
    [5,12,26,22,8,12,8,9,8,6])

sub("Mineral Composition — 12 rows (compound PK: sample_id + mineral)", YELLOW)
cur.execute("""SELECT mc.sample_id,rs.sample_code,rt.rock_class,mc.mineral,mc.abundance,mc.weight_pct
FROM mineral_composition mc
JOIN rock_samples rs ON mc.sample_id=rs.sample_id
JOIN rock_types rt ON rs.type_id=rt.type_id LIMIT 12""")
table_print(cur.fetchall(),["SID","Sample Code","Class","Mineral","Abundance","Wt%"],[5,12,12,14,12,8])

# ════════════════════════════════════════════════════════════════════════════
#  3.  DATA QUALITY AUDIT
# ════════════════════════════════════════════════════════════════════════════
header("🔍  Data Quality Audit — Missing & Duplicate Counts", RED)

sub("Missing Data (NULL counts per nullable column)", RED)
null_checks = [
    ("rock_samples","delta_o18",             "Isotope lab not available at collection site",    PROB_NULL_DELTA_O18),
    ("rock_samples","magnetic_susceptibility","Magnetometer unavailable at remote locations",   PROB_NULL_MAG_SUSC),
    ("rock_samples","depth_collected_m",      "Surface outcrop — depth is not applicable",      PROB_NULL_DEPTH),
    ("rock_samples","porosity_pct",           "Sample too fragile for mercury porosimetry",     PROB_NULL_POROSITY),
    ("lab_analyses","temperature_c",          "Ambient-condition run; temperature not logged",  PROB_NULL_TEMP),
    ("lab_analyses","pressure_mpa",           "Atmospheric pressure run; value not logged",     PROB_NULL_PRESSURE),
]
for tbl,col,reason,target in null_checks:
    cur.execute(f"SELECT COUNT(*) FROM {tbl}"); total=cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM {tbl} WHERE {col} IS NULL"); nulls=cur.fetchone()[0]
    pct=nulls/total*100
    print(f"  {CYAN}{tbl}.{col:<30}{RESET} {RED}{nulls:>4} NULLs{RESET} {WHITE}({pct:4.1f}% of {total:,}  target≈{target*100:.0f}%){RESET}")
    print(f"    {DIM}↳ {reason}{RESET}")

sub("Duplicate Data", RED)
cur.execute("SELECT COUNT(*) FROM rock_samples WHERE sample_code LIKE 'COPY_%'")
print(f"  {CYAN}rock_samples duplicates       {RESET}: {RED}{cur.fetchone()[0]} rows{RESET}")
print(f"    {DIM}↳ Field technician re-entered records after logging software error{RESET}")
print(f"    {DIM}↳ Detect with: sample_code LIKE 'COPY_%'  or  GROUP BY all non-PK cols{RESET}")

cur.execute("""SELECT SUM(cnt-1) FROM (
    SELECT COUNT(*) AS cnt FROM lab_analyses
    GROUP BY sample_id,analysis_type,lab_name,analyst_grade,
             analysis_date,temperature_c,pressure_mpa,result_value,result_unit
    HAVING cnt>1)""")
print(f"  {CYAN}lab_analyses duplicates       {RESET}: {RED}{cur.fetchone()[0] or 0} extra rows{RESET}")
print(f"    {DIM}↳ Instrument firmware bug submitted same result record twice{RESET}")
print(f"    {DIM}↳ Detect with: GROUP BY all non-PK columns HAVING COUNT(*) > 1{RESET}")

# ════════════════════════════════════════════════════════════════════════════
#  4.  ANALYTICAL QUERIES
# ════════════════════════════════════════════════════════════════════════════
header("📊  Analytical Query Results", BLUE)

sub("A) Sample count by rock class (NOMINAL — clean rows only)")
cur.execute("""SELECT rt.rock_class, COUNT(*) AS n,
    ROUND(COUNT(*)*100.0/(SELECT COUNT(*) FROM rock_samples WHERE sample_code NOT LIKE 'COPY_%'),1) AS pct
FROM rock_samples rs JOIN rock_types rt ON rs.type_id=rt.type_id
WHERE rs.sample_code NOT LIKE 'COPY_%'
GROUP BY rt.rock_class ORDER BY n DESC""")
table_print(cur.fetchall(),["Rock Class","# Samples","% of Total"],[16,12,14])

sub("B) Weathering grade distribution (ORDINAL — ordered by severity)")
cur.execute("""SELECT weathering_grade, COUNT(*) AS n,
    ROUND(COUNT(*)*100.0/(SELECT COUNT(*) FROM rock_samples WHERE sample_code NOT LIKE 'COPY_%'),1) AS pct
FROM rock_samples WHERE sample_code NOT LIKE 'COPY_%' GROUP BY weathering_grade
ORDER BY CASE weathering_grade
    WHEN 'Fresh' THEN 1 WHEN 'Slightly Weathered' THEN 2
    WHEN 'Moderately Weathered' THEN 3 WHEN 'Highly Weathered' THEN 4
    WHEN 'Completely Weathered' THEN 5 END""")
table_print(cur.fetchall(),["Weathering Grade","Count","Pct%"],[26,8,8])

sub("C) δ¹⁸O stats by class (INTERVAL — NULL count shown; AVG ignores NULLs)")
cur.execute("""SELECT rt.rock_class, COUNT(*) AS total,
    SUM(CASE WHEN rs.delta_o18 IS NULL THEN 1 ELSE 0 END) AS missing,
    ROUND(AVG(rs.delta_o18),3) AS avg_d18O,
    ROUND(MIN(rs.delta_o18),3) AS min_d18O,
    ROUND(MAX(rs.delta_o18),3) AS max_d18O
FROM rock_samples rs JOIN rock_types rt ON rs.type_id=rt.type_id
WHERE rs.sample_code NOT LIKE 'COPY_%' GROUP BY rt.rock_class""")
table_print(cur.fetchall(),["Rock Class","N","NULL δ18O","Avg δ18O","Min δ18O","Max δ18O"],[16,6,10,10,10,10])

sub("D) Average density & SiO₂ by rock type (RATIO data)")
cur.execute("""SELECT rt.type_name,rt.rock_class,
    ROUND(AVG(rs.density_gcc),3),ROUND(AVG(rs.mass_kg),3),ROUND(AVG(rs.silica_pct),1),COUNT(*)
FROM rock_samples rs JOIN rock_types rt ON rs.type_id=rt.type_id
WHERE rs.sample_code NOT LIKE 'COPY_%'
GROUP BY rt.type_name,rt.rock_class ORDER BY AVG(rs.density_gcc) DESC""")
table_print(cur.fetchall(),["Rock Type","Class","Avg g/cc","Avg kg","Avg SiO2%","N"],[14,14,10,10,12,6])

sub("E) Most common dominant mineral per rock class")
cur.execute("""SELECT rt.rock_class,mc.mineral,COUNT(*) AS n
FROM mineral_composition mc JOIN rock_samples rs ON mc.sample_id=rs.sample_id
JOIN rock_types rt ON rs.type_id=rt.type_id
WHERE mc.abundance='Dominant' AND rs.sample_code NOT LIKE 'COPY_%'
GROUP BY rt.rock_class,mc.mineral ORDER BY rt.rock_class,n DESC""")
table_print(cur.fetchall(),["Rock Class","Mineral","Dominant Count"],[16,16,16])

sub("F) High-elevation sites (>2000 m) with sample counts")
cur.execute("""SELECT cs.site_name,cs.country,cs.elevation_m,COUNT(rs.sample_id) AS n
FROM collection_sites cs
LEFT JOIN rock_samples rs ON cs.site_id=rs.site_id AND rs.sample_code NOT LIKE 'COPY_%'
WHERE cs.elevation_m>2000 GROUP BY cs.site_id ORDER BY cs.elevation_m DESC""")
table_print(cur.fetchall(),["Site","Country","Elev(m)","Samples"],[26,14,10,10])

sub("G) Lab analysis count by analyst grade (ORDINAL)")
cur.execute("""SELECT analyst_grade,COUNT(*) AS n,ROUND(AVG(result_value),3) AS avg_result
FROM lab_analyses GROUP BY analyst_grade
ORDER BY CASE analyst_grade WHEN 'Junior' THEN 1 WHEN 'Mid' THEN 2
WHEN 'Senior' THEN 3 WHEN 'Principal' THEN 4 END""")
table_print(cur.fetchall(),["Analyst Grade","# Analyses","Avg Result"],[16,14,14])

sub("H) Top 10 densest samples (RATIO subquery, clean rows only)")
cur.execute("SELECT ROUND(AVG(density_gcc),3) FROM rock_samples WHERE sample_code NOT LIKE 'COPY_%'")
print(f"  {DIM}Overall average density: {WHITE}{cur.fetchone()[0]} g/cc{RESET}")
cur.execute("""SELECT rs.sample_code,rt.type_name,rs.density_gcc,cs.site_name
FROM rock_samples rs JOIN rock_types rt ON rs.type_id=rt.type_id
JOIN collection_sites cs ON rs.site_id=cs.site_id
WHERE rs.density_gcc>(SELECT AVG(density_gcc) FROM rock_samples WHERE sample_code NOT LIKE 'COPY_%')
  AND rs.sample_code NOT LIKE 'COPY_%'
ORDER BY rs.density_gcc DESC LIMIT 10""")
table_print(cur.fetchall(),["Sample Code","Rock Type","Density g/cc","Site"],[14,16,14,26])

# ════════════════════════════════════════════════════════════════════════════
#  5.  DATA TYPE SUMMARY
# ════════════════════════════════════════════════════════════════════════════
header("📋  Data Type Column Summary", MAGENTA)
for dtype,desc,cols in [
    ("NOMINAL","Qualitative, categorical — no natural order",
     "rock_class, type_name, color, texture, luster, country, site_name, analysis_type, result_unit"),
    ("ORDINAL","Qualitative, ordered categories",
     "hardness_mohs (Mohs 1–10), weathering_grade (5 levels), grain_size_class (4 levels), analyst_grade (4 levels), mineral abundance (5 levels)"),
    ("INTERVAL","Quantitative, arbitrary zero — differences meaningful, ratios are not",
     "collection_year, delta_o18* (δ¹⁸O ‰ can be negative), magnetic_susceptibility*, latitude, longitude"),
    ("RATIO","Quantitative, true zero — all arithmetic operations valid",
     "mass_kg, density_gcc, silica_pct, porosity_pct*, depth_collected_m*, radioactivity_bq, elevation_m, start_mya, weight_pct, pressure_mpa*"),
]:
    print(f"\n  {BOLD}{CYAN}{dtype}{RESET}  {DIM}— {desc}{RESET}")
    words=cols.split(", "); line=[]; ll=0
    for w in words:
        if ll+len(w)>65: print(f"    {GREEN}"+", ".join(line)+f"{RESET}"); line=[w]; ll=len(w)
        else: line.append(w); ll+=len(w)+2
    if line: print(f"    {GREEN}"+", ".join(line)+f"{RESET}")
print(f"\n  {DIM}* columns marked with * contain deliberate NULLs (missing data){RESET}")

header("✅  Database saved to:  earth_rocks.db", GREEN)
print(f"  {DIM}Open with:    sqlite3 earth_rocks.db{RESET}")
print(f"  {DIM}Or in Python: conn = sqlite3.connect('earth_rocks.db'){RESET}\n")

conn.close()
