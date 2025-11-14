from sqlalchemy import create_engine, text

DB_CONFIG_KPI = {
    "host": "162.19.251.55",
    "port": 3306,
    "user": "nidec",
    "password": "MaV38f5xsGQp83",
    "database": "Charges",
}

# Connexion SQLAlchemy
engine = create_engine(
    f"mysql+pymysql://{DB_CONFIG_KPI['user']}:{DB_CONFIG_KPI['password']}@"
    f"{DB_CONFIG_KPI['host']}:{DB_CONFIG_KPI['port']}/{DB_CONFIG_KPI['database']}"
)

query = """
SELECT 
    kcu.TABLE_NAME AS table_name,
    tc.CONSTRAINT_NAME AS constraint_name,
    tc.CONSTRAINT_TYPE AS constraint_type,
    GROUP_CONCAT(kcu.COLUMN_NAME ORDER BY kcu.ORDINAL_POSITION) AS columns
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc 
    ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME 
    AND kcu.TABLE_NAME = tc.TABLE_NAME
    AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
WHERE kcu.TABLE_SCHEMA = :schema
  AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
GROUP BY kcu.TABLE_NAME, tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE
ORDER BY kcu.TABLE_NAME, tc.CONSTRAINT_TYPE;
"""

with engine.connect() as conn:
    result = conn.execute(text(query), {"schema": DB_CONFIG_KPI['database']})
    rows = result.fetchall()

print("🔑 Clés trouvées dans la base 'Charges':\n")
for row in rows:
    table, key, key_type, columns = row
    print(f"📦 Table: {table}\n  🔹 {key_type}: {key} ➜ Colonnes: [{columns}]\n")
