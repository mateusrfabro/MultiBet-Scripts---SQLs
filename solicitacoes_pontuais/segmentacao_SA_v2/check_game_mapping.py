"""
Verifica fontes de mapping de jogos:
1. Super Nova DB schema multibet (tabelas/views com 'game' ou 'mapping')
2. Athena bireports_ec2.tbl_vendor_games_mapping_data (colunas reais)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from db.supernova import execute_supernova
from db.athena import query_athena


def main():
    print("=" * 80)
    print("1. Super Nova DB (multibet schema) — tabelas/views com 'game' ou 'mapping'")
    print("=" * 80)
    rows = execute_supernova(
        """
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = 'multibet'
          AND (lower(table_name) LIKE '%game%' OR lower(table_name) LIKE '%mapping%'
               OR lower(table_name) LIKE '%catalog%' OR lower(table_name) LIKE '%image%')
        ORDER BY table_name;
        """,
        fetch=True,
    )
    if rows:
        for r in rows:
            print(f"  {r[2]:<10} {r[0]}.{r[1]}")
    else:
        print("  (nada encontrado)")
        # tenta um filtro mais aberto
        print("\nListando TODAS as tabelas/views em multibet:")
        rows2 = execute_supernova(
            """
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = 'multibet'
            ORDER BY table_name;
            """,
            fetch=True,
        )
        for r in rows2:
            print(f"  {r[1]:<10} multibet.{r[0]}")

    print("\n" + "=" * 80)
    print("2. Athena bireports_ec2.tbl_vendor_games_mapping_data — colunas reais")
    print("=" * 80)
    try:
        df = query_athena("SHOW COLUMNS FROM bireports_ec2.tbl_vendor_games_mapping_data",
                           database="bireports_ec2")
        print(df.to_string(index=False))
        print("\nSAMPLE 3 linhas:")
        sample = query_athena(
            "SELECT * FROM bireports_ec2.tbl_vendor_games_mapping_data LIMIT 3",
            database="bireports_ec2"
        )
        for col in sample.columns:
            print(f"  {col}: {sample[col].iloc[0]}")
    except Exception as e:
        print(f"ERRO: {e}")


if __name__ == "__main__":
    main()
