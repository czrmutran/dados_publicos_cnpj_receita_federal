from src.db_models.utils import execute_sql_cmd


def create_view():
    drop_sql = "DROP VIEW IF EXISTS rf_unified_minimal CASCADE"
    execute_sql_cmd(drop_sql)

    sql = """
    CREATE VIEW rf_unified_minimal AS
    SELECT
      c.cnpj AS cnpj,
      r.name AS razao_social,
      c.trade_name AS nome_fantasia,
      c.cnae_main AS cnae_principal,
      trim(coalesce(c.address_city_name,'') || ' / ' || coalesce(c.address_fu,'')) AS municipio_uf,
      r.size_desc AS porte,
      c.situation_desc AS situacao_cadastral,
      CASE
        WHEN c.tel1 IS NOT NULL AND c.tel1 <> '' THEN '(' || coalesce(c.tel1_dd,'') || ') ' || c.tel1
        WHEN c.tel2 IS NOT NULL AND c.tel2 <> '' THEN '(' || coalesce(c.tel2_dd,'') || ') ' || c.tel2
        ELSE NULL
      END AS telefone,
      c.email AS email,
      c.foundation_date AS data_inicio_atividade
    FROM rf_company c
    LEFT JOIN rf_company_root r ON r.cnpj_root = c.cnpj_root;
    """
    execute_sql_cmd(sql)


if __name__ == "__main__":
    create_view()