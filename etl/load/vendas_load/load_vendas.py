# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.7
#   kernelspec:
#     display_name: venv
#     language: python
#     name: python3
# ---

# +
#####################################
### Carregar arquivos no supabase ###
#####################################
# -

# imports
from supabase import create_client, Client
import pandas as pd
import os

# Conexão com Supabase
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)
TABELA = "vendas"

# Apagando toda a tabela
supabase.table(TABELA).delete().neq("id", 0).execute()

# Resetando o ID da tabela
supabase.rpc("execute_sql", {
    "sql": "ALTER SEQUENCE id RESTART WITH 1;"
})

# Carregar dados tratados
df_to_load = pd.read_csv("/home/lucas/Documentos/empresa-lucas-dashboard/data/processed/vendas_processed/base_vendas_tratada.csv")

res = supabase.table("vendas").select("*").execute()
df_supabase_atual = pd.DataFrame(res.data)

chave = ["Data da Venda", "Produto", "Cliente", "Total (R$)"]

if df_supabase_atual.empty:
    print("🟢 Tabela Supabase vazia. Inserindo todos os dados.")
    df_para_inserir = df_to_load.copy()
else:
    df_merge = df_to_load.merge(df_supabase_atual[chave], on=chave, how="left", indicator=True)
    df_para_inserir = df_merge[df_merge["_merge"] == "left_only"].drop(columns=["_merge"])


print(f"📥 Linhas novas detectadas: {len(df_para_inserir)}")


# +
# Inserir no Supabase (Em lote)

if not df_para_inserir.empty:
    dados = df_para_inserir.to_dict(orient="records")
    supabase.table("vendas").insert(dados).execute()
    print("✅ Novos dados inseridos com sucesso!")
else:
    print("✅ Tabela Supabase já está atualizada com os dados mais recentes.")
