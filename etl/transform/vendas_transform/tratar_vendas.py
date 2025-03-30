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

# imports
import os
import pandas as pd
import numpy as np
from pathlib import Path

# +
# Estrutura de diretórios

# empresa-lucas-dashboard/
# ├── data/
# │   ├── raw/base_vendas_supermercado_suja.csv
# │   └── processed/base_vendas_tratada.csv  ← vamos gerar aqui
# ├── etl/
# │   └── transform/tratar_vendas.py         ← script a ser criado
# ├── notebooks/
# │   └── tratar_vendas.ipynb           ← notebook a ser criado     


# +
# caminhos

#BASE_DIR = str(Path(os.getcwd()).resolve().parents[0])  # Para rodar no Jupyter Notebook
input_path = f"/home/lucas/Documentos/empresa-lucas-dashboard/data/raw/vendas_raw/base_vendas_supermercado_suja.csv"
output_path = f"/home/lucas/Documentos/empresa-lucas-dashboard/data/processed/vendas_processed/base_vendas_tratada.csv"


# +
# Funções
def transform_2_df(input_path):
    df = pd.read_csv(input_path)
    return df

def tratar_desconto(valor):
    """
    Output: valor na -> 0 
    Padronização dos dados: Se linha possui "%" -> ficará vazia ""
    """
    if pd.isna(valor):
        return 0.0
    if isinstance(valor, str):
        valor = valor.replace("%", "")
    try:
        return float(valor)
    except:
        return 0.0


# -

df_base_vendas_supermercado = transform_2_df(input_path)
df_base_vendas_supermercado.head(5)

# Tratando dados
# 
df_base_vendas_supermercado["Data da Venda"] = pd.to_datetime(df_base_vendas_supermercado["Data da Venda"], dayfirst=True)
df_base_vendas_supermercado["Vendedor"] = df_base_vendas_supermercado["Vendedor"].str.strip().str.title()
df_base_vendas_supermercado["Cliente"] = df_base_vendas_supermercado["Cliente"].str.strip().str.title()
df_base_vendas_supermercado["Forma de Pagamento"] = df_base_vendas_supermercado["Forma de Pagamento"].str.strip().str.capitalize()

df_base_vendas_supermercado["Desconto"]

df_base_vendas_supermercado['Desconto'] = df_base_vendas_supermercado["Desconto"].apply(tratar_desconto)
df_base_vendas_supermercado['Desconto (%)'] = df_base_vendas_supermercado["Desconto"]
df_base_vendas_supermercado["Total com Desconto (R$)"] = df_base_vendas_supermercado["Total (R$)"] * (1 - df_base_vendas_supermercado["Desconto"] / 100)

# Dropando colunas desnecessárias
df_base_vendas_supermercado.drop(columns=["Desconto"], inplace=True)

df_base_vendas_supermercado.head(5)

# Exportando CSV
df_base_vendas_supermercado.to_csv(output_path, index=False)

# +
# pegar caminho atual
#caminho_raiz = '/home/lucas/Documentos/empresa-lucas-dashboard'

# sincronizar para o arquivo .py
# !jupytext --to py $caminho_raiz/notebooks/tratar_vendas.ipynb -o $caminho_raiz/etl/transform/tratar_vendas.py
