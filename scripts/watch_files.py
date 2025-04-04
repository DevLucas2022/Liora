import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
from dotenv import load_dotenv

load_dotenv()


BASE_DIR = Path().resolve()

# transform_script = BASE_DIR / "etl" / "transform" / "vendas_transform" / "tratar_vendas.py"
# load_script = BASE_DIR / "etl" / "load" / "vendas_load" / "load_vendas.py"

RAW_FODLER = Path(f"{BASE_DIR}/data/raw")
HASH_TRACKER = Path(f"{BASE_DIR}/config/hash_tracker.json")

def calcular_hash_md5(caminho_arquivo):
    with open(caminho_arquivo, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()
    
def carregar_hashes_anteriores():
    if not HASH_TRACKER.exists():
        return {}
    with open(HASH_TRACKER, "r") as f:
        return json.load(f)
    
def salvar_hashes_atuais(hashes):
    HASH_TRACKER.parent.mkdir(parents=True, exist_ok=True)
    with open(HASH_TRACKER, "w") as f:
        json.dump(hashes, f, indent=2)

def encontrar_csvs(pasta_base):
    return list(pasta_base.rglob("*.csv"))


def main():
    arquivos = encontrar_csvs(RAW_FODLER)
    hashes_anteriores = carregar_hashes_anteriores()
    novos_hashes = {}
    arquivos_alterados = []


    for arquivo in arquivos:
        hash_atual = calcular_hash_md5(arquivo)
        caminho_str = str(arquivo)

        if hashes_anteriores.get(caminho_str) != hash_atual:
            print(f"🟢 Alteração detectada: {caminho_str}")
            arquivos_alterados.append(caminho_str)            

        novos_hashes[caminho_str] = hash_atual

    if arquivos_alterados:
        for arquivo in arquivos_alterados:
            partes = Path(arquivo).parts
            empresa = None

            for parte in partes:
                if parte.endswith("_raw"):
                    empresa = parte.replace("_raw", "")
                    break
            if empresa:
                transform_script = BASE_DIR / f"etl/transform/{empresa}_transform/tratar_{empresa}.py"
                load_script = BASE_DIR / f"etl/load/{empresa}_load/load_{empresa}.py"
                print(f"\n🚀 Rodando pipeline para: {empresa}")
                
                python_venv = BASE_DIR / "venv" / "bin" / "python"
                env = os.environ.copy()

                subprocess.run([str(python_venv), str(transform_script)], check=True, env = env)
                subprocess.run([str(python_venv), str(load_script)], check=True, env = env)
            else:
                print(f"⚠️ Empresa não identificada para: {arquivo}")
    else:
        print("🔴 Nenhuma alteração detectada nos arquivos CSV.")


        print("🔄 Arquivos alterados:")
        for arquivo in arquivos_alterados:
            print(f" - {arquivo}")

    salvar_hashes_atuais(novos_hashes)
    
if __name__ == "__main__":
    main()