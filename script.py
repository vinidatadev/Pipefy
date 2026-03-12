import requests
import pandas as pd
import os
from dotenv import load_dotenv
import sys
import numpy as np
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===============================
# CONFIG
# ===============================

MAX_WORKERS = 10  # número de requisições paralelas

# ===============================
# CARREGAR VARIÁVEIS
# ===============================

load_dotenv()

pipefy_api_key = os.getenv("API_KEY")
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_API_KEY")

if not pipefy_api_key or not supabase_url or not supabase_key:
    print("Erro: variáveis de ambiente não carregadas.")
    sys.exit(1)

pipefy_url = "https://api.pipefy.com/graphql"

headers_pipefy = {
    "Authorization": f"Bearer {pipefy_api_key}",
    "Content-Type": "application/json"
}

# ===============================
# FUNÇÃO GERAR LINK PUBLICO
# ===============================

def gerar_link_publico(card_id):

    mutation = f"""
    mutation {{
      configurePublicPhaseFormLink(
        input: {{
          cardId: "{card_id}"
          enable: true
        }}
      ) {{
        url
      }}
    }}
    """

    try:

        response = requests.post(
            pipefy_url,
            headers=headers_pipefy,
            json={"query": mutation},
            timeout=15
        )

        data = response.json()

        return data["data"]["configurePublicPhaseFormLink"]["url"]

    except:
        return None


# ===============================
# QUERY PIPEFY
# ===============================

query = """
{
  allCards(pipeId: 306963265) {
    edges {
      node {
        id
        title
        createdAt
        finished_at
        createdBy {
          name
        }
        current_phase {
          name
        }
        fields {
          name
          value
        }
      }
    }
  }
}
"""

print("Consultando API Pipefy...")

response = requests.post(
    pipefy_url,
    json={"query": query},
    headers=headers_pipefy
)

if response.status_code != 200:
    print("Erro HTTP Pipefy:", response.status_code)
    sys.exit(1)

data = response.json()

if "errors" in data:
    print("Erro retornado pela API Pipefy:")
    print(data)
    sys.exit(1)

cards = data["data"]["allCards"]["edges"]

print(f"{len(cards)} cards encontrados")

# ===============================
# GERAR LINKS EM PARALELO
# ===============================

print("Gerando links públicos...")

card_links = {}

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

    futures = {
        executor.submit(gerar_link_publico, card["node"]["id"]): card["node"]["id"]
        for card in cards
    }

    for future in as_completed(futures):

        card_id = futures[future]

        try:
            link = future.result()
        except:
            link = None

        card_links[card_id] = link


# ===============================
# EXTRAÇÃO
# ===============================

rows = []

for card in cards:

    node = card["node"]
    card_id = node["id"]

    row = {
        "codigo": card_id,
        "titulo": node["title"],
        "criado_em": node["createdAt"],
        "finalizado_em": node["finished_at"],
        "criador": node["createdBy"]["name"] if node.get("createdBy") else None,
        "fase_atual": node["current_phase"]["name"] if node.get("current_phase") else None,
        "link_publico_card": card_links.get(card_id),
        "causa_do_fca": None,
        "setor_responsavel": None,
        "area_causadora": None,
        "acao": None,
        "uf": None,
        "numero_da_remessa": None,
        "empresa": None,
        "detalhe_observacao": None,
        "problema_solucionado": None,
        "detalhe_devolutiva_tratativa": None
    }

    for field in node["fields"]:

        nome = field["name"]
        valor = field["value"]

        if nome == "Causa do FCA":
            row["causa_do_fca"] = valor
        elif nome == "Setor Responsavel":
            row["setor_responsavel"] = valor
        elif nome == "Área causadora":
            row["area_causadora"] = valor
        elif nome == "Ação":
            row["acao"] = valor
        elif nome == "UF":
            row["uf"] = valor
        elif nome == "Número da Remessa":
            row["numero_da_remessa"] = valor
        elif nome == "Empresa":
            row["empresa"] = valor
        elif nome == "Detalhe/Observação":
            row["detalhe_observacao"] = valor
        elif nome == "1. Problema Solucionado?":
            row["problema_solucionado"] = valor
        elif nome == "2.Detalhe/Devolutiva da Tratativa":
            row["detalhe_devolutiva_tratativa"] = valor

    rows.append(row)

# ===============================
# TRANSFORM
# ===============================

df = pd.DataFrame(rows)

data_atualizacao = datetime.now().isoformat()
df["atualizado_em"] = data_atualizacao

print("Dados extraídos do Pipefy:")
print(df)

# ===============================
# LIMPEZA JSON
# ===============================

records = df.to_dict(orient="records")

def clean_value(value):

    if value is None:
        return None

    if isinstance(value, float):

        if np.isnan(value) or np.isinf(value):
            return None

    if pd.isna(value):
        return None

    return value


cleaned_records = []

for record in records:

    cleaned_record = {key: clean_value(value) for key, value in record.items()}

    cleaned_records.append(cleaned_record)

records = cleaned_records

print("Registros preparados:", len(records))

# validar json

json.dumps(records)

# ===============================
# LOAD SUPABASE
# ===============================

endpoint = f"{supabase_url}/rest/v1/pipefy_cards"

headers_supabase = {
    "apikey": supabase_key,
    "Authorization": f"Bearer {supabase_key}",
    "Content-Type": "application/json"
}

print("Limpando tabela...")

requests.delete(
    f"{endpoint}?codigo=neq.0",
    headers=headers_supabase
)

print("Inserindo dados...")

response = requests.post(
    endpoint,
    headers=headers_supabase,
    data=json.dumps(records, ensure_ascii=False)
)

print("Status:", response.status_code)

if response.status_code not in [200, 201]:

    print("Erro:", response.text)
    sys.exit(1)

print("ETL executado com sucesso!")