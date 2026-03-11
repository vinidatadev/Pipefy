import requests
import pandas as pd
import os
from dotenv import load_dotenv

# carregar variáveis do .env
load_dotenv()

pipefy_api_key = os.getenv("API_KEY")
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_API_KEY")

pipefy_url = "https://api.pipefy.com/graphql"

headers_pipefy = {
    "Authorization": f"Bearer {pipefy_api_key}",
    "Content-Type": "application/json"
}

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

# ===============================
# EXTRAÇÃO PIPEFY
# ===============================

response = requests.post(pipefy_url, json={"query": query}, headers=headers_pipefy)
data = response.json()

if "errors" in data:
    print("Erro na API Pipefy")
    print(data)
    exit()

rows = []

for card in data["data"]["allCards"]["edges"]:

    node = card["node"]

    row = {
        "codigo": node["id"],
        "titulo": node["title"],
        "criado_em": node["createdAt"],
        "finalizado_em": node["finished_at"],
        "criador": node["createdBy"]["name"] if node["createdBy"] else None,
        "fase_atual": node["current_phase"]["name"] if node["current_phase"] else None,
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

df = pd.DataFrame(rows)

print("Dados extraídos do Pipefy:")
print(df)

# ===============================
# TRATAMENTO
# ===============================

# converter NaN para None (JSON aceita null)
df = df.where(pd.notnull(df), None)

records = df.to_dict(orient="records")

# ===============================
# LOAD SUPABASE
# ===============================

endpoint = f"{supabase_url}/rest/v1/pipefy_cards"

headers_supabase = {
    "apikey": supabase_key,
    "Authorization": f"Bearer {supabase_key}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

response = requests.post(
    endpoint,
    headers=headers_supabase,
    json=records
)

print("Status envio Supabase:", response.status_code)
print("Resposta:", response.text)