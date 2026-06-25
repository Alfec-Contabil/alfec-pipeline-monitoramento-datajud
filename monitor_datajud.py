#!/usr/bin/env python3
"""
monitor_datajud.py — Pipeline de monitoramento processual.

Fluxo:
  1. Lê config/processos.json (lista de processos monitorados).
  2. Para cada processo, consulta a API pública DataJud/CNJ.
  3. Compara os movimentos retornados com o que já está salvo em SQLite local.
  4. Para cada movimento NOVO, chama a API Anthropic (Claude Sonnet 4.6) para
     classificar em A / B / C (ver references/prompt-api-classificacao.md da skill
     monitoramento-processual-datajud).
  5. Se categoria C (ou B com urgência alta), aciona uma segunda chamada para gerar
     diagnóstico + minuta (com cabeçalho obrigatório de rascunho).
  6. Envia push via Telegram com o resultado.

Uso:
    python3 monitor_datajud.py                 # roda uma vez (uso via cron)
    python3 monitor_datajud.py --dry-run        # não envia push, só imprime
    python3 monitor_datajud.py --processo 0000000-00.0000.0.00.0000  # só 1 processo

Variáveis de ambiente obrigatórias:
    ANTHROPIC_API_KEY   — chave da API Anthropic (sk-ant-...)
    DATAJUD_API_KEY      — chave pública do DataJud (ver README, é pública e fixa,
                            mas o CNJ pode trocá-la; conferir em
                            https://datajud-wiki.cnj.jus.br/api-publica/acesso)

Configuração de processos e Telegram: config/processos.json
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "movimentos.db"
CONFIG_PATH = BASE_DIR / "config" / "processos.json"

DATAJUD_BASE_URL = "https://api-publica.datajud.cnj.jus.br/api_publica_{tribunal}/_search"
# Chave pública do DataJud — vigente em jun/2026. O CNJ pode alterá-la; se as
# consultas começarem a falhar com 401/403, conferir a chave atual em:
# https://datajud-wiki.cnj.jus.br/api-publica/acesso
DATAJUD_DEFAULT_KEY = (
    "cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw=="
)

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL = "claude-sonnet-4-6"

TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"

# ---------------------------------------------------------------------------
# Prompt de classificação (manter sincronizado com a skill
# monitoramento-processual-datajud/references/prompt-api-classificacao.md)
# ---------------------------------------------------------------------------
SYSTEM_PROMPT_CLASSIFICACAO = """\
Você é um classificador técnico de movimentações processuais para um escritório de
contabilidade/perícia que monitora processos judiciais via API DataJud/CNJ. Seu único
trabalho nesta chamada é classificar UM movimento processual em uma de três
categorias, com base nas regras abaixo. Você NÃO deve gerar minuta de petição nesta
etapa — isso é uma chamada separada, acionada apenas se a categoria for C.

CATEGORIAS:
- A = Andamento de serventia (ato cartorário/administrativo, sem efeito sobre
  direitos da parte: juntada, distribuição, certidão, remessa, conclusão)
- B = Atenção, sem ação imediata (movimento relevante mas que não abre prazo ou ação
  imediata para o cliente: designação de audiência futura, perícia designada,
  decisão favorável sujeita a recurso da contrária)
- C = Requer ação (abre prazo, exige manifestação, ou cria risco imediato: citação,
  intimação com prazo, penhora contra o cliente, sentença desfavorável, decisão
  liminar contra o cliente, bloqueio de valores, leilão designado, prazo do próprio
  cliente decorrido sem manifestação registrada)

REGRA DE DESEMPATE: na dúvida entre B e C, classifique como C. Custo de revisão
desnecessária é menor que custo de prazo perdido.

REGRA PARA MOVIMENTOS GENÉRICOS/TRUNCADOS: se o nome do movimento for vago ("Outras
decisões", "Ato ordinatório") sem permitir classificação segura, classifique como B
no mínimo, nunca como A.

LIMITAÇÃO A SEMPRE MENCIONAR NO CAMPO "ressalva_prazo": o DataJud não contém o texto
integral de publicações no DJe. Qualquer prazo mencionado é estimativa baseada no
tipo de movimento, não confirmação da contagem real — isso depende da publicação
oficial.

Você receberá: número do processo, tribunal, polo do cliente (réu/executado ou
autor/exequente), tipo de ação, e o JSON do movimento retornado pelo DataJud.

Responda SOMENTE com um objeto JSON neste formato exato, sem texto antes ou depois,
sem markdown, sem ```:

{
  "categoria": "A" | "B" | "C",
  "resumo_curto": "uma frase, linguagem direta, sem juridiquês",
  "implicacao": "o que isso muda na posição processual do cliente (vazio se A)",
  "ressalva_prazo": "texto sobre necessidade de confirmar no DJe, preenchido se B/C",
  "urgencia": "baixa" | "media" | "alta",
  "aciona_diagnostico_completo": true | false
}
"""

SYSTEM_PROMPT_DIAGNOSTICO = """\
Você é um assistente técnico de diagnóstico processual, atuando para um escritório
de contabilidade/perícia (não advocacia). Você recebe um movimento processual já
classificado como categoria C ("requer ação") e deve produzir:

1. Um diagnóstico estruturado no formato abaixo.
2. Quando solicitado e cabível, uma MINUTA preliminar da peça processual indicada,
   SEMPRE com o cabeçalho de alerta obrigatório.

Você NÃO é advogado e NÃO está autorizado a apresentar a minuta como peça final.
Toda minuta é rascunho técnico para revisão de advogado constituído no processo.

FORMATO DO DIAGNÓSTICO:

PROCESSO: [número CNJ] — [Tribunal]
POLO DO CLIENTE: [réu/executado | autor/exequente]
MOVIMENTO: [data] — [nome do movimento]
CATEGORIA: C

O QUE AGORA CONSTA NOS AUTOS:
[explicação em linguagem direta, sem juridiquês]

IMPLICAÇÃO PARA O CLIENTE:
[o que muda na posição processual]

PRAZO:
[prazo legal aplicável, citando lei/artigo, com ressalva de que a contagem real
depende de publicação no DJe/intimação pessoal — DataJud não confirma isso]

RECOMENDAÇÃO:
[curso de ação em uma frase]

Se o usuário pedir a minuta, gere-a com este cabeçalho OBRIGATÓRIO antes de qualquer
conteúdo:

⚠️ MINUTA AUTOMATIZADA — RASCUNHO PARA REVISÃO DE ADVOGADO
Gerada a partir de classificação automática de movimento processual via DataJud/CNJ.
NÃO PROTOCOLAR sem revisão integral por advogado constituído no processo, incluindo:
- Confirmação do prazo real (publicação no DJe / intimação)
- Verificação dos fatos e documentos dos autos (esta minuta não teve acesso aos
  autos completos, apenas ao metadado do movimento)
- Validação da estratégia e teses aplicáveis ao caso concreto

Se houver mais de uma peça cabível para a situação (ex.: embargos à execução vs.
exceção de pré-executividade, dependendo de garantia do juízo), apresente as opções
com o critério de escolha, sem decidir por conta própria qual usar.
"""


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}", flush=True)


def init_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS movimentos_vistos (
            numero_cnj TEXT NOT NULL,
            codigo INTEGER,
            nome TEXT,
            data_hora TEXT,
            hash_unico TEXT NOT NULL UNIQUE,
            categoria TEXT,
            visto_em TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def carregar_config() -> dict:
    if not CONFIG_PATH.exists():
        log(f"ERRO: arquivo de configuração não encontrado em {CONFIG_PATH}")
        sys.exit(1)
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def consultar_datajud(numero_cnj: str, tribunal_alias: str, api_key: str) -> dict | None:
    url = DATAJUD_BASE_URL.format(tribunal=tribunal_alias)
    headers = {
        "Authorization": f"APIKey {api_key}",
        "Content-Type": "application/json",
    }
    payload = {"query": {"match": {"numeroProcesso": numero_cnj.replace("-", "").replace(".", "")}}}
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        log(f"  ERRO ao consultar DataJud para {numero_cnj} ({tribunal_alias}): {e}")
        return None

    hits = data.get("hits", {}).get("hits", [])
    if not hits:
        log(f"  Nenhum resultado retornado pelo DataJud para {numero_cnj}.")
        return None
    # Em geral o primeiro hit é o processo; se houver múltiplos graus/instâncias
    # retornados, pode ser necessário ajustar para escolher o correto.
    return hits[0].get("_source", {})


def movimento_hash(numero_cnj: str, mov: dict) -> str:
    base = f"{numero_cnj}|{mov.get('codigo')}|{mov.get('dataHora')}|{mov.get('nome')}"
    return base


def chamar_anthropic(system_prompt: str, user_content: str, api_key: str, max_tokens: int = 600) -> str:
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }
    payload = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": max_tokens,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_content}],
    }
    resp = requests.post(ANTHROPIC_API_URL, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    partes_texto = [b["text"] for b in data.get("content", []) if b.get("type") == "text"]
    return "\n".join(partes_texto).strip()


def classificar_movimento(processo: dict, movimento: dict, anthropic_key: str) -> dict:
    user_content = json.dumps(
        {
            "numero_processo": processo["numero_cnj"],
            "tribunal": processo["tribunal_alias"],
            "polo_cliente": processo["polo_cliente"],
            "tipo_acao": processo.get("tipo_acao", ""),
            "movimento": movimento,
        },
        ensure_ascii=False,
    )
    texto = chamar_anthropic(SYSTEM_PROMPT_CLASSIFICACAO, user_content, anthropic_key, max_tokens=400)
    try:
        return json.loads(texto)
    except json.JSONDecodeError:
        log(f"  AVISO: resposta de classificação não veio em JSON válido: {texto[:200]}")
        # Fallback conservador: trata como C para não perder nada.
        return {
            "categoria": "C",
            "resumo_curto": "Falha ao interpretar classificação automática — verificar manualmente.",
            "implicacao": "",
            "ressalva_prazo": "Classificação automática falhou; tratar como urgente até revisão manual.",
            "urgencia": "alta",
            "aciona_diagnostico_completo": True,
        }


def gerar_diagnostico(processo: dict, movimento: dict, anthropic_key: str) -> str:
    user_content = (
        f"Gere o diagnóstico (sem minuta ainda) para este movimento:\n\n"
        + json.dumps(
            {
                "numero_processo": processo["numero_cnj"],
                "tribunal": processo["tribunal_alias"],
                "polo_cliente": processo["polo_cliente"],
                "tipo_acao": processo.get("tipo_acao", ""),
                "observacoes": processo.get("observacoes", ""),
                "movimento": movimento,
            },
            ensure_ascii=False,
        )
    )
    return chamar_anthropic(SYSTEM_PROMPT_DIAGNOSTICO, user_content, anthropic_key, max_tokens=900)


def enviar_telegram(token: str, chat_id: str, texto: str) -> None:
    if not token or "COLOQUE_AQUI" in token:
        log("  Telegram não configurado — pulando envio de push (ver config/processos.json).")
        return
    url = TELEGRAM_API_URL.format(token=token)
    # Telegram limita ~4096 caracteres por mensagem.
    for inicio in range(0, len(texto), 3500):
        bloco = texto[inicio:inicio + 3500]
        try:
            requests.post(url, json={"chat_id": chat_id, "text": bloco}, timeout=15)
        except requests.RequestException as e:
            log(f"  ERRO ao enviar push Telegram: {e}")


def montar_mensagem_push(processo: dict, movimento: dict, classificacao: dict, diagnostico: str | None) -> str:
    emoji = {"A": "🔵", "B": "🟡", "C": "🔴"}.get(classificacao["categoria"], "⚪")
    linhas = [
        f"{emoji} [{classificacao['categoria']}] Processo {processo['numero_cnj']} ({processo['tribunal_alias'].upper()})",
        f"Movimento: {movimento.get('nome')} — {movimento.get('dataHora')}",
        f"Resumo: {classificacao.get('resumo_curto', '')}",
    ]
    if classificacao.get("implicacao"):
        linhas.append(f"Implicação: {classificacao['implicacao']}")
    if classificacao.get("ressalva_prazo"):
        linhas.append(f"⚠️ {classificacao['ressalva_prazo']}")
    if diagnostico:
        linhas.append("\n--- DIAGNÓSTICO COMPLETO ---\n")
        linhas.append(diagnostico)
        adv = processo.get("advogado_responsavel", "")
        if not adv:
            linhas.append(
                "\n⚠️ Nenhum advogado responsável cadastrado para este processo — "
                "definir destinatário desta minuta antes de qualquer providência."
            )
        else:
            linhas.append(f"\nDestinatário sugerido para revisão: {adv}")
    return "\n".join(linhas)


def processar_processo(processo: dict, conn: sqlite3.Connection, datajud_key: str,
                        anthropic_key: str, telegram_cfg: dict, dry_run: bool) -> None:
    numero_cnj = processo["numero_cnj"]
    log(f"Consultando {numero_cnj} ({processo['tribunal_alias']})...")
    fonte = consultar_datajud(numero_cnj, processo["tribunal_alias"], datajud_key)
    if not fonte:
        return

    movimentos = fonte.get("movimentos", [])
    if not movimentos:
        log("  Sem movimentos retornados.")
        return

    novos = 0
    for mov in movimentos:
        h = movimento_hash(numero_cnj, mov)
        existe = conn.execute(
            "SELECT 1 FROM movimentos_vistos WHERE hash_unico = ?", (h,)
        ).fetchone()
        if existe:
            continue

        novos += 1
        log(f"  Movimento novo: {mov.get('nome')} ({mov.get('dataHora')})")
        classificacao = classificar_movimento(processo, mov, anthropic_key)

        diagnostico = None
        if classificacao.get("aciona_diagnostico_completo"):
            log(f"  Categoria {classificacao['categoria']} — gerando diagnóstico completo...")
            diagnostico = gerar_diagnostico(processo, mov, anthropic_key)

        mensagem = montar_mensagem_push(processo, mov, classificacao, diagnostico)

        if dry_run:
            print("\n" + "=" * 70)
            print(mensagem)
            print("=" * 70 + "\n")
        else:
            enviar_telegram(telegram_cfg.get("bot_token", ""), telegram_cfg.get("chat_id", ""), mensagem)

        conn.execute(
            """
            INSERT INTO movimentos_vistos
                (numero_cnj, codigo, nome, data_hora, hash_unico, categoria, visto_em)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                numero_cnj,
                mov.get("codigo"),
                mov.get("nome"),
                mov.get("dataHora"),
                h,
                classificacao.get("categoria"),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        time.sleep(1)  # respeitar rate limit do DataJud e da API Anthropic

    if novos == 0:
        log("  Nenhum movimento novo.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitoramento processual via DataJud + Claude.")
    parser.add_argument("--dry-run", action="store_true", help="Não envia push, apenas imprime no terminal.")
    parser.add_argument("--processo", help="Filtra apenas um número CNJ específico.")
    args = parser.parse_args()

    anthropic_key = os.environ.get("ANTHROPIC_API_KEY")
    if not anthropic_key:
        log("ERRO: variável de ambiente ANTHROPIC_API_KEY não definida.")
        sys.exit(1)
    datajud_key = os.environ.get("DATAJUD_API_KEY", DATAJUD_DEFAULT_KEY)

    config = carregar_config()
    processos = config.get("processos", [])
    if args.processo:
        processos = [p for p in processos if p["numero_cnj"] == args.processo]
        if not processos:
            log(f"Nenhum processo encontrado com número {args.processo} no config.")
            sys.exit(1)

    telegram_cfg = config.get("telegram", {})
    conn = init_db()

    log(f"Iniciando varredura de {len(processos)} processo(s)...")
    for processo in processos:
        for campo in ("numero_cnj", "tribunal_alias", "polo_cliente"):
            if not processo.get(campo):
                log(f"AVISO: processo sem campo obrigatório '{campo}' — pulando. {processo}")
                break
        else:
            processar_processo(processo, conn, datajud_key, anthropic_key, telegram_cfg, args.dry_run)

    conn.close()
    log("Varredura concluída.")


if __name__ == "__main__":
    main()
