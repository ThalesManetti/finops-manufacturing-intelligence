"""
=============================================================================
FinOps Manufacturing Intelligence — Geração de Dados Sintéticos
Segmento: Autopeças (Tier 2 — Fornecedor automotivo)
Período: Jan/2022 a Dez/2024 (36 meses)
=============================================================================
Autor: FinOps Pipeline
Descrição: Gera dados realistas de uma indústria de autopeças com:
    - 50 produtos em 4 linhas de produção
    - Sazonalidade automotiva (parada dez/jan, pico pré-lançamento)
    - Choque de commodities (aço, alumínio, polímeros)
    - Orçamento anual vs realizado
    - Histórico de mudanças de custo (base para SCD Type 2)
"""

import os
import random
import hashlib
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

import numpy as np
import pandas as pd
from faker import Faker

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================
fake = Faker("pt_BR")
np.random.seed(42)
random.seed(42)

OUTPUT_DIR = "/home/claude/data_output"
os.makedirs(f"{OUTPUT_DIR}/csv", exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/parquet", exist_ok=True)

PERIODO_INICIO = "2022-01-01"
PERIODO_FIM = "2024-12-31"
NUM_PRODUTOS = 50

# ============================================================================
# 1. LINHAS DE PRODUÇÃO E PRODUTOS
# ============================================================================

LINHAS_PRODUCAO = {
    "LP01": {
        "nome": "Estampados e Estruturais",
        "descricao": "Peças estampadas em aço e componentes estruturais",
        "commodity_principal": "aco",
        "produtos_base": [
            ("Suporte de motor", 45.00, 78.00),
            ("Travessa do painel", 62.00, 108.00),
            ("Reforço de coluna A", 38.00, 66.00),
            ("Suporte do radiador", 29.00, 50.00),
            ("Chapa de assoalho", 85.00, 148.00),
            ("Longarina dianteira", 120.00, 210.00),
            ("Suporte do amortecedor", 55.00, 96.00),
            ("Travessa do teto", 48.00, 84.00),
            ("Reforço de porta", 33.00, 58.00),
            ("Painel corta-fogo", 95.00, 166.00),
            ("Suporte do para-choque", 42.00, 73.00),
            ("Caixa de roda", 58.00, 101.00),
            ("Reforço do capô", 36.00, 63.00),
        ],
    },
    "LP02": {
        "nome": "Usinados de Precisão",
        "descricao": "Componentes usinados em alumínio e aço liga",
        "commodity_principal": "aluminio",
        "produtos_base": [
            ("Carcaça da bomba d'água", 78.00, 145.00),
            ("Flange do turbo", 95.00, 176.00),
            ("Suporte do alternador", 42.00, 78.00),
            ("Carcaça do diferencial", 180.00, 334.00),
            ("Tampa do cabeçote", 65.00, 121.00),
            ("Coletor de admissão", 110.00, 204.00),
            ("Suporte da caixa de câmbio", 88.00, 163.00),
            ("Flange de escape", 35.00, 65.00),
            ("Corpo de válvula", 125.00, 232.00),
            ("Suporte do compressor AC", 52.00, 97.00),
            ("Tampa do cárter", 70.00, 130.00),
            ("Carcaça do servo freio", 92.00, 171.00),
        ],
    },
    "LP03": {
        "nome": "Injeção de Polímeros",
        "descricao": "Peças plásticas injetadas para acabamento e funcional",
        "commodity_principal": "polimero",
        "produtos_base": [
            ("Painel de instrumentos", 55.00, 110.00),
            ("Console central", 38.00, 76.00),
            ("Moldura do farol", 18.00, 36.00),
            ("Capa do retrovisor", 12.00, 24.00),
            ("Grade dianteira", 28.00, 56.00),
            ("Defletor do radiador", 15.00, 30.00),
            ("Caixa do filtro de ar", 22.00, 44.00),
            ("Reservatório de expansão", 8.00, 16.00),
            ("Capa da coluna de direção", 10.00, 20.00),
            ("Moldura da lanterna", 14.00, 28.00),
            ("Duto de ventilação", 9.00, 18.00),
            ("Capa do parachoque traseiro", 32.00, 64.00),
            ("Guia do para-brisa", 7.00, 14.00),
        ],
    },
    "LP04": {
        "nome": "Montagem e Subconjuntos",
        "descricao": "Subconjuntos montados com componentes internos e de terceiros",
        "commodity_principal": "misto",
        "produtos_base": [
            ("Módulo do pedal de freio", 65.00, 125.00),
            ("Chicote do motor", 48.00, 92.00),
            ("Conjunto do limpador", 35.00, 67.00),
            ("Módulo da coluna de direção", 88.00, 169.00),
            ("Conjunto do fechadura", 28.00, 54.00),
            ("Suporte com abraçadeiras", 15.00, 29.00),
            ("Módulo do airbag (carcaça)", 72.00, 138.00),
            ("Conjunto do freio de mão", 42.00, 81.00),
            ("Suporte do banco (trilho)", 55.00, 106.00),
            ("Módulo do retrovisor elétrico", 60.00, 115.00),
            ("Conjunto do maçaneta", 22.00, 42.00),
            ("Kit fixação do motor", 38.00, 73.00),
        ],
    },
}


def gerar_cadastro_produtos():
    """Gera cadastro de 50 produtos distribuídos nas 4 linhas."""
    produtos = []
    prod_id = 1

    for linha_cod, linha_info in LINHAS_PRODUCAO.items():
        for nome, custo_base, preco_base in linha_info["produtos_base"]:
            prod_code = f"PROD-{prod_id:04d}"
            produtos.append(
                {
                    "produto_id": prod_code,
                    "produto_nome": nome,
                    "linha_producao_id": linha_cod,
                    "linha_producao_nome": linha_info["nome"],
                    "unidade_medida": "PÇ",
                    "custo_padrao_unitario": round(custo_base, 2),
                    "preco_venda_unitario": round(preco_base, 2),
                    "peso_kg": round(np.random.uniform(0.3, 15.0), 2),
                    "status": "ATIVO" if random.random() > 0.06 else "INATIVO",
                    "data_cadastro": fake.date_between(
                        start_date="-5y", end_date="-3y"
                    ),
                    "ncm": f"{random.randint(7000,8700)}.{random.randint(10,99)}.{random.randint(10,99)}",
                }
            )
            prod_id += 1

    df = pd.DataFrame(produtos)
    return df


# ============================================================================
# 2. CENTROS DE CUSTO
# ============================================================================

CENTROS_CUSTO = [
    ("CC001", "Produção - Estamparia", "PRODUÇÃO", "LP01"),
    ("CC002", "Produção - Usinagem", "PRODUÇÃO", "LP02"),
    ("CC003", "Produção - Injeção", "PRODUÇÃO", "LP03"),
    ("CC004", "Produção - Montagem", "PRODUÇÃO", "LP04"),
    ("CC005", "Manutenção Industrial", "PRODUÇÃO", None),
    ("CC006", "Qualidade", "PRODUÇÃO", None),
    ("CC007", "PCP / Logística Interna", "PRODUÇÃO", None),
    ("CC010", "Comercial / Vendas", "COMERCIAL", None),
    ("CC011", "Marketing", "COMERCIAL", None),
    ("CC020", "Administrativo / Financeiro", "ADMINISTRATIVO", None),
    ("CC021", "RH / Departamento Pessoal", "ADMINISTRATIVO", None),
    ("CC022", "TI", "ADMINISTRATIVO", None),
    ("CC030", "Diretoria", "ADMINISTRATIVO", None),
    ("CC040", "Logística / Expedição", "LOGÍSTICA", None),
]


def gerar_cadastro_centros_custo():
    rows = []
    for cc_id, nome, tipo, linha in CENTROS_CUSTO:
        rows.append(
            {
                "centro_custo_id": cc_id,
                "centro_custo_nome": nome,
                "tipo": tipo,
                "linha_producao_vinculada": linha,
                "responsavel": fake.name(),
                "ativo": True,
            }
        )
    return pd.DataFrame(rows)


# ============================================================================
# 3. SAZONALIDADE E COMMODITIES
# ============================================================================

# Sazonalidade mensal automotiva brasileira (indice multiplicador)
# Jan: parada coletiva, Fev: retomada lenta, Mar-Jun: produção normal,
# Jul: férias coletivas parciais, Ago-Nov: pico (lançamentos), Dez: parada
SAZONALIDADE_MENSAL = {
    1: 0.55,   # Janeiro — parada coletiva
    2: 0.72,   # Fevereiro — retomada
    3: 0.90,   # Março
    4: 0.95,   # Abril
    5: 1.00,   # Maio — produção plena
    6: 1.00,   # Junho
    7: 0.80,   # Julho — férias coletivas parciais
    8: 1.10,   # Agosto — ramp-up lançamentos
    9: 1.15,   # Setembro — pico
    10: 1.18,  # Outubro — pico máximo
    11: 1.08,  # Novembro — desaceleração leve
    12: 0.50,  # Dezembro — parada coletiva
}

# Índice de commodities — simula choque real
# Base 1.0 em Jan/2022, com variações acumuladas
def gerar_indice_commodities():
    """
    Simula variação de preço de commodities ao longo de 36 meses.
    Inclui choque de aço em 2022-Q2 (guerra Ucrânia) e normalização gradual.
    """
    meses = pd.date_range(PERIODO_INICIO, PERIODO_FIM, freq="MS")
    indices = {}

    for commodity in ["aco", "aluminio", "polimero", "misto"]:
        base = 1.0
        serie = []
        for i, mes in enumerate(meses):
            # Tendência base: inflação industrial ~0.3% ao mês
            tendencia = 1.003 ** i

            # Choque específico por commodity
            choque = 1.0
            if commodity == "aco":
                # Choque Q2/2022 (guerra) — pico de +35%, normaliza em 12 meses
                if 3 <= i <= 8:  # Abr-Set 2022
                    choque = 1.0 + 0.35 * np.sin(np.pi * (i - 3) / 5)
                elif 9 <= i <= 18:
                    choque = 1.0 + 0.15 * max(0, 1 - (i - 9) / 9)
            elif commodity == "aluminio":
                # Choque menor, defasado 1 mês
                if 4 <= i <= 10:
                    choque = 1.0 + 0.22 * np.sin(np.pi * (i - 4) / 6)
                elif 11 <= i <= 18:
                    choque = 1.0 + 0.10 * max(0, 1 - (i - 11) / 7)
            elif commodity == "polimero":
                # Polímeros: choque em 2023 por petróleo
                if 14 <= i <= 20:  # Mar-Set 2023
                    choque = 1.0 + 0.18 * np.sin(np.pi * (i - 14) / 6)
            elif commodity == "misto":
                # Média ponderada simplificada
                choque = 1.0 + 0.12 * np.sin(np.pi * max(0, i - 3) / 10) * (1 if i < 20 else 0.5)

            # Volatilidade aleatória mensal (±3%)
            ruido = np.random.normal(1.0, 0.015)

            indice = round(base * tendencia * choque * ruido, 4)
            serie.append({"mes": mes, "commodity": commodity, "indice": indice})

        indices[commodity] = serie

    rows = []
    for commodity, serie in indices.items():
        rows.extend(serie)

    return pd.DataFrame(rows)


# ============================================================================
# 4. VENDAS (TRANSAÇÕES DIÁRIAS)
# ============================================================================

def gerar_vendas(df_produtos, df_commodities):
    """
    Gera ~150k transações de venda ao longo de 3 anos.
    Considera sazonalidade, mix de produto e variação de preço.
    """
    produtos_ativos = df_produtos[df_produtos["status"] == "ATIVO"].copy()
    meses = pd.date_range(PERIODO_INICIO, PERIODO_FIM, freq="MS")

    # Clientes (montadoras e distribuidores)
    clientes = []
    montadoras = [
        "Stellantis Brasil", "Volkswagen do Brasil", "General Motors Brasil",
        "Toyota do Brasil", "Hyundai Motor Brasil", "Honda Automóveis",
        "Renault do Brasil", "Nissan do Brasil", "CAOA Chery",
    ]
    for i, nome in enumerate(montadoras):
        clientes.append({
            "cliente_id": f"CLI-{i+1:04d}",
            "cliente_nome": nome,
            "tipo": "MONTADORA",
            "peso_volume": random.uniform(0.08, 0.18),
        })
    # Distribuidores / reposição
    for i in range(15):
        clientes.append({
            "cliente_id": f"CLI-{len(montadoras)+i+1:04d}",
            "cliente_nome": fake.company(),
            "tipo": "DISTRIBUIDOR",
            "peso_volume": random.uniform(0.01, 0.04),
        })

    # Normalizar pesos
    total_peso = sum(c["peso_volume"] for c in clientes)
    for c in clientes:
        c["peso_volume"] /= total_peso

    vendas = []
    venda_id = 1

    # Crescimento ano a ano: 2022=1.0, 2023=1.08, 2024=1.15
    crescimento_anual = {2022: 1.00, 2023: 1.08, 2024: 1.15}

    for mes in meses:
        ano = mes.year
        mes_num = mes.month
        sazon = SAZONALIDADE_MENSAL[mes_num]
        cresc = crescimento_anual[ano]

        # Volume base mensal por produto: ~80-400 peças (depende do produto)
        for _, prod in produtos_ativos.iterrows():
            # Produtos mais baratos vendem mais volume
            vol_base = max(30, int(800 / (prod["preco_venda_unitario"] ** 0.4)))
            vol_mensal = int(vol_base * sazon * cresc * np.random.uniform(0.8, 1.2))

            if vol_mensal <= 0:
                continue

            # Distribuir vendas em ~8-18 pedidos no mês
            num_pedidos = max(1, int(vol_mensal / np.random.uniform(15, 40)))
            qtds = np.random.multinomial(
                vol_mensal, [1.0 / num_pedidos] * num_pedidos
            )

            for qtd in qtds:
                if qtd <= 0:
                    continue

                # Selecionar cliente (ponderado)
                cliente = random.choices(
                    clientes, weights=[c["peso_volume"] for c in clientes], k=1
                )[0]

                # Data aleatória dentro do mês (dias úteis simulados)
                dia = random.randint(1, 28)
                data_venda = mes.replace(day=dia)

                # Preço pode ter desconto por volume (montadoras negociam mais)
                desconto_pct = 0.0
                if cliente["tipo"] == "MONTADORA":
                    if qtd > 100:
                        desconto_pct = round(random.uniform(0.03, 0.08), 4)
                    else:
                        desconto_pct = round(random.uniform(0.01, 0.04), 4)
                else:
                    desconto_pct = round(random.uniform(0.00, 0.02), 4)

                preco_unit = prod["preco_venda_unitario"]

                # Reajuste de preço anual (repasse parcial de commodity)
                if ano == 2023:
                    preco_unit *= 1.06
                elif ano == 2024:
                    preco_unit *= 1.11

                preco_unit = round(preco_unit, 2)
                valor_bruto = round(preco_unit * int(qtd), 2)
                valor_desconto = round(valor_bruto * desconto_pct, 2)

                # Devolução (2.5% das vendas tem devolução parcial)
                devolucao_qtd = 0
                devolucao_valor = 0.0
                if random.random() < 0.025:
                    devolucao_qtd = max(1, int(qtd * random.uniform(0.05, 0.30)))
                    devolucao_valor = round(
                        devolucao_qtd * preco_unit * (1 - desconto_pct), 2
                    )

                nf_numero = f"NF-{venda_id:07d}"

                vendas.append(
                    {
                        "venda_id": f"VND-{venda_id:07d}",
                        "nota_fiscal": nf_numero,
                        "data_emissao": data_venda,
                        "cliente_id": cliente["cliente_id"],
                        "cliente_nome": cliente["cliente_nome"],
                        "tipo_cliente": cliente["tipo"],
                        "produto_id": prod["produto_id"],
                        "quantidade": int(qtd),
                        "preco_unitario": preco_unit,
                        "valor_bruto": valor_bruto,
                        "desconto_percentual": desconto_pct,
                        "valor_desconto": valor_desconto,
                        "devolucao_quantidade": devolucao_qtd,
                        "devolucao_valor": devolucao_valor,
                        "valor_liquido": round(
                            valor_bruto - valor_desconto - devolucao_valor, 2
                        ),
                        "linha_producao_id": prod["linha_producao_id"],
                    }
                )
                venda_id += 1

    df = pd.DataFrame(vendas)
    print(f"  → Vendas geradas: {len(df):,} transações")
    print(f"  → Receita bruta total: R$ {df['valor_bruto'].sum():,.2f}")
    return df, pd.DataFrame(clientes)


# ============================================================================
# 5. CUSTOS DE PRODUÇÃO (CMV MENSAL POR PRODUTO)
# ============================================================================

def gerar_custos_producao(df_produtos, df_commodities):
    """
    Gera CMV mensal por produto, aplicando índice de commodity sobre custo padrão.
    Componentes do CMV: matéria-prima (60%), mão de obra direta (25%), CIF (15%).
    """
    meses = pd.date_range(PERIODO_INICIO, PERIODO_FIM, freq="MS")
    produtos_ativos = df_produtos[df_produtos["status"] == "ATIVO"].copy()

    custos = []

    for _, prod in produtos_ativos.iterrows():
        linha = prod["linha_producao_id"]
        commodity = LINHAS_PRODUCAO[linha]["commodity_principal"]
        custo_padrao = prod["custo_padrao_unitario"]

        # Decomposição do custo
        mp_base = custo_padrao * 0.60    # Matéria-prima
        mod_base = custo_padrao * 0.25   # Mão de obra direta
        cif_base = custo_padrao * 0.15   # Custos indiretos de fabricação

        for mes in meses:
            # Buscar índice da commodity no mês
            idx_row = df_commodities[
                (df_commodities["commodity"] == commodity)
                & (df_commodities["mes"] == mes)
            ]
            indice_commodity = idx_row["indice"].values[0] if len(idx_row) > 0 else 1.0

            # MOD: reajuste salarial anual + dissídio
            fator_mod = 1.0
            if mes.year == 2023:
                fator_mod = 1.07  # dissídio 7%
            elif mes.year == 2024:
                fator_mod = 1.12  # acumulado

            # CIF: reajuste pela inflação geral (~5% ao ano)
            meses_desde_inicio = (mes.year - 2022) * 12 + mes.month - 1
            fator_cif = 1.004 ** meses_desde_inicio

            # Custo real do mês
            mp_real = round(mp_base * indice_commodity, 2)
            mod_real = round(mod_base * fator_mod, 2)
            cif_real = round(cif_base * fator_cif, 2)
            custo_total = round(mp_real + mod_real + cif_real, 2)

            custos.append(
                {
                    "produto_id": prod["produto_id"],
                    "mes_referencia": mes,
                    "custo_materia_prima": mp_real,
                    "custo_mao_obra_direta": mod_real,
                    "custo_indireto_fabricacao": cif_real,
                    "custo_total_unitario": custo_total,
                    "custo_padrao_unitario": custo_padrao,
                    "variacao_custo_pct": round(
                        (custo_total - custo_padrao) / custo_padrao, 4
                    ),
                    "indice_commodity": indice_commodity,
                    "linha_producao_id": prod["linha_producao_id"],
                }
            )

    df = pd.DataFrame(custos)
    print(f"  → Custos gerados: {len(df):,} registros (produto x mês)")
    return df


# ============================================================================
# 6. DESPESAS OPERACIONAIS (MENSAIS POR CENTRO DE CUSTO)
# ============================================================================

NATUREZAS_DESPESA = {
    "PRODUÇÃO": [
        ("Salários e Encargos - Produção", 280000, 350000, "DESPESA_FABRICA"),
        ("Energia Elétrica Industrial", 85000, 120000, "DESPESA_FABRICA"),
        ("Manutenção de Máquinas", 35000, 65000, "DESPESA_FABRICA"),
        ("Materiais Auxiliares", 15000, 30000, "DESPESA_FABRICA"),
        ("Depreciação - Máquinas", 45000, 45000, "DEPRECIACAO"),
        ("Seguro Industrial", 12000, 12000, "DESPESA_FABRICA"),
    ],
    "COMERCIAL": [
        ("Salários e Encargos - Comercial", 95000, 130000, "DESPESA_VENDAS"),
        ("Comissões sobre Vendas", 0, 0, "DESPESA_VENDAS"),  # Calculado % receita
        ("Fretes sobre Vendas", 0, 0, "DESPESA_VENDAS"),  # Calculado % receita
        ("Marketing e Feiras", 15000, 40000, "DESPESA_VENDAS"),
        ("Viagens Comerciais", 8000, 20000, "DESPESA_VENDAS"),
    ],
    "ADMINISTRATIVO": [
        ("Salários e Encargos - Admin", 180000, 220000, "DESPESA_ADMIN"),
        ("Aluguel e Condomínio", 35000, 35000, "DESPESA_ADMIN"),
        ("Serviços de Contabilidade", 12000, 12000, "DESPESA_ADMIN"),
        ("TI e Sistemas", 18000, 28000, "DESPESA_ADMIN"),
        ("Depreciação - Administrativo", 8000, 8000, "DEPRECIACAO"),
        ("Honorários da Diretoria", 65000, 65000, "DESPESA_ADMIN"),
        ("Despesas Diversas", 5000, 12000, "DESPESA_ADMIN"),
    ],
    "LOGÍSTICA": [
        ("Salários e Encargos - Logística", 55000, 75000, "DESPESA_ADMIN"),
        ("Combustível e Frota", 12000, 22000, "DESPESA_ADMIN"),
        ("Armazenagem", 8000, 15000, "DESPESA_ADMIN"),
    ],
}

# Despesas financeiras (separadas)
DESPESAS_FINANCEIRAS = [
    ("Juros sobre Empréstimos", 25000, 45000),
    ("Tarifas Bancárias", 3000, 5000),
    ("Variação Cambial", -15000, 20000),  # Pode ser positiva ou negativa
]


def gerar_despesas_operacionais(df_vendas):
    """Gera despesas mensais por centro de custo e natureza."""
    meses = pd.date_range(PERIODO_INICIO, PERIODO_FIM, freq="MS")
    despesas = []
    desp_id = 1

    # Receita mensal para calcular comissões e fretes
    receita_mensal = (
        df_vendas.groupby(pd.Grouper(key="data_emissao", freq="MS"))["valor_bruto"]
        .sum()
        .to_dict()
    )

    crescimento_despesa = {2022: 1.00, 2023: 1.06, 2024: 1.11}

    for mes in meses:
        ano = mes.year
        mes_num = mes.month
        cresc = crescimento_despesa[ano]
        sazon_desp = 0.85 + 0.15 * SAZONALIDADE_MENSAL[mes_num]  # Despesa é menos sazonal

        receita_mes = receita_mensal.get(mes, 0)

        for tipo_cc, naturezas in NATUREZAS_DESPESA.items():
            for nome_desp, val_min, val_max, classificacao in naturezas:
                # Comissões e fretes: % da receita
                if "Comissões" in nome_desp:
                    valor = round(receita_mes * random.uniform(0.018, 0.025), 2)
                elif "Fretes sobre Vendas" in nome_desp:
                    valor = round(receita_mes * random.uniform(0.012, 0.020), 2)
                elif val_min == val_max:
                    # Custo fixo (depreciação, aluguel, etc.)
                    valor = round(val_min * cresc, 2)
                else:
                    valor = round(
                        random.uniform(val_min, val_max) * cresc * sazon_desp, 2
                    )

                # 13º e férias em novembro/dezembro
                if mes_num in [11, 12] and "Salários" in nome_desp:
                    valor *= 1.35

                despesas.append(
                    {
                        "despesa_id": f"DESP-{desp_id:07d}",
                        "mes_referencia": mes,
                        "centro_custo_tipo": tipo_cc,
                        "natureza": nome_desp,
                        "classificacao_dre": classificacao,
                        "valor": valor,
                    }
                )
                desp_id += 1

        # Despesas financeiras (sem centro de custo específico)
        for nome_fin, val_min, val_max in DESPESAS_FINANCEIRAS:
            valor = round(random.uniform(val_min, val_max) * cresc, 2)
            despesas.append(
                {
                    "despesa_id": f"DESP-{desp_id:07d}",
                    "mes_referencia": mes,
                    "centro_custo_tipo": "FINANCEIRO",
                    "natureza": nome_fin,
                    "classificacao_dre": "RESULTADO_FINANCEIRO",
                    "valor": valor,
                }
            )
            desp_id += 1

    df = pd.DataFrame(despesas)
    print(f"  → Despesas geradas: {len(df):,} registros")
    print(f"  → Total despesas: R$ {df['valor'].sum():,.2f}")
    return df


# ============================================================================
# 7. ORÇAMENTO ANUAL (ORÇADO VS REALIZADO)
# ============================================================================

def gerar_orcamento(df_despesas, df_vendas):
    """
    Gera orçamento anual por natureza de despesa e receita.
    O orçamento é feito no início do ano com premissas.
    """
    orcamentos = []
    orc_id = 1

    for ano in [2022, 2023, 2024]:
        # RECEITA ORÇADA
        # Premissa: crescimento de 10% ao ano sobre o realizado anterior
        vendas_ano = df_vendas[df_vendas["data_emissao"].dt.year == ano]
        receita_real = vendas_ano["valor_bruto"].sum()

        # Orçamento tem desvio de ±5-15% do real (simulando erro de previsão)
        fator_erro_receita = random.uniform(0.92, 1.08)
        receita_orcada_anual = round(receita_real * fator_erro_receita, 2)

        for mes_num in range(1, 13):
            # Distribuição do orçamento segue sazonalidade esperada
            peso_mes = SAZONALIDADE_MENSAL[mes_num] / sum(SAZONALIDADE_MENSAL.values())
            receita_orcada_mes = round(receita_orcada_anual * peso_mes, 2)

            orcamentos.append(
                {
                    "orcamento_id": f"ORC-{orc_id:07d}",
                    "ano": ano,
                    "mes": mes_num,
                    "tipo": "RECEITA",
                    "natureza": "Receita Bruta de Vendas",
                    "centro_custo_tipo": "COMERCIAL",
                    "valor_orcado": receita_orcada_mes,
                }
            )
            orc_id += 1

        # DESPESAS ORÇADAS
        despesas_ano = df_despesas[df_despesas["mes_referencia"].dt.year == ano]
        despesas_por_natureza = (
            despesas_ano.groupby("natureza")["valor"].sum().to_dict()
        )

        for natureza, total_real in despesas_por_natureza.items():
            fator_erro = random.uniform(0.88, 1.05)
            total_orcado = round(total_real * fator_erro, 2)

            for mes_num in range(1, 13):
                peso_mes = 1.0 / 12  # Despesas orçadas linear (simplificação realista)
                # Exceto salários que tem 13º
                if "Salários" in natureza and mes_num in [11, 12]:
                    peso_mes = 1.35 / 12

                valor_orcado_mes = round(total_orcado * peso_mes, 2)

                cc_tipo = despesas_ano[despesas_ano["natureza"] == natureza][
                    "centro_custo_tipo"
                ].iloc[0]

                orcamentos.append(
                    {
                        "orcamento_id": f"ORC-{orc_id:07d}",
                        "ano": ano,
                        "mes": mes_num,
                        "tipo": "DESPESA",
                        "natureza": natureza,
                        "centro_custo_tipo": cc_tipo,
                        "valor_orcado": valor_orcado_mes,
                    }
                )
                orc_id += 1

    df = pd.DataFrame(orcamentos)
    print(f"  → Orçamento gerado: {len(df):,} linhas")
    return df


# ============================================================================
# 8. HISTÓRICO DE CUSTOS (BASE PARA SCD TYPE 2)
# ============================================================================

def gerar_historico_custos(df_produtos, df_custos):
    """
    Gera tabela de mudanças de custo padrão ao longo do tempo.
    Simula revisões trimestrais de custo padrão (prática real em indústrias).
    """
    historico = []
    hist_id = 1

    for _, prod in df_produtos[df_produtos["status"] == "ATIVO"].iterrows():
        custo_atual = prod["custo_padrao_unitario"]
        data_vigencia = pd.Timestamp("2022-01-01")

        # Revisões em Abr, Jul, Out de cada ano (trimestrais)
        datas_revisao = [
            pd.Timestamp("2022-04-01"),
            pd.Timestamp("2022-07-01"),
            pd.Timestamp("2022-10-01"),
            pd.Timestamp("2023-01-01"),
            pd.Timestamp("2023-04-01"),
            pd.Timestamp("2023-07-01"),
            pd.Timestamp("2023-10-01"),
            pd.Timestamp("2024-01-01"),
            pd.Timestamp("2024-04-01"),
            pd.Timestamp("2024-07-01"),
            pd.Timestamp("2024-10-01"),
        ]

        # Registro inicial
        historico.append(
            {
                "historico_id": f"HIST-{hist_id:07d}",
                "produto_id": prod["produto_id"],
                "custo_padrao_anterior": None,
                "custo_padrao_novo": custo_atual,
                "data_vigencia_inicio": pd.Timestamp("2022-01-01"),
                "data_vigencia_fim": datas_revisao[0] - timedelta(days=1),
                "motivo": "CUSTO_INICIAL",
                "is_current": False,
            }
        )
        hist_id += 1

        for i, data_rev in enumerate(datas_revisao):
            # Nem toda revisão muda o custo (60% de chance de mudança)
            if random.random() < 0.60:
                variacao = random.uniform(-0.05, 0.12)  # -5% a +12%
                custo_anterior = custo_atual
                custo_atual = round(custo_atual * (1 + variacao), 2)

                fim = (
                    datas_revisao[i + 1] - timedelta(days=1)
                    if i + 1 < len(datas_revisao)
                    else pd.Timestamp("9999-12-31")
                )
                is_current = i == len(datas_revisao) - 1 or (
                    i + 1 < len(datas_revisao)
                    and all(random.random() >= 0.60 for _ in range(len(datas_revisao) - i - 1))
                )

                historico.append(
                    {
                        "historico_id": f"HIST-{hist_id:07d}",
                        "produto_id": prod["produto_id"],
                        "custo_padrao_anterior": custo_anterior,
                        "custo_padrao_novo": custo_atual,
                        "data_vigencia_inicio": data_rev,
                        "data_vigencia_fim": fim,
                        "motivo": random.choice([
                            "REAJUSTE_COMMODITY",
                            "REVISAO_TRIMESTRAL",
                            "RENEGOCIACAO_FORNECEDOR",
                            "ALTERACAO_PROCESSO",
                        ]),
                        "is_current": fim == pd.Timestamp("9999-12-31"),
                    }
                )
                hist_id += 1

    df = pd.DataFrame(historico)

    # Corrigir is_current: apenas o último registro de cada produto
    for prod_id in df["produto_id"].unique():
        mask = df["produto_id"] == prod_id
        idx_last = df[mask].index[-1]
        df.loc[mask, "is_current"] = False
        df.loc[idx_last, "is_current"] = True
        df.loc[idx_last, "data_vigencia_fim"] = pd.Timestamp("9999-12-31")

    print(f"  → Histórico de custos: {len(df):,} registros (SCD Type 2)")
    return df


# ============================================================================
# EXECUÇÃO PRINCIPAL
# ============================================================================

def salvar(df, nome):
    """Salva em CSV e Parquet."""
    csv_path = f"{OUTPUT_DIR}/csv/{nome}.csv"
    parquet_path = f"{OUTPUT_DIR}/parquet/{nome}.parquet"

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_parquet(parquet_path, index=False, engine="pyarrow")

    mb_csv = os.path.getsize(csv_path) / (1024 * 1024)
    mb_parquet = os.path.getsize(parquet_path) / (1024 * 1024)

    print(f"  💾 {nome}: CSV={mb_csv:.2f}MB | Parquet={mb_parquet:.2f}MB")


def main():
    print("=" * 70)
    print("🏭 FinOps Manufacturing Intelligence — Geração de Dados")
    print("   Segmento: Autopeças Tier 2 | Período: 2022-2024")
    print("=" * 70)

    print("\n📦 [1/8] Gerando cadastro de produtos...")
    df_produtos = gerar_cadastro_produtos()
    salvar(df_produtos, "cadastro_produtos")
    print(f"  → {len(df_produtos)} produtos | {df_produtos['status'].value_counts().to_dict()}")

    print("\n🏢 [2/8] Gerando centros de custo...")
    df_centros = gerar_cadastro_centros_custo()
    salvar(df_centros, "cadastro_centros_custo")
    print(f"  → {len(df_centros)} centros de custo")

    print("\n📈 [3/8] Gerando índices de commodities...")
    df_commodities = gerar_indice_commodities()
    salvar(df_commodities, "indices_commodities")
    print(f"  → {len(df_commodities)} registros (4 commodities x 36 meses)")

    print("\n🛒 [4/8] Gerando transações de vendas...")
    df_vendas, df_clientes = gerar_vendas(df_produtos, df_commodities)
    salvar(df_vendas, "vendas")
    salvar(df_clientes, "cadastro_clientes")

    print("\n💰 [5/8] Gerando custos de produção (CMV)...")
    df_custos = gerar_custos_producao(df_produtos, df_commodities)
    salvar(df_custos, "custos_producao")

    print("\n📋 [6/8] Gerando despesas operacionais...")
    df_despesas = gerar_despesas_operacionais(df_vendas)
    salvar(df_despesas, "despesas_operacionais")

    print("\n📊 [7/8] Gerando orçamento anual...")
    df_orcamento = gerar_orcamento(df_despesas, df_vendas)
    salvar(df_orcamento, "orcamento_anual")

    print("\n🔄 [8/8] Gerando histórico de custos (SCD Type 2)...")
    df_historico = gerar_historico_custos(df_produtos, df_custos)
    salvar(df_historico, "historico_custos_produtos")

    # ========== RESUMO FINAL ==========
    print("\n" + "=" * 70)
    print("✅ GERAÇÃO CONCLUÍDA — RESUMO")
    print("=" * 70)

    receita_total = df_vendas["valor_bruto"].sum()
    receita_liquida = df_vendas["valor_liquido"].sum()
    descontos = df_vendas["valor_desconto"].sum()
    devolucoes = df_vendas["devolucao_valor"].sum()

    print(f"\n  📊 Receita Bruta Total:    R$ {receita_total:>15,.2f}")
    print(f"  📊 Descontos:             -R$ {descontos:>15,.2f}")
    print(f"  📊 Devoluções:            -R$ {devolucoes:>15,.2f}")
    print(f"  📊 Receita Líquida:        R$ {receita_liquida:>15,.2f}")
    print(f"\n  📦 Tabelas geradas: 8")
    print(f"  📂 Local CSV:     {OUTPUT_DIR}/csv/")
    print(f"  📂 Local Parquet: {OUTPUT_DIR}/parquet/")

    # Listar arquivos gerados
    print("\n  📁 Arquivos:")
    for fmt in ["csv", "parquet"]:
        for f in sorted(os.listdir(f"{OUTPUT_DIR}/{fmt}")):
            size = os.path.getsize(f"{OUTPUT_DIR}/{fmt}/{f}") / (1024 * 1024)
            print(f"     {fmt}/{f} ({size:.2f} MB)")


if __name__ == "__main__":
    main()
