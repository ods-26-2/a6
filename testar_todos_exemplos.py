"""
testar_todos_exemplos.py
=========================
Processa todos os arquivos de exemplo do I4 através do Módulo 5 (LibrasCorrectionPipeline)
e salva relatórios detalhados com as sugestões de correções e os JSONs prontos para o avatar.

Saídas salvas em: a6/saida_correcoes/
- relatorio_correcoes.md (Resumo completo para humanos)
- relatorio_correcoes.json (Resumo estruturado)
- [nome_exemplo]_correcao.json (Resultado detalhado com keypoints suavizados e corrigidos)
- [nome_exemplo]_avatar.json (Estrutura pronta para renderização no rig Mixamo/Blender)
"""

import json
import os
import sys
import time
from pathlib import Path

# Garante importação do pipeline
sys.path.insert(0, str(Path(__file__).parent))
from sugestao_correcao_pipeline import LibrasCorrectionPipeline

DIRETORIO_EXEMPLOS = Path("../i4/examples")
if not DIRETORIO_EXEMPLOS.exists():
    DIRETORIO_EXEMPLOS = Path("i4/examples")

DIRETORIO_SAIDA = Path("saida_correcoes")
DIRETORIO_SAIDA.mkdir(parents=True, exist_ok=True)

pipeline = LibrasCorrectionPipeline()


def processar_arquivo(caminho_arquivo: Path):
    print(f"\n==================================================")
    print(f"Processando: {caminho_arquivo.name}...")
    
    # Carrega com suporte a UTF-8 BOM
    conteudo = json.loads(caminho_arquivo.read_text(encoding="utf-8-sig"))
    stem = caminho_arquivo.stem

    resultados = []

    # Caso 1: Arquivo composto com múltiplos segmentos de sinais (ex: exemplo_s6_aluno_banco_cinco.json)
    if "segments" in conteudo:
        keypoints_totais = conteudo.get("keypoints", [])
        segmentos_sinais = [s for s in conteudo.get("segments", []) if s.get("tipo") == "sinal"]
        contexto_acumulado = []

        for seg in segmentos_sinais:
            sinal = seg.get("sinal", "SINAL")
            start = seg.get("start_frame", 0)
            end = seg.get("end_frame", len(keypoints_totais))
            kp_fatia = keypoints_totais[start:end]

            resultado = pipeline.process_sign_window(
                sign_label=sinal,
                confidence=0.92,
                keypoints=kp_fatia,
                context_glosses=list(contexto_acumulado),
            )
            contexto_acumulado.append(sinal)
            resultados.append(resultado)

    # Caso 2: Arquivo com sinal único (ex: exemplo_aluno.json, exemplo_ruim.json, exemplo_teste_mp4.json)
    else:
        sign_info = conteudo.get("sign", {})
        sinal = sign_info.get("sinal", "DESCONHECIDO")
        confianca = float(sign_info.get("confianca", 0.90))
        keypoints = conteudo.get("keypoints", [])

        # Para testar a sintaxe contextual de LIBRAS [T+S+O+V], injetamos glosas de contexto
        # Ex: Se o sinal for ALUNO ou RUIM, simulamos uma oração como ["ONTEM", "ALUNO", "LIVRO", "ESTUDAR"]
        contexto_teste = ["ONTEM"] if sinal.upper() == "ALUNO" else ["HOJE"]

        resultado = pipeline.process_sign_window(
            sign_label=sinal,
            confidence=confianca,
            keypoints=keypoints,
            context_glosses=contexto_teste,
        )
        resultados.append(resultado)

    # 1. Salva arquivo de resultado da correção
    caminho_res_json = DIRETORIO_SAIDA / f"{stem}_correcao.json"
    caminho_avatar_json = DIRETORIO_SAIDA / f"{stem}_avatar.json"

    # Salva o arquivo de correção detalhada
    caminho_res_json.write_text(
        json.dumps(resultados, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    # Salva os frames prontos para o avatar (concatena se forem múltiplos sinais)
    avatar_frames_todos = []
    for r in resultados:
        avatar_frames_todos.extend(r.get("avatar_frames", []))

    caminho_avatar_json.write_text(
        json.dumps(avatar_frames_todos, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    print(f"[OK] Salvo correção detalhada: {caminho_res_json}")
    print(f"[OK] Salvo JSON do Avatar 3D ({len(avatar_frames_todos)} frames): {caminho_avatar_json}")

    return {
        "arquivo": caminho_arquivo.name,
        "sinais": [r["sign"] for r in resultados],
        "detalhes": [
            {
                "sinal": r["sign"],
                "confianca": r["confidence"],
                "fonologia": r["phonology"],
                "correcoes_biomecanicas": r["biomechanical_corrections"],
                "sintaxe_original": r["syntactic_correction"]["original"],
                "sintaxe_corrigida": r["syntactic_correction"]["reordered"],
                "houve_reordenacao_sintatica": r["syntactic_correction"]["was_reordered"],
                "total_frames_avatar": len(r["avatar_frames"]),
            }
            for r in resultados
        ],
    }


def gerar_relatorio_markdown(resumo_geral: list):
    caminho_md = DIRETORIO_SAIDA / "relatorio_correcoes.md"
    
    linhas = [
        "# Relatório de Validação e Sugestão de Correção (Módulo 5)",
        f"*Data de Execução:* {time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"*Diretório de Saída:* `{DIRETORIO_SAIDA.resolve()}`",
        "",
        "---",
        "## Resumo dos Arquivos Processados",
        "",
        "| Arquivo de Exemplo | Sinais Detectados | CM (Mão) | Articulação (PA) | Correção Biomecânica | Reordenação Sintática |",
        "|---|---|---|---|---|---|",
    ]

    for item in resumo_geral:
        arq = item["arquivo"]
        for det in item["detalhes"]:
            sinal = det["sinal"]
            cm = det["fonologia"]["configuracao_mao"]
            pa = det["fonologia"]["ponto_articulacao"]
            bio = f"Sim ({len(det['correcoes_biomecanicas'])})" if det["correcoes_biomecanicas"] else "Normal"
            sint = f"`{' '.join(det['sintaxe_corrigida'])}`" if det["houve_reordenacao_sintatica"] else "Conforme"
            linhas.append(f"| `{arq}` | **{sinal}** | {cm} | {pa} | {bio} | {sint} |")

    linhas.extend([
        "",
        "---",
        "## Detalhamento das Correções por Exemplo",
        ""
    ])

    for item in resumo_geral:
        linhas.append(f"### Exemplo: `{item['arquivo']}`")
        for det in item["detalhes"]:
            linhas.append(f"#### Sinal: **{det['sinal']}** (Confiança: {det['confianca']:.2f})")
            linhas.append(f"- **Parâmetros Fonológicos:**")
            linhas.append(f"  - Configuração de Mão: `{det['fonologia']['configuracao_mao']}`")
            linhas.append(f"  - Ponto de Articulação: `{det['fonologia']['ponto_articulacao']}`")
            linhas.append(f"  - Orientação da Palma: `{det['fonologia']['orientacao_palma']}`")
            linhas.append(f"  - Dinâmica de Movimento: `{det['fonologia']['movimento']}`")
            
            linhas.append(f"- **Validação Biomecânica:**")
            if det["correcoes_biomecanicas"]:
                for bio in det["correcoes_biomecanicas"]:
                    linhas.append(f"  - ⚠️ {bio}")
            else:
                linhas.append("  - ✅ Nenhuma distorção articular ou rotação anômala detectada.")

            linhas.append(f"- **Organização Gramatical (Sintaxe LIBRAS):**")
            linhas.append(f"  - Sequência Recebida: `{det['sintaxe_original']}`")
            linhas.append(f"  - Sequência Sugerida: `{det['sintaxe_corrigida']}`")
            linhas.append(f"  - Houve Correção na Ordem: {'Sim' if det['houve_reordenacao_sintatica'] else 'Não'}")
            linhas.append(f"- **Avatar 3D:** {det['total_frames_avatar']} quadros convertidos para o schema `libras_avatar.py`.")
            linhas.append("")

    caminho_md.write_text("\n".join(linhas), encoding="utf-8")
    print(f"\n[SUCESSO] Relatório Markdown completo salvo em: {caminho_md.resolve()}")


def main():
    exemplos = sorted(DIRETORIO_EXEMPLOS.glob("*.json"))
    if not exemplos:
        print(f"Nenhum exemplo encontrado em {DIRETORIO_EXEMPLOS}")
        return

    print(f"Encontrados {len(exemplos)} arquivos de exemplo em {DIRETORIO_EXEMPLOS}.")
    
    resumo_todos = []
    for f in exemplos:
        resumo = processar_arquivo(f)
        resumo_todos.append(resumo)

    # Salva resumo JSON
    caminho_resumo_json = DIRETORIO_SAIDA / "relatorio_correcoes.json"
    caminho_resumo_json.write_text(
        json.dumps(resumo_todos, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    # Gera relatório legível em Markdown
    gerar_relatorio_markdown(resumo_todos)

    print("\n==================================================")
    print(" TODOS OS EXEMPLOS FORAM PROCESSADOS COM SUCESSO!")
    print(f"Verifique a pasta: {DIRETORIO_SAIDA.resolve()}")


if __name__ == "__main__":
    main()
