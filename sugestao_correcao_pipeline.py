"""

sugestao_correcao_pipeline.py

==============================

Módulo de Sugestão de Correção e Gramática de LIBRAS (Squad A6 · Camada 4 · Arquitetura 6c).



Opera EXCLUSIVAMENTE sobre palavras/glosas e metadados gramaticais (sem manipular keypoints 3D):

1. Artefato LEXICON compartilhado com S6 e I4 (100 glosas oficiais + conjunto fechado de marcadores faciais).

2. Sentido Libras -> Português (Entrada I4: `ods.i4.sign`):

   - Avalia `confidence`. Quando baixa ou fora do léxico, oferece as alternativas de `alternatives`

     para manter a conversa fluindo sem falhar em silêncio.

3. Sentido Libras -> Português (I4): envia o histórico de glosas para a API Qwen3 4B,
   que gera uma frase em português brasileiro e conjuga os verbos pelo contexto.

4. Sentido Português -> Libras (Saída p/ S6: `ods.s6.synthesis.command` e `ods.s6.synthesis.reply`):

   - Remove as stopwords do Português ANTES de iniciar o processo de validação e correção.

   - Faz a PRÉ-VALIDAÇÃO local ANTES de enviar para o S6: compara cada palavra contra o LEXICON de 100 palavras,

     sugerindo e aplicando substituições por sinônimos ou proximidade ortográfica (Levenshtein/SequenceMatcher).

   - Aplica a gramática própria da LIBRAS: ordem canônica [TEMPO] + [SUJEITO] + [OBJETO] + [VERBO] e

     extração do conjunto fechado de marcadores faciais (`INTERROGATIVA_QU`, `INTERROGATIVA_SN`, `NEGACAO`, etc.).

   - Mantém a ordenação determinística das glosas para a gramática de LIBRAS.
   - Também trata `unknown_signs` vindo na reply de S6 como fallback de segurança.

Configuração da API Qwen3:
   - QWEN3_API_URL: URL do serviço Cloud Run (com ou sem o sufixo /gerar).
   - QWEN3_API_KEY: chave enviada no cabeçalho X-API-Key.
   - QWEN3_TIMEOUT: timeout HTTP em segundos (padrão: 270).

"""



from __future__ import annotations



import difflib
import json
import os
import threading
import time
import unicodedata
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple





# Configuração padrão do serviço Qwen3 4B hospedado no Google Cloud Run
DEFAULT_QWEN3_API_URL = "https://qwen3-frases-460006729047.southamerica-east1.run.app"
DEFAULT_QWEN3_API_KEY = "W3Vw6CaDIt4_zUUquL1KzKPH3onbvo1oHf2bjnIn2mWwCjO-W3pU8g"


def normalize_gloss(text: str) -> str:

    """Remove acentos, pontuação e converte para maiúsculas: 'Amanhã?' -> 'AMANHA'."""

    decomposed = unicodedata.normalize("NFD", str(text))

    stripped = "".join(c for c in decomposed if unicodedata.category(c) != "Mn")

    cleaned = "".join(c for c in stripped if c.isalnum() or c in ("-", "_"))

    return cleaned.upper().strip()





class LibrasCorrectionPipeline:

    """

    Corretor Léxico, Sintático e de Sugestão de Alternativas em LIBRAS (Camada 4 - A6).

    """



# Conjunto fechado oficial de marcadores gramaticais (definido uma única vez em A6)

    CLOSED_FACIAL_MARKERS = (

        "INTERROGATIVA_QU",

        "INTERROGATIVA_SN",

        "NEGACAO",

        "AFIRMACAO",

        "INTENSIDADE",

        "NEUTRO",

    )



# Dicionário de sinônimos / palavras próximas em Português -> Glosa oficial do LEXICON de 100 palavras

    SYNONYM_MAP: Dict[str, str] = {

# Pessoas / Sujeitos

        "ESTUDANTE": "ALUNO",

        "DISCENTE": "ALUNO",

        "UNIVERSITARIO": "ALUNO",

        "COLEGIAL": "ALUNO",

        "DOCENTE": "PROFESSOR",

        "MESTRE": "PROFESSOR",

        "EDUCADOR": "PROFESSOR",

        "INSTRUTOR": "PROFESSOR",

        "DOUTOR": "MEDICO",

        "ENFERMEIRO": "MEDICO",

        "GAROTO": "CRIANCA",

        "GAROTA": "CRIANCA",

        "MENINO": "CRIANCA",

        "MENINA": "CRIANCA",

        "BEBE": "CRIANCA",

        "JOVEM": "CRIANCA",

        "FILHA": "FILHO",

        "IRMA": "IRMAO",

        "COISA": "PROBLEMA",

        "GENTE": "PESSOA",

        "INDIVIDUO": "PESSOA",

        "COLEGA": "AMIGO",

        "PARCEIRO": "AMIGO",

        "COMPANHEIRO": "AMIGO",

        "PAPAI": "PAI",

        "MAMAE": "MAE",

        "SENHOR": "HOMEM",

        "SENHORA": "MULHER",

        "MOCA": "MULHER",

        "RAPAZ": "HOMEM",

        "TU": "VOCE",

        "VOCES": "VOCE",

        "AGENTE": "NOS",



# Objetos / Lugares / Substantivos

        "COLEGIO": "ESCOLA",

        "AULA": "ESCOLA",

        "UNIVERSIDADE": "FACULDADE",

        "CURSO": "FACULDADE",

        "RESIDENCIA": "CASA",

        "LAR": "CASA",

        "MORADIA": "CASA",

        "APARTAMENTO": "CASA",

        "CLINICA": "HOSPITAL",

        "POSTO": "HOSPITAL",

        "EMPREGO": "TRABALHO",

        "SERVICO": "TRABALHO",

        "ESCRITORIO": "TRABALHO",

        "MUNICIPIO": "CIDADE",

        "PAIS": "BRASIL",

        "GRANA": "DINHEIRO",

        "MOEDA": "DINHEIRO",

        "VALOR": "DINHEIRO",

        "PRECO": "DINHEIRO",

        "PAGAMENTO": "BANCO",

        "CAIXA": "BANCO",

        "AGENCIA": "BANCO",

        "SANITARIO": "BANHEIRO",

        "TOALETE": "BANHEIRO",

        "WC": "BANHEIRO",

        "PRIVADA": "BANHEIRO",

        "SOM": "BARULHO",

        "RUIDO": "BARULHO",

        "GRITO": "BARULHO",

        "DOCE": "BALA",

        "BOMBOM": "BALA",

        "ACUCAR": "BALA",

        "FRUTA": "MACA",

        "ALIMENTO": "COMIDA",

        "REFEICAO": "COMIDA",

        "ALMOCO": "COMIDA",

        "JANTAR": "COMIDA",

        "LANCHE": "PAO",

        "BEBIDA": "AGUA",

        "REFRIGERANTE": "SUCO",

        "TELEFONE": "CELULAR",

        "SMARTPHONE": "CELULAR",

        "NOTEBOOK": "COMPUTADOR",

        "PC": "COMPUTADOR",

        "LAPTOP": "COMPUTADOR",

        "AUTOMOVEL": "CARRO",

        "VEICULO": "CARRO",

        "TRANSPORTE": "ONIBUS",

        "APOSTILA": "LIVRO",

        "TEXTO": "LIVRO",

        "LAPIS": "CANETA",

        "PAPEL": "CADERNO",

        "INJECAO": "VACINA",

        "REMEDIO": "VACINA",

        "MEDICAMENTO": "VACINA",

        "RECEIO": "MEDO",

        "PAVOR": "MEDO",

        "ASSUSTADO": "MEDO",

        "SUSTO": "MEDO",

        "MAU": "RUIM",

        "PESSIMO": "RUIM",

        "HORRIVEL": "RUIM",

        "FEIO": "RUIM",

        "OTIMO": "BOM",

        "EXCELENTE": "BOM",

        "LEGAL": "BOM",

        "BEM": "BOM",

        "LINDO": "BONITO",

        "BELO": "BONITO",

        "SIMPLES": "FACIL",

        "COMPLICADO": "DIFICIL",

        "DURO": "DIFICIL",



# Verbos / Ações

        "DESEJAR": "VONTADE",

        "DESEJO": "VONTADE",

        "AFIM": "VONTADE",

        "OCORRER": "ACONTECER",

        "SUCEDER": "ACONTECER",

        "HAVER": "ACONTECER",

        "CURTIR": "APROVEITAR",

        "DESFRUTAR": "APROVEITAR",

        "USAR": "APROVEITAR",

        "COMPREENDER": "ENTENDER",

        "PERCEBER": "ENTENDER",

        "CONHECIMENTO": "SABER",

        "CONHECO": "CONHECER",

        "LER": "ESTUDAR",

        "PESQUISAR": "ESTUDAR",

        "TREINAR": "APRENDER",

        "EXPLICAR": "ENSINAR",

        "ORIENTAR": "ENSINAR",

        "AUXILIAR": "AJUDAR",

        "SOCORRER": "AJUDAR",

        "APOIO": "AJUDAR",

        "ADQUIRIR": "COMPRAR",

        "PAGAR": "COMPRAR",

        "OLHAR": "VER",

        "ENXERGAR": "VER",

        "ASSISTIR": "VER",

        "OBSERVAR": "VER",

        "ALIMENTAR": "COMER",

        "ALMOCAR": "COMER",

        "JANTAR": "COMER",

        "TOMAR": "BEBER",

        "INGERIR": "BEBER",

        "DESCANSAR": "DORMIR",

        "DEITAR": "DORMIR",

        "SONO": "DORMIR",

        "CAMINHAR": "IR",

        "ANDAR": "IR",

        "VIAJAR": "IR",

        "PARTIR": "IR",

        "CHEGAR": "VIR",

        "VOLTAR": "VIR",

        "RETORNAR": "VIR",

        "AMAR": "GOSTAR",

        "ADORAR": "GOSTAR",

        "NECESSITAR": "PRECISAR",

        "CONSEGUIR": "PODER",

    }



# Palavras funcionais do Português que são omitidas na glosa de Libras

# (artigos, preposições, conjunções, verbos de ligação e contrações sem acento)

    PORTUGUESE_STOPWORDS = {

        "O", "A", "OS", "AS", "UM", "UMA", "UNS", "UMAS",

        "DE", "DO", "DA", "DOS", "DAS", "DUM", "DUMA",

        "EM", "NO", "NA", "NOS", "NAS", "NUM", "NUMA",

        "PARA", "PRO", "PRA", "PROS", "PRAS", "COM", "POR",

        "PELO", "PELA", "PELOS", "PELAS", "AO", "AOS", "ATE",

        "QUE", "E", "MAS", "OU", "SE", "COMO", "POIS", "POREM",

        "ESTA", "ESTE", "ESTES", "ESTAS", "ESSE", "ESSA", "ESSES", "ESSAS",

        "ISSO", "ISTO", "AQUILO", "AQUELE", "AQUELA", "AQUELES", "AQUELAS",

        "ERA", "FOI", "SER", "ESTAR", "SOU", "ES", "SOMOS", "SAO",

        "ESTOU", "ESTAMOS", "ESTAO", "ESTAVA", "FORAM", "SEJA",

        "MUITO", "MAIS", "JA", "TAMBEM", "SO", "LHE", "LHES", "ME", "TE", "SE", "NOS"

    }



# Palavras interrogativas e de negação para ativar marcadores faciais

    QUESTION_QU_WORDS = {"QUAL", "QUEM", "QUANDO", "ONDE", "COMO", "PORQUE", "POR-QUE", "QUANTO"}

    NEGATION_WORDS = {"NAO", "NUNCA", "NADA", "NINGUEM", "JAMAIS"}



    def __init__(
        self,
        lexicon_path: Optional[str | Path] = None,
        confidence_threshold: float = 0.75,
        qwen_api_url: Optional[str] = None,
        qwen_api_key: Optional[str] = None,
        qwen_timeout: Optional[float] = None,
    ):
        self.confidence_threshold = confidence_threshold

        # A API fica hospedada no Google Cloud Run; este módulo atua como cliente.
        self.qwen_api_url = (
            qwen_api_url or os.getenv("QWEN3_API_URL") or DEFAULT_QWEN3_API_URL
        ).strip().rstrip("/")
        self.qwen_api_key = qwen_api_key or os.getenv("QWEN3_API_KEY") or DEFAULT_QWEN3_API_KEY
        self.qwen_timeout = float(
            qwen_timeout if qwen_timeout is not None
            else os.getenv("QWEN3_TIMEOUT", "60")
        )
        self._qwen_lock = threading.Lock()
        self._qwen_cache: Dict[str, str] = {}

        if lexicon_path is None:
            lexicon_path = Path(__file__).parent / "lexicon_a6.json"

        self.lexicon_data = self._load_lexicon_file(Path(lexicon_path))
        self.lexicon_version: str = self.lexicon_data.get("lexicon_version", "lexicon-a6-1.3")
        self.markers: List[str] = self.lexicon_data.get("markers", list(self.CLOSED_FACIAL_MARKERS))



# Mapa {GLOSA: CATEGORIA_GRAMATICAL} para as 100 palavras

        self.lexicon_signs: Dict[str, str] = {

            normalize_gloss(item["gloss"]): item.get("category", "OBJETO")

            for item in self.lexicon_data.get("signs", [])

        }

        self.valid_glosses: List[str] = sorted(self.lexicon_signs.keys())



    @staticmethod

    def _load_lexicon_file(path: Path) -> Dict[str, Any]:

        if path.exists():

            return json.loads(path.read_text(encoding="utf-8-sig"))

        raise FileNotFoundError(f"Artefato LEXICON não encontrado em: {path}")



# =========================================================================

# 0. REMOÇÃO PRÉVIA DE STOPWORDS DO PORTUGUÊS

# =========================================================================

    def is_stopword(self, word: str) -> bool:

        """

        Verifica se a palavra normalizada é uma stopword do português.

        Preserva palavras que façam parte do LEXICON oficial, de perguntas QU ou de negação.

        """

        norm = normalize_gloss(word)

        if not norm:

            return True

        if norm in self.lexicon_signs or norm in self.QUESTION_QU_WORDS or norm in self.NEGATION_WORDS:

            return False

        return norm in self.PORTUGUESE_STOPWORDS



    def remove_portuguese_stopwords(self, tokens: List[str]) -> Tuple[List[str], List[str]]:

        """

        Remove stopwords em português de uma lista de tokens ANTES de iniciar as correções.

        Retorna (tokens_filtrados, stopwords_removidas).

        """

        filtered_tokens: List[str] = []

        removed_stopwords: List[str] = []



        for raw_token in tokens:

            if raw_token in ("?", "!"):

                continue

            norm = normalize_gloss(raw_token)

            if not norm:

                continue



            if self.is_stopword(norm):

                removed_stopwords.append(raw_token)

            else:

                filtered_tokens.append(raw_token)



        return filtered_tokens, removed_stopwords



# =========================================================================

# 1. SUGESTÃO DE PALAVRAS DO LÉXICO (Sinônimos + Proximidade Textual)

# =========================================================================

    def suggest_alternatives_for_word(self, word: str, max_suggestions: int = 3) -> List[Dict[str, Any]]:

        """

        Dada uma palavra que não está no LEXICON de 100 sinais, retorna uma lista ordenada

        de sugestões válidas do LEXICON (usando sinônimos diretos e similaridade de caracteres).

        Ignora stopwords do português para evitar falsos positivos por similaridade ortográfica.

        """

        norm = normalize_gloss(word)

        if not norm or self.is_stopword(norm):

            return []



# Se já existe no léxico, ela mesma é 100% válida

        if norm in self.lexicon_signs:

            return [{"gloss": norm, "reason": "presente_no_lexico", "score": 1.0}]



        suggestions: List[Dict[str, Any]] = []



# 1. Verifica mapeamento semântico / sinônimos

        if norm in self.SYNONYM_MAP:

            target = self.SYNONYM_MAP[norm]

            if target in self.lexicon_signs:

                suggestions.append({

                    "gloss": target,

                    "reason": f"sinonimo_semantico ('{norm}' -> '{target}')",

                    "score": 0.95,

                })



# 2. Verifica radicais / prefixos verbais do Português (ex: ESTUDANDO -> ESTUDAR, ALUNOS -> ALUNO)

        for candidate in self.valid_glosses:

            if any(s["gloss"] == candidate for s in suggestions):

                continue

            if (norm.startswith(candidate[:4]) and len(candidate) >= 4) or (

                norm.rstrip("S") == candidate

            ):

                ratio = difflib.SequenceMatcher(None, norm, candidate).ratio()

                suggestions.append({

                    "gloss": candidate,

                    "reason": f"variacao_morfologica ('{norm}' ~ '{candidate}')",

                    "score": round(max(0.80, ratio), 2),

                })



# 3. Similaridade ortográfica (difflib.get_close_matches)

        close_matches = difflib.get_close_matches(norm, self.valid_glosses, n=max_suggestions, cutoff=0.55)

        for match in close_matches:

            if any(s["gloss"] == match for s in suggestions):

                continue

            score = round(difflib.SequenceMatcher(None, norm, match).ratio(), 2)

            suggestions.append({

                "gloss": match,

                "reason": "similaridade_ortografica",

                "score": score,

            })



        suggestions.sort(key=lambda x: x["score"], reverse=True)

        return suggestions[:max_suggestions]



# =========================================================================

# 2. PRÉ-VALIDAÇÃO E CORREÇÃO ANTES DE ENVIAR PARA O S6 (CASO B - PREVENTIVO)

# =========================================================================

    def prepare_synthesis_command(

        self,

        input_words_or_sentence: List[str] | str,

        speed_factor: float = 1.0,

        explicit_marker: Optional[str] = None,

        auto_apply_top_suggestion: bool = True,

    ) -> Dict[str, Any]:

        """

        Compara todas as palavras contra o LEXICON de 100 palavras ANTES de enviar ao S6:

        - Passo 1: Remove as stopwords em português antes de começar as correções;

        - Passo 2: Identifica marcadores faciais gramaticais (Pergunta, Negação, Intensidade);

        - Passo 3: Sugere e aplica correções para palavras fora do léxico;

        - Passo 4: Reordena as glosas na gramática de LIBRAS: [TEMPO] + [SUJEITO] + [OBJETO] + [VERBO];

        - Passo 5: Devolve o payload pronto de `ods.s6.synthesis.command` + relatório de sugestões.

        """

        if isinstance(input_words_or_sentence, str):

            raw_text = input_words_or_sentence

            raw_tokens = raw_text.replace("?", " ? ").replace("!", " ! ").split()

        else:

            raw_text = " ".join(input_words_or_sentence)

            raw_tokens = list(input_words_or_sentence)



        has_question_mark = "?" in raw_text

        has_exclamation = "!" in raw_text



# 1. Remove as stopwords em português ANTES de começar as correções

        tokens_without_stopwords, removed_stopwords = self.remove_portuguese_stopwords(raw_tokens)



        detected_qu = False

        detected_negation = False



        validated_glosses: List[str] = []

        word_corrections: List[Dict[str, Any]] = []

        unresolved_words: List[str] = []



# 2. Inicia as correções apenas sobre os tokens relevantes

        for raw_token in tokens_without_stopwords:

            norm = normalize_gloss(raw_token)

            if not norm:

                continue



            if norm in self.QUESTION_QU_WORDS:

                detected_qu = True

                continue

            if norm in self.NEGATION_WORDS:

                detected_negation = True

                if norm not in self.lexicon_signs:

                    continue



# Palavra já existe diretamente no LEXICON de 100 sinais

            if norm in self.lexicon_signs:

                validated_glosses.append(norm)

            else:

# Palavra fora do léxico -> Aciona motor de Sugestão de Correção ANTES de enviar ao S6

                alternatives = self.suggest_alternatives_for_word(norm)

                chosen = alternatives[0]["gloss"] if (alternatives and auto_apply_top_suggestion) else None



                word_corrections.append({

                    "original_word": raw_token,

                    "normalized": norm,

                    "status": "SUBSTITUIDA_POR_SUGESTAO" if chosen else "SEM_CORRESPONDENCIA_DIRETA",

                    "applied_gloss": chosen,

                    "suggestions": alternatives,

                })



                if chosen:

                    validated_glosses.append(chosen)

                else:

                    unresolved_words.append(norm)



# Reordenação Gramatical da LIBRAS: [TEMPO] + [SUJEITO] + [OBJETO] + [VERBO]

        ordered_glosses, was_reordered = self.reorganize_syntax(validated_glosses)



# Determina o marcador facial do conjunto fechado

        marker_to_use = explicit_marker

        if not marker_to_use:

            if detected_qu:

                marker_to_use = "INTERROGATIVA_QU"

            elif has_question_mark:

                marker_to_use = "INTERROGATIVA_SN"

            elif detected_negation:

                marker_to_use = "NEGACAO"

            elif has_exclamation:

                marker_to_use = "INTENSIDADE"



        markers_payload = []

        if marker_to_use and marker_to_use != "NEUTRO" and ordered_glosses:

            markers_payload.append({

                "marker": marker_to_use,

                "from_gloss_index": 0,

                "to_gloss_index": len(ordered_glosses) - 1,

            })



        command_payload = {

            "glosses": ordered_glosses,

            "speed_factor": float(speed_factor),

            "markers": markers_payload,

            "lexicon_version": self.lexicon_version,

        }



        return {

            "input": input_words_or_sentence,

            "pre_validation": {

                "removed_stopwords": removed_stopwords,

                "tokens_after_stopword_removal": tokens_without_stopwords,

                "all_in_lexicon_originally": len(word_corrections) == 0,

                "word_corrections": word_corrections,

                "unresolved_words": unresolved_words,

            },

            "grammar": {

                "before_syntax_order": validated_glosses,

                "after_syntax_order": ordered_glosses,

                "was_reordered": was_reordered,

                "rule": "[TEMPO] + [SUJEITO] + [OBJETO] + [VERBO]",

                "facial_marker_applied": marker_to_use or "NEUTRO",

            },

            "s6_command_payload": command_payload,

        }



# =========================================================================

# 3. TRATAMENTO DE `ods.i4.sign` (CASO A - LIBRAS -> PORTUGUÊS)

# =========================================================================

    def generate_portuguese_sentence(
        self,
        glosses: List[str],
        context: Optional[str] = None,
    ) -> str:
        """Gera uma frase em português a partir do histórico de glosas usando Qwen3 4B.

        A API remota é hospedada no Cloud Run (concurrency=1).
        Utiliza lock de concorrência, cache em memória e retry com backoff contra HTTP 429.
        """
        if not glosses:
            raise ValueError("Não há glosas para gerar a frase.")

        cleaned_glosses = [str(gloss).strip().upper() for gloss in glosses if str(gloss).strip()]
        if not cleaned_glosses:
            raise ValueError("Lista de glosas sem termos válidos.")

        cache_key = " ".join(cleaned_glosses)
        if context and context.strip():
            cache_key += f"|{context.strip()}"

        with self._qwen_lock:
            if cache_key in self._qwen_cache:
                return self._qwen_cache[cache_key]

            if not self.qwen_api_url:
                raise RuntimeError(
                    "QWEN3_API_URL não configurada. Informe a URL do serviço Cloud Run."
                )
            if not self.qwen_api_key:
                raise RuntimeError(
                    "QWEN3_API_KEY não configurada. Informe a chave da API do Qwen3."
                )

            endpoint = self.qwen_api_url
            if not endpoint.endswith("/gerar"):
                endpoint += "/gerar"

            request_data: Dict[str, Any] = {
                "palavras": cleaned_glosses
            }
            if context and context.strip():
                request_data["contexto"] = context.strip()

            request = urllib.request.Request(
                url=endpoint,
                data=json.dumps(request_data, ensure_ascii=False).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "X-API-Key": self.qwen_api_key,
                },
                method="POST",
            )

            max_retries = 3
            backoff_base = 1.5
            response_text = ""

            for attempt in range(max_retries + 1):
                try:
                    with urllib.request.urlopen(request, timeout=self.qwen_timeout) as response:
                        response_text = response.read().decode("utf-8")
                    break
                except urllib.error.HTTPError as exc:
                    # Cloud Run concurrency=1: se ocupado, aguarda a requisição anterior terminar
                    if exc.code in (429, 503) and attempt < max_retries:
                        time.sleep(backoff_base * (attempt + 1))
                        continue
                    detail = exc.read().decode("utf-8", errors="replace")[:500]
                    raise RuntimeError(
                        f"A API Qwen3 respondeu HTTP {exc.code}: {detail}"
                    ) from exc
                except urllib.error.URLError as exc:
                    raise RuntimeError(
                        f"Não foi possível acessar a API Qwen3: {exc.reason}"
                    ) from exc
                except TimeoutError as exc:
                    raise RuntimeError(
                        f"A API Qwen3 excedeu o timeout de {self.qwen_timeout:g} segundos."
                    ) from exc

            try:
                response_data = json.loads(response_text)
            except json.JSONDecodeError as exc:
                raise RuntimeError("A API Qwen3 retornou uma resposta que não é JSON válido.") from exc

            sentence = response_data.get("frase") if isinstance(response_data, dict) else None
            if not isinstance(sentence, str) or not sentence.strip():
                raise RuntimeError("A API Qwen3 não retornou uma frase válida no campo 'frase'.")

            result = sentence.strip()
            # Se o modelo gerou raciocínio prévio em múltiplas linhas, extrai a frase final
            if "\n" in result:
                lines = [line.strip() for line in result.splitlines() if line.strip()]
                quoted = [line.strip(' "\'') for line in lines if (line.startswith('"') and line.endswith('"')) or (line.startswith("'") and line.endswith("'"))]
                if quoted:
                    result = quoted[-1]
                elif lines:
                    result = lines[-1]
            result = result.strip(' "\'')
            self._qwen_cache[cache_key] = result
            return result


    def evaluate_incoming_i4_sign(

        self,

        payload: Dict[str, Any],

        session_history: Optional[List[str]] = None,

    ) -> Dict[str, Any]:

        """

        Analisa o evento `ods.i4.sign` publicado pelo I4:

        - Verifica se `confidence` é menor que o limiar (ex: 0.75) ou se o sinal não está no léxico.

        - Extrai as alternativas de `alternatives` do payload de I4 e também complementa com o LEXICON se necessário.

        - Constrói a frase acumulada com a ordem sintática para exibição ao usuário.

        """

        raw_sign = payload.get("sign") or payload.get("sinal", "DESCONHECIDO")

        sign_norm = normalize_gloss(raw_sign)

        confidence = float(payload.get("confidence") or payload.get("confianca", 0.0))

        i4_alternatives = payload.get("alternatives", [])



        in_lexicon = sign_norm in self.lexicon_signs

        low_confidence = confidence < self.confidence_threshold

        needs_suggestion = low_confidence or (not in_lexicon)



        suggestions: List[Dict[str, Any]] = []

# 1. Prioriza as alternativas enviadas pelo classificador de I4 no campo `alternatives`

        for alt in i4_alternatives:

            alt_sign = normalize_gloss(alt.get("sign", ""))

            alt_conf = float(alt.get("confidence", 0.0))

            if alt_sign:

                suggestions.append({

                    "gloss": alt_sign,

                    "confidence": alt_conf,

                    "in_lexicon": alt_sign in self.lexicon_signs,

                    "source": "i4_alternatives",

                })



# 2. Se o sinal não estiver no léxico, busca no LEXICON local de 100 palavras

        if not in_lexicon:

            for lex_sug in self.suggest_alternatives_for_word(sign_norm):

                if not any(s["gloss"] == lex_sug["gloss"] for s in suggestions):

                    suggestions.append({

                        "gloss": lex_sug["gloss"],

                        "confidence": lex_sug["score"],

                        "in_lexicon": True,

                        "source": f"a6_lexicon ({lex_sug['reason']})",



                    })

        # Mantém a ordem gramatical de LIBRAS para metadados/compatibilidade.
        # A frase em português, por outro lado, é construída pela Qwen3.
        history = list(session_history or []) + [sign_norm]
        ordered_libras_glosses, was_reordered = self.reorganize_syntax(history)

        additional_context = payload.get("contexto") or payload.get("context")
        qwen_error: Optional[str] = None
        portuguese_sentence: Optional[str] = None
        try:
            portuguese_sentence = self.generate_portuguese_sentence(
                glosses=history,
                context=str(additional_context) if additional_context else None,
            )
            qwen_status = "OK"

            # Exibe a frase gerada no stdout do processo que executa o servidor.
            # Com isso, ela aparece no terminal local e também nos logs do Cloud Run.
            print("\n[QWEN3] Frase em português:", flush=True)
            print(portuguese_sentence, flush=True)
            print(f"[QWEN3] Glosas utilizadas: {' '.join(history)}\n", flush=True)
        except (RuntimeError, ValueError) as exc:
            # A falha do serviço de IA não interrompe o processamento do evento I4.
            qwen_status = "ERRO"
            qwen_error = str(exc)
            print(f"\n[QWEN3] Erro ao gerar frase: {qwen_error}\n", flush=True)



        if not in_lexicon:

            status_msg = f"Sinal '{sign_norm}' fora do LEXICON. Sugerindo alternativas."

        elif low_confidence:

            status_msg = (

                f"Baixa confiança ({confidence:.2f} < {self.confidence_threshold:.2f}) para '{sign_norm}'. "

                f"Oferecendo alternativas de I4."

            )

        else:

            status_msg = f"Sinal '{sign_norm}' reconhecido com alta confiança ({confidence:.2f})."



        return {

            "recognized_sign": sign_norm,

            "confidence": confidence,

            "in_lexicon": in_lexicon,

            "needs_correction_suggestion": needs_suggestion,

            "status_message": status_msg,

            "suggested_alternatives": suggestions,

            "sentence_context": {
                "accumulated_glosses": history,
                # Esta lista segue a organização de glosas de LIBRAS, não é a frase em português.
                "grammatical_order": ordered_libras_glosses,
                "was_reordered": was_reordered,
                # Frase natural em português gerada pelo Qwen3 4B.
                "portuguese_sentence": portuguese_sentence,
                "qwen3_generation": {
                    "status": qwen_status,
                    "model": "qwen3:4b" if portuguese_sentence else None,
                    "error": qwen_error,
                },
            },

        }



# =========================================================================

# 4. TRATAMENTO DE `ods.s6.synthesis.reply` (FALLBACK DE `unknown_signs`)

# =========================================================================

    def evaluate_s6_synthesis_reply(self, reply_payload: Dict[str, Any]) -> Dict[str, Any]:

        """

        Analisa a reply `ods.s6.synthesis.reply` vinda de S6:

        - Se `unknown_signs` vier preenchido, remove eventuais stopwords e oferece alternativas do LEXICON.

        """

        raw_unknown_signs = reply_payload.get("unknown_signs") or []

        filtered_unknowns, removed_stopwords = self.remove_portuguese_stopwords(raw_unknown_signs)



        suggestions_by_unknown: Dict[str, List[Dict[str, Any]]] = {}

        for unk in filtered_unknowns:

            suggestions_by_unknown[unk] = self.suggest_alternatives_for_word(unk)



        return {

            "status": reply_payload.get("status", "OK"),

            "trajectory_ref": reply_payload.get("trajectory_ref"),

            "duration_ms": reply_payload.get("duration_ms"),

            "facial_markers": reply_payload.get("facial_markers", []),

            "unknown_signs": filtered_unknowns,

            "ignored_stopwords": removed_stopwords,

            "has_unknown_signs": len(filtered_unknowns) > 0,

            "suggestions_for_unknown_signs": suggestions_by_unknown,

        }



# =========================================================================

# 5. GRAMÁTICA LIBRAS: ORDENAÇÃO [TEMPO] + [SUJEITO] + [OBJETO] + [VERBO]

# =========================================================================

    def reorganize_syntax(self, glosses: List[str]) -> Tuple[List[str], bool]:

        """Ordena glosas para o payload de LIBRAS: [TEMPO] + [SUJEITO] + [OBJETO] + [VERBO].

        Este método permanece determinístico porque prepara glosas para o S6.
        A geração de frases em português é responsabilidade de generate_portuguese_sentence().
        """

        if not glosses or len(glosses) <= 1:

            return [normalize_gloss(g) for g in glosses], False



        tempo: List[str] = []

        sujeito: List[str] = []

        objeto: List[str] = []

        verbo: List[str] = []

        outros: List[str] = []



        normalized_input = [normalize_gloss(g) for g in glosses]

        for g in normalized_input:

            role = self.lexicon_signs.get(g)

            if role == "TEMPO":

                tempo.append(g)

            elif role == "SUJEITO":

                sujeito.append(g)

            elif role == "OBJETO":

                objeto.append(g)

            elif role == "VERBO":

                verbo.append(g)

            else:

                outros.append(g)



        reordered = tempo + sujeito + objeto + verbo + outros

        return reordered, (normalized_input != reordered)
