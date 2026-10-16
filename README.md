#  Mapeamento Geométrico das Expressões Não-Manuais (ENMs) em LIBRAS

Na Língua Brasileira de Sinais (LIBRAS), as **Expressões Não-Manuais (ENMs)** desempenham um papel gramatical fundamental para definir a intenção (perguntas, afirmações, negações) e a intensidade dos sinais.

Para capturar essas variações de forma robusta e independente da distância ou ângulo do usuário em relação à câmera, utilizou-se o método de **Aspect Ratios (Razões de Aspecto)** sobre a malha tridimensional de pontos (*landmarks*) do **MediaPipe Face Mesh**.

### 1. Eye Aspect Ratio (EAR) — *Abertura dos Olhos*

* **Descrição:** Mede a proporção de abertura da fenda palpebral em relação ao comprimento do olho. Permite identificar olhos arregalados, piscadas ou olhos semi-cerrados.

* **Fórmula Matemática:**

$$
\text{EAR} = \frac{\vert{}\vert{}p_{160} - p_{144}\vert{}\vert{} + \vert{}\vert{}p_{158} - p_{153}\vert{}\vert{}}{2 \cdot \vert{}\vert{}p_{33} - p_{133}\vert{}\vert{}}
$$

* **Mapeamento dos Pontos (MediaPipe):**

  * $p_{160}, p_{158}$: Pálpebra superior (olho esquerdo).

  * $p_{144}, p_{153}$: Pálpebra inferior (olho esquerdo).

  * $p_{33}$: Canto externo do olho esquerdo.

  * $p_{133}$: Canto interno do olho esquerdo.

* **Aplicação em LIBRAS:**

  * **EAR Elevado (**$> 0.28$**):** Olhos arregalados — acompanham perguntas gerais (*Sim/Não*), espanto, surpresa e intensidade.

  * **EAR Baixo (**$< 0.18$**):** Olhos semi-cerrados — acompanham dúvida, foco ou verificação de detalhes.

### 2. Mouth Aspect Ratio (MAR) — *Abertura Vertical da Boca*

* **Descrição:** Mede a proporção da abertura vertical dos lábios em relação à largura total da boca.

* **Fórmula Matemática:**

$$
\text{MAR} = \frac{\vert{}\vert{}p_{13} - p_{14}\vert{}\vert{} + \vert{}\vert{}p_{82} - p_{87}\vert{}\vert{} + \vert{}\vert{}p_{312} - p_{317}\vert{}\vert{}}{3 \cdot \vert{}\vert{}p_{61} - p_{291}\vert{}\vert{}}
$$

* **Mapeamento dos Pontos (MediaPipe):**

  * $p_{13}$: Centro do lábio superior (parte interna).

  * $p_{14}$: Centro do lábio inferior (parte interna).

  * $p_{82}, p_{87}$: Lábio superior e inferior no lado esquerdo.

  * $p_{312}, p_{317}$: Lábio superior e inferior no lado direito.

  * $p_{61}$: Canto esquerdo da boca.

  * $p_{291}$: Canto direito da boca.

* **Aplicação em LIBRAS:**

  * **MAR Elevado (**$> 0.38$**):** Boca aberta — necessária para reproduzir fonemas faciais visuais (como *"A-A-A"*, *"O-O-O"*, *"PÓ"*), espanto ou ênfase.

  * **MAR Baixo (**$< 0.18$**):** Lábios cerrados, em repouso ou comprimidos.

### 3. Brow Aspect Ratio (BAR) — *Movimentação das Sobrancelhas*

* **Descrição:** Mede a altura relativa das sobrancelhas acima dos olhos e a aproximação entre elas, ambas normalizadas pela distância interocular (entre os olhos).

* **Fórmulas Matemáticas:**

$$
\text{BAR}_{\text{altura}} = \frac{\frac{\vert{}\vert{}p_{70} - p_{159}\vert{}\vert{} + \vert{}\vert{}p_{300} - p_{386}\vert{}\vert{}}{2}}{\vert{}\vert{}p_{33} - p_{263}\vert{}\vert{}} \qquad \text{BAR}_{\text{juntas}} = \frac{\vert{}\vert{}p_{70} - p_{300}\vert{}\vert{}}{\vert{}\vert{}p_{33} - p_{263}\vert{}\vert{}}
$$

* **Mapeamento dos Pontos (MediaPipe):**

  * $p_{70}$: Ponto mais alto da sobrancelha esquerda.

  * $p_{300}$: Ponto mais alto da sobrancelha direita.

  * $p_{159}$: Topo da pálpebra superior do olho esquerdo.

  * $p_{386}$: Topo da pálpebra superior do olho direito.

  * $p_{33}$: Canto externo do olho esquerdo *(referência interocular)*.

  * $p_{263}$: Canto externo do olho direito *(referência interocular)*.

* **Aplicação em LIBRAS:**

$\text{BAR}_{\text{altura}}$ Elevada ($> 0.32$): Sobrancelhas levantadas — marca gramatical de **Perguntas Gerais** (*resposta Sim ou Não*).

$\text{BAR}_{\text{juntas}}$ Baixa ($< 0.30$): Sobrancelhas franzidas/juntas — marca gramatical de **Perguntas Específicas** (*Quem, Onde, O quê, Por quê*).

### 4. Normalized Mouth Corner Ratio (NMAR) — *Esticamento Lateral da Boca*

* **Descrição:** Mede a largura horizontal da boca normalizada em relação à largura dos olhos.

* **Fórmula Matemática:**

$$
\text{NMAR} = \frac{\vert{}\vert{}p_{61} - p_{291}\vert{}\vert{}}{\vert{}\vert{}p_{33} - p_{263}\vert{}\vert{}}
$$

* **Mapeamento dos Pontos (MediaPipe):**

  * $p_{61}$: Canto esquerdo da boca.

  * $p_{291}$: Canto direito da boca.

  * $p_{33}$: Canto externo do olho esquerdo.

  * $p_{263}$: Canto externo do olho direito.

* **Aplicação em LIBRAS:**

  * **NMAR Elevado (**$> 0.70$**) com MAR Baixo:** Boca esticada lateralmente — indica sorriso, afirmação positiva, confirmação ou expressão de esforço/tensão.

### 5. Pucker Ratio (PUP) — *Bico / Lábios Projetados*

* **Descrição:** Mede a proporção entre a altura externa central dos lábios e a largura total da boca.

* **Fórmula Matemática:**

$$
\text{PUP} = \frac{\vert{}\vert{}p_{0} - p_{17}\vert{}\vert{}}{\vert{}\vert{}p_{61} - p_{291}\vert{}\vert{}}
$$

* **Mapeamento dos Pontos (MediaPipe):**

  * $p_{0}$: Ponto central superior do lábio externo.

  * $p_{17}$: Ponto central inferior do lábio externo.

  * $p_{61}$: Canto esquerdo da boca.

  * $p_{291}$: Canto direito da boca.

* **Aplicação em LIBRAS:**

  * **PUP Elevado (**$> 0.45$**):** Lábios projetados para a frente ("bico") — utilizado para reproduzir fonemas como *"PSI"*, *"PE"*, *"PÚ"*, ou para indicar coisas pequenas, finas e precisas.

### 6. Cheek Aspect Ratio (CAR) — *Bochechas Infladas*

* **Descrição:** Mede a expansão lateral da região média da face em relação à altura do terço inferior do rosto (ponta do nariz ao queixo).

* **Fórmula Matemática:**

$$
\text{CAR} = \frac{\vert{}\vert{}p_{234} - p_{454}\vert{}\vert{}}{\vert{}\vert{}p_{1} - p_{152}\vert{}\vert{}}
$$

* **Mapeamento dos Pontos (MediaPipe):**

  * $p_{234}$: Ponto mais externo da bochecha esquerda (osso zigomático).

  * $p_{454}$: Ponto mais externo da bochecha direita.

  * $p_{1}$: Ponta do nariz.

  * $p_{152}$: Ponto inferior do queixo (mento).

* **Aplicação em LIBRAS:**

  * **CAR Elevado (**$> 1.65$**):** Bochechas infladas — marca gramatical que expressa **grande volume, peso, quantidade ou intensidade** (ex: *muito pesado*, *muita gente*).

### 7. Nasal Aspect Ratio (NAR) — *Nariz Franzido*

* **Descrição:** Mede a compressão vertical do dorso do nariz em relação à distância entre os cantos internos dos olhos.

* **Fórmula Matemática:**

$$
\text{NAR} = \frac{\vert{}\vert{}p_{198} - p_{2}\vert{}\vert{}}{\vert{}\vert{}p_{133} - p_{362}\vert{}\vert{}}
$$

* **Mapeamento dos Pontos (MediaPipe):**

  * $p_{198}$: Topo do dorso do nariz (região da glabela).

  * $p_{2}$: Base inferior do nariz.

  * $p_{133}$: Canto interno do olho esquerdo.

  * $p_{362}$: Canto interno do olho direito.

* **Aplicação em LIBRAS:**

  * **NAR Baixo (**$< 0.38$**):** Nariz franzido/comprimido — acompanha expressões de nojo, aversão, negação ou intensidade extrema em perguntas.