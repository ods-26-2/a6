import cv2
import mediapipe as mp
import json
import os
from datetime import datetime

# --- CONFIGURAÇÃO DE PASTA DE SAÍDA ---
PASTA_DATASET = 'dataset_maos'
PASTA_IMAGENS = os.path.join(PASTA_DATASET, 'imagens')
PASTA_JSON = os.path.join(PASTA_DATASET, 'keypoints')

os.makedirs(PASTA_IMAGENS, exist_ok=True)
os.makedirs(PASTA_JSON, exist_ok=True)

# Configuração do MediaPipe Hands
mp_hands = mp.solutions.hands
mp_drawing = mp.solutions.drawing_utils
mp_drawing_styles = mp.solutions.drawing_styles

cap = cv2.VideoCapture(0)

# Tenta aplicar resolução HD
cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)

NOME_JANELA = 'Rotulador de Maos LIBRAS'
cv2.namedWindow(NOME_JANELA, cv2.WINDOW_NORMAL | cv2.WINDOW_KEEPRATIO)
cv2.resizeWindow(NOME_JANELA, 1280, 720)

modo_rotulacao = False
frame_congelado = None
dados_maos_congelados = []
input_texto_legenda = ""

with mp_hands.Hands(
    model_complexity=1,
    min_detection_confidence=0.6,
    min_tracking_confidence=0.6,
    max_num_hands=2
) as hands:

    while cap.isOpened():
        if not modo_rotulacao:
            ret, frame = cap.read()
            if not ret or frame is None:
                print("[ERRO] Falha ao capturar frame da câmera.")
                break

            frame = cv2.flip(frame, 1)
            altura, largura, _ = frame.shape

            image_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            image_rgb.flags.writeable = False
            results = hands.process(image_rgb)
            image_rgb.flags.writeable = True
            image = cv2.cvtColor(image_rgb, cv2.COLOR_RGB2BGR)

            dados_maos_atuais = []

            if results.multi_hand_landmarks:
                for hand_idx, hand_landmarks in enumerate(results.multi_hand_landmarks):
                    # Desenha as conexões e os 21 pontos da mão na tela
                    mp_drawing.draw_landmarks(
                        image,
                        hand_landmarks,
                        mp_hands.HAND_CONNECTIONS,
                        mp_drawing_styles.get_default_hand_landmarks_style(),
                        mp_drawing_styles.get_default_hand_connections_style()
                    )

                    # Identifica se é mão Esquerda ou Direita
                    lado_mao = "Desconhecido"
                    if results.multi_handedness:
                        lado_mao = results.multi_handedness[hand_idx].classification[0].label

                    # Converte os 21 pontos 3D para lista
                    pistas_pontos = []
                    for idx, lm in enumerate(hand_landmarks.landmark):
                        pistas_pontos.append({
                            'id': idx,
                            'x': float(lm.x),
                            'y': float(lm.y),
                            'z': float(lm.z)
                        })

                    dados_maos_atuais.append({
                        'mao_indice': hand_idx,
                        'lado': lado_mao,
                        'keypoints': pistas_pontos
                    })

                cv2.putText(image, "[ESPACO] Congelar e Salvar Sinal", (20, 40),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2)
            else:
                cv2.putText(image, "Posicione a mao na tela...", (20, 40),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 255), 2)

            frame_exibicao = image.copy()
            dados_maos_congelados = dados_maos_atuais
        else:
            # Modo de congelamento para digitação da legenda
            frame_exibicao = frame_congelado.copy()
            altura, largura, _ = frame_exibicao.shape

            # Caixa de diálogo no centro da tela
            overlay = frame_exibicao.copy()
            cv2.rectangle(overlay, (50, altura//2 - 100), (largura - 50, altura//2 + 100), (0, 0, 0), -1)
            cv2.addWeighted(overlay, 0.75, frame_exibicao, 0.25, 0, frame_exibicao)

            cv2.putText(frame_exibicao, "DIGITE A LEGENDA DO SINAL DA MAO:", (70, altura//2 - 50),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2)

            cv2.rectangle(frame_exibicao, (70, altura//2 - 20), (largura - 70, altura//2 + 30), (255, 255, 255), 2)
            cv2.putText(frame_exibicao, input_texto_legenda + "|", (80, altura//2 + 15),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 255), 2)

            cv2.putText(frame_exibicao, "[ESC] Cancelar e Tirar Outra | [ENTER] Confirmar e Salvar", (70, altura//2 + 70),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.5, (200, 200, 200), 1)

        cv2.imshow(NOME_JANELA, frame_exibicao)

        key = cv2.waitKey(10) & 0xFF

        # TECLA ESPAÇO -> CONGELA SE HOUVER MÃO DETECTADA
        if key == 32 and not modo_rotulacao and len(dados_maos_congelados) > 0:
            modo_rotulacao = True
            frame_congelado = image.copy()
            input_texto_legenda = ""

        # TECLA ESC -> CANCELA E VOLTA À CÂMERA
        elif key == 27 and modo_rotulacao:
            modo_rotulacao = False
            frame_congelado = None

        # TECLA ENTER -> SALVA A FOTO E O ARQUIVO JSON DE KEYPOINTS
        elif key == 13 and modo_rotulacao and len(input_texto_legenda.strip()) > 0:
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            legenda_limpa = input_texto_legenda.strip().replace(" ", "_")
            
            nome_base = f"{legenda_limpa}_{timestamp_str}"
            nome_img = f"{nome_base}.jpg"
            nome_json = f"{nome_base}.json"

            caminho_img = os.path.join(PASTA_IMAGENS, nome_img)
            caminho_json = os.path.join(PASTA_JSON, nome_json)

            # 1. Salva a foto JPG do momento exato
            cv2.imwrite(caminho_img, frame_congelado)

            # 2. Salva o arquivo JSON com a legenda e os keypoints 3D
            conteudo_json = {
                'legenda': legenda_limpa,
                'timestamp': timestamp_str,
                'arquivo_imagem': nome_img,
                'quantidade_maos': len(dados_maos_congelados),
                'maos': dados_maos_congelados
            }

            with open(caminho_json, 'w', encoding='utf-8') as f:
                json.dump(conteudo_json, f, indent=4)

            print(f"\n[SALVO COM SUCESSO]")
            print(f" -> Legenda: {legenda_limpa}")
            print(f" -> Imagem:  {caminho_img}")
            print(f" -> JSON:    {caminho_json}\n")

            modo_rotulacao = False
            frame_congelado = None

        # CAPTURA DA DIGITAÇÃO DO NOME DO SINAL
        elif modo_rotulacao:
            if key == 8:  # Backspace
                input_texto_legenda = input_texto_legenda[:-1]
            elif 32 <= key <= 126:
                input_texto_legenda += chr(key)

        # TECLA Q PARA ENCERRAR
        if key == ord('q'):
            break

        if cv2.getWindowProperty(NOME_JANELA, cv2.WND_PROP_VISIBLE) < 1:
            break

cap.release()
cv2.destroyAllWindows()