import cv2
import mediapipe as mp
import numpy as np
import json
import csv
import os
import sys
from datetime import datetime
import calculos_ar as ar

print("[DIAGNÓSTICO] Iniciando o script...")

# 1. VERIFICAÇÃO DA CÂMERA
cap = cv2.VideoCapture(0)

if not cap.isOpened():
    print("\n[ERRO CRÍTICO] Não foi possível acessar a webcam!")
    print("-> Verifique se a câmera está conectada ou em uso por outro app.")
    print("-> No Linux, tente rodar: ls /dev/video*")
    sys.exit(1)

# Tenta definir resolução (se falhar, o OpenCV usa a nativa)
cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)

ret_teste, frame_teste = cap.read()
if not ret_teste or frame_teste is None:
    print("\n[ERRO CRÍTICO] A câmera abriu, mas não enviou nenhum frame (ret = False).")
    cap.release()
    sys.exit(1)

print(f"[OK] Câmera capturada com sucesso! Resolução inicial: {frame_teste.shape[1]}x{frame_teste.shape[0]}")

# 2. DIRETÓRIOS E CSV
PASTA_DATASET = 'dataset_libras'
PASTA_IMAGENS = os.path.join(PASTA_DATASET, 'imagens')
ARQUIVO_CSV = os.path.join(PASTA_DATASET, 'rotulos_ratios.csv')

os.makedirs(PASTA_IMAGENS, exist_ok=True)

if not os.path.exists(ARQUIVO_CSV):
    with open(ARQUIVO_CSV, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'arquivo_imagem', 'classe', 
                         'bar_altura', 'bar_juntas', 'ear_medio', 'mar', 
                         'nmar', 'pup', 'car', 'nar', 'yaw'])

mp_holistic = mp.solutions.holistic
mp_drawing = mp.solutions.drawing_utils

PONTOS_OLHO_ESQ = [160, 144, 158, 153, 33, 133]
PONTOS_OLHO_DIR = [385, 380, 387, 373, 362, 263]
PONTOS_VERMELHOS = [165, 391, 164, 18]

NOME_JANELA = 'Rotulador de Expressoes LIBRAS'

try:
    cv2.namedWindow(NOME_JANELA, cv2.WINDOW_NORMAL | cv2.WINDOW_KEEPRATIO)
    cv2.resizeWindow(NOME_JANELA, 1280, 720)
    print("[OK] Janela gráfica do OpenCV inicializada.")
except Exception as e:
    print(f"\n[ERRO GUI] Falha ao criar janela no OpenCV: {e}")
    print("-> Se estiver via SSH, certifique-se de executar com 'ssh -X' ou estar no terminal local do Linux.")
    sys.exit(1)

modo_rotulacao = False
frame_congelado = None
landmarks_congelados = None
vetor_ratios_congelado = None
input_texto_classe = ""

print("\n[RODANDO] Pressione ESPAÇO na janela para congelar e rotular. Pressione Q para sair.\n")

with mp_holistic.Holistic(
    min_detection_confidence=0.5,
    min_tracking_confidence=0.5
) as holistic:

    while cap.isOpened():
        if not modo_rotulacao:
            ret, frame = cap.read()
            if not ret or frame is None:
                print("[AVISO] Perda de frame da câmera.")
                break

            frame = cv2.flip(frame, 1)
            altura, largura, _ = frame.shape

            image_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            image_rgb.flags.writeable = False
            results = holistic.process(image_rgb)
            image_rgb.flags.writeable = True
            image = cv2.cvtColor(image_rgb, cv2.COLOR_RGB2BGR)

            if results.face_landmarks:
                landmarks = results.face_landmarks.landmark
                landmarks_list = [[lm.x, lm.y, lm.z] for lm in landmarks]

                ear_esq = ar.calcular_ear(PONTOS_OLHO_ESQ, landmarks, largura, altura)
                ear_dir = ar.calcular_ear(PONTOS_OLHO_DIR, landmarks, largura, altura)
                ear_medio = (ear_esq + ear_dir) / 2.0

                mar = ar.calcular_mar(landmarks, largura, altura)
                bar_altura, bar_juntas = ar.calcular_bar(landmarks, largura, altura)
                nmar = ar.calcular_nmar(landmarks, largura, altura)
                pup = ar.calcular_pup(landmarks, largura, altura)
                car = ar.calcular_car(landmarks, largura, altura)
                nar = ar.calcular_nar(landmarks, largura, altura)
                yaw = ar.estimar_yaw(landmarks)

                vetor_ratios = [bar_altura, bar_juntas, ear_medio, mar, nmar, pup, car, nar, yaw]

                mp_drawing.draw_landmarks(
                    image, results.face_landmarks, mp_holistic.FACEMESH_TESSELATION,
                    mp_drawing.DrawingSpec(color=(80, 110, 10), thickness=1, circle_radius=1)
                )

                for idx in PONTOS_VERMELHOS:
                    pt = landmarks[idx]
                    cx, cy = int(pt.x * largura), int(pt.y * altura)
                    cv2.circle(image, (cx, cy), 5, (0, 0, 255), -1)

                cv2.putText(image, "[ESPACO] Congelar e Rotular", (20, 40),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2)

            frame_exibicao = image.copy()
        else:
            frame_exibicao = frame_congelado.copy()
            altura, largura, _ = frame_exibicao.shape

            overlay = frame_exibicao.copy()
            cv2.rectangle(overlay, (50, altura//2 - 100), (largura - 50, altura//2 + 100), (0, 0, 0), -1)
            cv2.addWeighted(overlay, 0.75, frame_exibicao, 0.25, 0, frame_exibicao)

            cv2.putText(frame_exibicao, "DIGITE A LEGENDA DA EXPRESSAO:", (70, altura//2 - 50),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2)

            cv2.rectangle(frame_exibicao, (70, altura//2 - 20), (largura - 70, altura//2 + 30), (255, 255, 255), 2)
            cv2.putText(frame_exibicao, input_texto_classe + "|", (80, altura//2 + 15),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 255), 2)

            cv2.putText(frame_exibicao, "[ESC] Tirar Nova Foto | [ENTER] Confirmar e Salvar", (70, altura//2 + 70),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.5, (200, 200, 200), 1)

        cv2.imshow(NOME_JANELA, frame_exibicao)

        key = cv2.waitKey(10) & 0xFF

        if key == 32 and not modo_rotulacao and results.face_landmarks:
            modo_rotulacao = True
            frame_congelado = image.copy()
            landmarks_congelados = landmarks_list
            vetor_ratios_congelado = vetor_ratios
            input_texto_classe = ""

        elif key == 27 and modo_rotulacao:
            modo_rotulacao = False
            frame_congelado = None

        elif key == 13 and modo_rotulacao and len(input_texto_classe.strip()) > 0:
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            legenda_limpa = input_texto_classe.strip().replace(" ", "_")
            
            nome_base = f"{legenda_limpa}_{timestamp_str}"
            nome_img = f"{nome_base}.jpg"
            caminho_img = os.path.join(PASTA_IMAGENS, nome_img)

            cv2.imwrite(caminho_img, frame_congelado)

            caminho_json = os.path.join(PASTA_IMAGENS, f"{nome_base}.json")
            with open(caminho_json, 'w', encoding='utf-8') as f_json:
                json.dump({
                    'legenda': legenda_limpa,
                    'timestamp': timestamp_str,
                    'arquivo_imagem': nome_img,
                    'landmarks_3d': landmarks_congelados,
                    'ratios': {
                        'bar_altura': vetor_ratios_congelado[0],
                        'bar_juntas': vetor_ratios_congelado[1],
                        'ear_medio': vetor_ratios_congelado[2],
                        'mar': vetor_ratios_congelado[3],
                        'nmar': vetor_ratios_congelado[4],
                        'pup': vetor_ratios_congelado[5],
                        'car': vetor_ratios_congelado[6],
                        'nar': vetor_ratios_congelado[7],
                        'yaw': vetor_ratios_congelado[8]
                    }
                }, f_json, indent=4)

            with open(ARQUIVO_CSV, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([timestamp_str, nome_img, legenda_limpa] + vetor_ratios_congelado)

            print(f"[SALVO] Legenda: '{legenda_limpa}' | Imagem: {nome_img}")

            modo_rotulacao = False
            frame_congelado = None

        elif modo_rotulacao:
            if key == 8:
                input_texto_classe = input_texto_classe[:-1]
            elif 32 <= key <= 126:
                input_texto_classe += chr(key)

        if key == ord('q'):
            print("[ENCERRADO] Finalizado pelo usuário.")
            break

        if cv2.getWindowProperty(NOME_JANELA, cv2.WND_PROP_VISIBLE) < 1:
            print("[ENCERRADO] Janela fechada.")
            break

cap.release()
cv2.destroyAllWindows()