import paho.mqtt.client as mqtt
import psycopg2
import json
import requests
import datetime
import urllib.parse
import ssl
import time

# --- CONFIGURAÇÕES ---
DB_URL = "postgresql://postgres:wQsuidbkKmMEmLCpNPuCwQCvdsUAdCUl@ballast.proxy.rlwy.net:56019/railway"
MQTT_BROKER = "2f05eafddad44f3d9511e4b2313e07b9.s1.eu.hivemq.cloud"
MQTT_USER = "top1nfo"
MQTT_PASS = "Top1nfo2026"
TOPIC = "top1nfo/fazenda_piloto/sensor_solo"

# --- WHATSAPP ---
WHATSAPP_PHONE = "%2B5512996005169"
WHATSAPP_APIKEY = "7714077"
ultimo_alerta = 0

def enviar_alerta_whatsapp(temp):
    global ultimo_alerta
    agora = time.time()
    if (agora - ultimo_alerta) > 3600:
        try:
            msg = f"🚨 *Alerta top1nfo:* Temperatura em {temp}C!"
            url = f"https://api.callmebot.com/whatsapp.php?phone={WHATSAPP_PHONE}&text={urllib.parse.quote(msg)}&apikey={WHATSAPP_APIKEY}"
            requests.get(url, timeout=10)
            ultimo_alerta = agora
            print("📱 [WHATSAPP] Alerta enviado!")
        except: print("❌ [WHATSAPP] Erro no envio")

# --- BANCO DE DADOS ---
def salvar_no_banco(temp, umi):
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        query = "INSERT INTO leituras_cafe (temperatura, umidade, sensor_id) VALUES (%s, %s, %s)"
        cursor.execute(query, (temp, umi, "ESP32_Fazenda"))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"☕ [DATABASE] Salvo com sucesso: {temp}C | {umi}%")
    except Exception as e:
        print(f"❌ [DATABASE ERROR] {e}")

# --- CALLBACKS MQTT (O SEGREDO DA ESTABILIDADE) ---
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ [MQTT] Conectado ao Broker HiveMQ!")
        # IMPORTANTE: Inscrever-se aqui dentro garante que ele sempre ouça
        client.subscribe(TOPIC)
        print(f"📡 [MQTT] Inscrito com sucesso no topico: {TOPIC}")
    else:
        print(f"❌ [MQTT] Erro na conexao. Codigo: {rc}")

def on_message(client, userdata, msg):
    # Log imediato para saber que o dado ENCOSTOU no Worker
    print(f"📩 [MQTT] MENSAGEM RECEBIDA! Topico: {msg.topic}")
    try:
        payload = msg.payload.decode()
        print(f"📦 [PAYLOAD] {payload}")
        data = json.loads(payload)
        salvar_no_banco(data.get('temp'), data.get('umi'))
        if data.get('temp') >= 30:
            enviar_alerta_whatsapp(data.get('temp'))
    except Exception as e:
        print(f"⚠️ [PROCESSAMENTO] Erro ao ler dado: {e}")

# --- CONFIGURAÇÃO DO CLIENTE ---
# Usando VERSION1 para manter compatibilidade com seu log anterior
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="Railway_Worker_Thiago")
client.username_pw_set(MQTT_USER, MQTT_PASS)

# SSL para HiveMQ Cloud
context = ssl.create_default_context()
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE
client.tls_set_context(context)

# Atribuição de funções
client.on_connect = on_connect
client.on_message = on_message

print("🚀 [WORKER] Iniciando sistema...")
try:
    client.connect(MQTT_BROKER, 8883, 60)
    # loop_forever mantém o script "vivo" e ouvindo
    client.loop_forever()
except Exception as e:
    print(f"🔴 [FATAL] Erro ao iniciar Worker: {e}")
