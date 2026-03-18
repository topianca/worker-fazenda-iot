import paho.mqtt.client as mqtt
import psycopg2
import json
import requests
import datetime

# Configurações das Credenciais (Suas chaves da Fazenda Piloto)
DB_URL = "postgresql://postgres:wQsuidbkKmMEmLCpNPuCwQCvdsUAdCUl@ballast.proxy.rlwy.net:56019/railway"
MQTT_BROKER = "2f05eafddad44f3d9511e4b2313e07b9.s1.eu.hivemq.cloud"
MQTT_USER = "top1nfo"
MQTT_PASS = "Top1nfo2026"
TOPIC = "top1nfo/fazenda_piloto/sensor_solo"

# --- CONFIGURAÇÕES DE ALERTA (WHATSAPP) ---
# Coloque seu número com código do país (+55) e DDD. Ex: +5531999999999
WHATSAPP_PHONE = "+5512996005169" 
WHATSAPP_APIKEY = "7714077" 

# Memória para não floodar o WhatsApp (1 mensagem por hora)
ultimo_alerta = None

def enviar_alerta_whatsapp(temp):
    global ultimo_alerta
    agora = datetime.datetime.now()
    
    # Manda mensagem se for o 1º alerta ou se passou 1 hora (3600 segundos) do último
    if ultimo_alerta is None or (agora - ultimo_alerta).total_seconds() > 3600:
        try:
            # Mensagem sem acentos para evitar erros na formatação da URL
            msg = f"🚨 *Alerta top1nfo:* Temperatura critica atingiu {temp}C! Risco de dano ao grao."
            url = f"https://api.callmebot.com/whatsapp.php?phone={WHATSAPP_PHONE}&text={msg}&apikey={WHATSAPP_APIKEY}"
            
            resposta = requests.get(url)
            if resposta.status_code == 200:
                print("📱 [WHATSAPP] Alerta enviado para o produtor!")
                ultimo_alerta = agora
            else:
                print(f"❌ [ERRO WHATSAPP] Servidor recusou: {resposta.text}")
        except Exception as e:
            print(f"❌ [ERRO WHATSAPP] Falha ao enviar: {e}")

# Função para salvar no banco e verificar alertas
def salvar_no_banco(temp, umi):
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        query = "INSERT INTO leituras_cafe (temperatura, umidade) VALUES (%s, %s)"
        cursor.execute(query, (temp, umi))
        conn.commit()
        print(f"☕ [DATABASE] Dado salvo na Fazenda: {temp}C | {umi}%")
        
        # --- GATILHO DO ALERTA ---
        if temp >= 30:
            enviar_alerta_whatsapp(temp)
            
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ [ERRO DATABASE] Falha ao gravar no Postgres: {e}")

# Quando o HiveMQ manda mensagem
def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)
        temp = data.get('temp')
        umi = data.get('umi')
        if temp is not None and umi is not None:
            salvar_no_banco(temp, umi)
    except Exception as e:
        pass

# Cliente MQTT
client = mqtt.Client(client_id="Railway_Worker_Thiago", transport="tcp")
client.username_pw_set(MQTT_USER, MQTT_PASS)
client.tls_set()
client.on_message = on_message

print("🚀 [WORKER] Iniciando ponte MQTT -> POSTGRES + WHATSAPP...")
try:
    client.connect(MQTT_BROKER, 8883)
    client.subscribe(TOPIC)
    client.loop_forever()
except Exception as e:
    print(f"🔴 [ERRO CRÍTICO] Falha ao iniciar: {e}")
