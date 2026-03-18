import paho.mqtt.client as mqtt
import psycopg2
import json
import requests
import datetime
import urllib.parse # Importante para formatar a mensagem do WhatsApp

# Configurações das Credenciais
DB_URL = "postgresql://postgres:wQsuidbkKmMEmLCpNPuCwQCvdsUAdCUl@ballast.proxy.rlwy.net:56019/railway"
MQTT_BROKER = "2f05eafddad44f3d9511e4b2313e07b9.s1.eu.hivemq.cloud"
MQTT_USER = "top1nfo"
MQTT_PASS = "Top1nfo2026"
TOPIC = "top1nfo/fazenda_piloto/sensor_solo"

# --- CONFIGURAÇÕES DE ALERTA (WHATSAPP) ---
# O '+' precisa ser substituído por '%2B' para funcionar na URL
WHATSAPP_PHONE = "%2B5512996005169" 
WHATSAPP_APIKEY = "7714077" 

ultimo_alerta = None

def enviar_alerta_whatsapp(temp):
    global ultimo_alerta
    agora = datetime.datetime.now()
    
    if ultimo_alerta is None or (agora - ultimo_alerta).total_seconds() > 3600:
        try:
            # Criamos a mensagem e codificamos para formato de URL (resolve espaços e emojis)
            msg_texto = f"🚨 *Alerta top1nfo:* Temperatura critica atingiu {temp}C! Risco de dano ao grao."
            msg_encoded = urllib.parse.quote(msg_texto)
            
            url = f"https://api.callmebot.com/whatsapp.php?phone={WHATSAPP_PHONE}&text={msg_encoded}&apikey={WHATSAPP_APIKEY}"
            
            resposta = requests.get(url, timeout=10)
            if resposta.status_code == 200:
                print("📱 [WHATSAPP] Alerta enviado com sucesso!")
                ultimo_alerta = agora
            else:
                print(f"❌ [ERRO WHATSAPP] Erro na API: {resposta.status_code}")
        except Exception as e:
            print(f"❌ [ERRO WHATSAPP] Falha na conexao: {e}")

# Função para salvar no banco
def salvar_no_banco(temp, umi):
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        
        # Ajustado para incluir o 'sensor_id' que vimos na sua tabela do Railway
        query = "INSERT INTO leituras_cafe (temperatura, umidade, sensor_id) VALUES (%s, %s, %s)"
        cursor.execute(query, (temp, umi, "ESP32_Fazenda"))
        
        conn.commit()
        print(f"☕ [DATABASE] Gravado: {temp}C | {umi}%")
        
        if temp >= 30:
            enviar_alerta_whatsapp(temp)
            
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ [ERRO DATABASE] Falha no Postgres: {e}")

# Callback de Mensagem
def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)
        temp = data.get('temp')
        umi = data.get('umi')
        if temp is not None and umi is not None:
            salvar_no_banco(temp, umi)
    except Exception as e:
        print(f"⚠️ [JSON ERROR] Payload invalido: {e}")

# Cliente MQTT com SSL (Porta 8883)
client = mqtt.Client(client_id="Railway_Worker_Thiago", transport="tcp")
client.username_pw_set(MQTT_USER, MQTT_PASS)
client.tls_set() # Necessário para o HiveMQ Cloud
client.on_message = on_message

print("🚀 [WORKER] Iniciando ponte MQTT -> DATABASE...")
try:
    client.connect(MQTT_BROKER, 8883)
    client.subscribe(TOPIC)
    client.loop_forever()
except Exception as e:
    print(f"🔴 [ERRO CRÍTICO] Falha ao iniciar: {e}")
