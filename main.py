import paho.mqtt.client as mqtt
import psycopg2
import json
import requests
import datetime
import urllib.parse
import ssl # Necessário para destravar o SSL

# 1. Configurações de Credenciais
DB_URL = "postgresql://postgres:wQsuidbkKmMEmLCpNPuCwQCvdsUAdCUl@ballast.proxy.rlwy.net:56019/railway"
MQTT_BROKER = "2f05eafddad44f3d9511e4b2313e07b9.s1.eu.hivemq.cloud"
MQTT_USER = "top1nfo"
MQTT_PASS = "Top1nfo2026"
TOPIC = "top1nfo/fazenda_piloto/sensor_solo"

# 2. Configurações de Alerta (WhatsApp)
WHATSAPP_PHONE = "%2B5512996005169" 
WHATSAPP_APIKEY = "7714077" 

ultimo_alerta = None

def enviar_alerta_whatsapp(temp):
    global ultimo_alerta
    agora = datetime.datetime.now()
    
    # Limite de 1 alerta por hora para não gastar API ou incomodar
    if ultimo_alerta is None or (agora - ultimo_alerta).total_seconds() > 3600:
        try:
            msg_texto = f"🚨 *Alerta top1nfo:* Temperatura critica atingiu {temp}C! Risco de dano ao grao."
            msg_encoded = urllib.parse.quote(msg_texto)
            url = f"https://api.callmebot.com/whatsapp.php?phone={WHATSAPP_PHONE}&text={msg_encoded}&apikey={WHATSAPP_APIKEY}"
            
            resposta = requests.get(url, timeout=10)
            if resposta.status_code == 200:
                print("📱 [WHATSAPP] Alerta enviado com sucesso!")
                ultimo_alerta = agora
            else:
                print(f"❌ [WHATSAPP ERROR] Status: {resposta.status_code}")
        except Exception as e:
            print(f"❌ [WHATSAPP ERROR] Falha na conexao: {e}")

def salvar_no_banco(temp, umi):
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        
        # Inserindo dados com o ID do sensor para organizar o banco
        query = "INSERT INTO leituras_cafe (temperatura, umidade, sensor_id) VALUES (%s, %s, %s)"
        cursor.execute(query, (temp, umi, "ESP32_Fazenda"))
        
        conn.commit()
        print(f"☕ [DATABASE] Dado Gravado: {temp}C | {umi}%")
        
        # Dispara alerta se passar de 30 graus
        if temp >= 30:
            enviar_alerta_whatsapp(temp)
            
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ [DATABASE ERROR] Falha no Postgres: {e}")

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

# --- CONFIGURAÇÃO DO CLIENTE MQTT (VERSÃO 2.0 COMPATÍVEL) ---
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="Railway_Worker_Thiago")
client.username_pw_set(MQTT_USER, MQTT_PASS)

# CONFIGURAÇÃO DE SSL "INSECURE" PARA O RAILWAY
# Isso permite conectar ao HiveMQ Cloud sem precisar de arquivos de certificado .pem
context = ssl.create_default_context()
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE
client.tls_set_context(context)

client.on_message = on_message

print("🚀 [WORKER] Iniciando ponte MQTT -> DATABASE...")
try:
    client.connect(MQTT_BROKER, 8883)
    client.subscribe(TOPIC)
    print(f"✅ [WORKER] Conectado e ouvindo o topico: {TOPIC}")
    client.loop_forever()
except Exception as e:
    print(f"🔴 [ERRO CRÍTICO] Falha ao iniciar loop: {e}")
