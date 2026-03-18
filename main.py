import paho.mqtt.client as mqtt
import psycopg2
import json

# Configurações das Credenciais (Suas chaves da Fazenda Piloto)
DB_URL = "postgresql://postgres:wQsuidbkKmMEmLCpNPuCwQCvdsUAdCUl@ballast.proxy.rlwy.net:56019/railway"
MQTT_BROKER = "2f05eafddad44f3d9511e4b2313e07b9.s1.eu.hivemq.cloud"
MQTT_USER = "top1nfo"
MQTT_PASS = "Top1nfo2026"
TOPIC = "top1nfo/fazenda_piloto/sensor_solo"

# Função para salvar os dados na tabela que você criou
def salvar_no_banco(temp, umi):
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        # Usando a tabela 'leituras_cafe' que está no seu banco
        query = "INSERT INTO leituras_cafe (temperatura, umidade) VALUES (%s, %s)"
        cursor.execute(query, (temp, umi))
        conn.commit()
        print(f"☕ [DATABASE] Dado salvo na Fazenda: {temp}C | {umi}%")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ [ERRO DATABASE] Falha ao gravar no Postgres: {e}")

# Quando o HiveMQ manda uma mensagem, o script executa isto:
def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        print(f"📩 [MQTT] Mensagem recebida: {payload}")
        data = json.loads(payload)
        temp = data.get('temp')
        umi = data.get('umi')
        if temp is not None and umi is not None:
            salvar_no_banco(temp, umi)
    except Exception as e:
        print(f"⚠️ [ERRO PAYLOAD] Falha ao processar JSON: {e}")

# Configuração do Cliente MQTT
client = mqtt.Client(client_id="Railway_Worker_Thiago", transport="tcp")
client.username_pw_set(MQTT_USER, MQTT_PASS)
client.tls_set() # Habilita SSL/TLS para o HiveMQ Cloud

client.on_message = on_message

print("🚀 [WORKER] Iniciando ponte MQTT -> POSTGRES...")
try:
    client.connect(MQTT_BROKER, 8883)
    client.subscribe(TOPIC)
    client.loop_forever()
except Exception as e:
    print(f"🔴 [ERRO CRÍTICO] Falha ao iniciar: {e}")
