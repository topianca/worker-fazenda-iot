import paho.mqtt.client as mqtt
import psycopg2
import json
import requests
import urllib.parse
import ssl
import time
import datetime

# ============================================================
# CONFIGURAÇÕES
# ============================================================
DB_URL      = "postgresql://postgres:wQsuidbkKmMEmLCpNPuCwQCvdsUAdCUl@ballast.proxy.rlwy.net:56019/railway"
MQTT_BROKER = "2f05eafddad44f3d9511e4b2313e07b9.s1.eu.hivemq.cloud"
MQTT_USER   = "top1nfo"
MQTT_PASS   = "Top1nfo2026"
TOPIC       = "top1nfo/fazenda_piloto/sensor_solo"

WHATSAPP_PHONE  = "%2B5512996005169"
WHATSAPP_APIKEY = "7714077"

# ============================================================
# DICIONÁRIO AGRONÔMICO (MANTIDO INTACTO)
# ============================================================
REGRAS_AGRONOMICAS = {
    "geada": {
        "cooldown": 900,   
        "condicao": lambda t, h: t <= 4.0,
        "mensagem": "❄️ *ALERTA GEADA — top1nfo*\nTemperatura: {temp}°C | Umidade: {umi}%\n\n📋 *Plano de Ação:*\n• Mantenha as ruas limpas para o ar frio escoar\n• Chegue terra no tronco das plantas novas"
    },
    "phoma": {
        "cooldown": 10800,  
        "condicao": lambda t, h: 10.0 <= t <= 18.0 and h > 85.0,
        "mensagem": "🍄 *ALERTA PHOMA — top1nfo*\nTemperatura: {temp}°C | Umidade: {umi}%\n\n📋 *Plano de Ação:*\n• Vistorie brotações nas próximas 48h\n• Lesões escuras = aplique cúpricos"
    },
    "ferrugem": {
        "cooldown": 43200,  
        "condicao": lambda t, h: 18.0 <= t <= 24.0 and h > 90.0,
        "mensagem": "🟠 *ALERTA FERRUGEM TARDIA — top1nfo*\nTemperatura: {temp}°C | Umidade: {umi}%\n\n📋 *Plano de Ação:*\n• Amostragem foliar em 20 plantas\n• Se incidência > 5%, aplique fungicida sistêmico"
    },
    "escaldadura": {
        "cooldown": 1800,   
        "condicao": lambda t, h: t >= 32.0,
        "mensagem": "🔥 *ALERTA ESCALDADURA — top1nfo*\nTemperatura: {temp}°C\n\n📋 *Plano de Ação:*\n• NÃO roçar a braquiária nas entrelinhas agora\n• Cobertura vegetal protege a raiz"
    }
}

ultimo_alerta = {nome: 0.0 for nome in REGRAS_AGRONOMICAS}

# ============================================================
# WHATSAPP
# ============================================================
def enviar_alerta_whatsapp(nome_regra, temp, umi):
    regra = REGRAS_AGRONOMICAS[nome_regra]
    agora = time.time()
    
    if (agora - ultimo_alerta[nome_regra]) < regra["cooldown"]:
        return

    try:
        msg = regra["mensagem"].format(temp=round(temp, 1), umi=round(umi, 1))
        url = f"https://api.callmebot.com/whatsapp.php?phone={WHATSAPP_PHONE}&text={urllib.parse.quote(msg)}&apikey={WHATSAPP_APIKEY}"
        resp = requests.get(url, timeout=10)
        
        if resp.status_code == 200:
            ultimo_alerta[nome_regra] = agora
            print(f"📱 [WHATSAPP] Alerta '{nome_regra}' disparado!")
    except Exception as e:
        print(f"❌ [WHATSAPP] Erro: {e}")

def avaliar_e_alertar(temp, umi):
    for nome_regra, regra in REGRAS_AGRONOMICAS.items():
        if regra["condicao"](temp, umi):
            enviar_alerta_whatsapp(nome_regra, temp, umi)

# ============================================================
# BANCO DE DADOS BLINDADO
# ============================================================
def inicializar_banco():
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        
        # Cria a tabela base
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS leituras_cafe (
                id SERIAL PRIMARY KEY,
                data_hora TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                temperatura FLOAT NOT NULL,
                umidade FLOAT NOT NULL
            )
        """)
        
        # AUTO-CURA: Adiciona a coluna sensor_id caso seja uma tabela antiga
        cursor.execute("ALTER TABLE leituras_cafe ADD COLUMN IF NOT EXISTS sensor_id TEXT DEFAULT 'ESP32_Fazenda'")
        
        cursor.execute("CREATE TABLE IF NOT EXISTS controle_sistema (chave TEXT PRIMARY KEY, valor TEXT NOT NULL)")
        cursor.execute("INSERT INTO controle_sistema (chave, valor) VALUES ('ultimo_cleanup', '2000-01-01T00:00:00') ON CONFLICT DO NOTHING")
        
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ [DATABASE] Estrutura verificada e atualizada.")
    except Exception as e:
        print(f"❌ [DATABASE INIT ERRO] {e}")

def salvar_no_banco(temp, umi, sensor_id="ESP32_Fazenda"):
    # BLOCO 1: Salva o dado (Isolado)
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO leituras_cafe (temperatura, umidade, sensor_id) VALUES (%s, %s, %s)", (temp, umi, sensor_id))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"☕ [DATABASE] Salvo: {temp}°C | {umi}%")
    except Exception as e:
        print(f"❌ [DATABASE INSERT ERROR] {e}")
        return # Se falhar aqui, não tenta limpar o banco

    # BLOCO 2: Cleanup de 30 dias (Isolado)
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        cursor.execute("SELECT valor FROM controle_sistema WHERE chave = 'ultimo_cleanup'")
        row = cursor.fetchone()
        
        agora = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        ultimo_cleanup = datetime.datetime.fromisoformat(row[0]) if row else datetime.datetime(2000, 1, 1)

        if (agora - ultimo_cleanup).total_seconds() >= 86400: # 1 vez por dia
            cursor.execute("DELETE FROM leituras_cafe WHERE data_hora < NOW() - INTERVAL '30 days'")
            deletados = cursor.rowcount
            cursor.execute("UPDATE controle_sistema SET valor = %s WHERE chave = 'ultimo_cleanup'", (agora.isoformat(),))
            conn.commit()
            print(f"🧹 [CLEANUP] {deletados} registros antigos apagados.")
            
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ [CLEANUP ERROR] Ignorado: {e}")

# ============================================================
# CALLBACKS MQTT
# ============================================================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ [MQTT] Conectado! Ouvindo HiveMQ...")
        client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        temp = float(data.get("temp"))
        umi  = float(data.get("umi"))
        sensor_id = str(data.get("sensor_id", "ESP32_Fazenda"))
        
        salvar_no_banco(temp, umi, sensor_id)
        avaliar_e_alertar(temp, umi)
    except Exception as e:
        print(f"⚠️ [PAYLOAD ERRO] Dados inválidos: {e}")

inicializar_banco()

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="Worker_Fazenda_Top1nfo")
client.username_pw_set(MQTT_USER, MQTT_PASS)

context = ssl.create_default_context()
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE
client.tls_set_context(context)

client.on_connect = on_connect
client.on_message = on_message

print("🚀 [WORKER] Iniciando ponte MQTT -> Postgres...")
client.connect(MQTT_BROKER, 8883, keepalive=60)
client.loop_forever()
