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
# DICIONÁRIO AGRONÔMICO — 4 REGRAS DO TERROIR DE PEDRA BONITA
# Para adicionar nova regra: basta incluir novo bloco no dict.
# ============================================================
REGRAS_AGRONOMICAS = {
    "geada": {
        "cooldown": 900,   # 15 min — risco imediato
        "condicao": lambda t, h: t <= 4.0,
        "mensagem": (
            "❄️ *ALERTA GEADA — top1nfo*\n"
            "Temperatura: {temp}°C | Umidade: {umi}%\n\n"
            "📋 *Plano de Ação:*\n"
            "• Mantenha as ruas limpas — o ar frio precisa escoar morro abaixo\n"
            "• Chegue terra no tronco das plantas novas (proteção do colo)\n"
            "• Evite irrigação noturna — a água gelada agrava o dano"
        )
    },
    "phoma": {
        "cooldown": 10800,  # 3 horas
        "condicao": lambda t, h: 10.0 <= t <= 18.0 and h > 85.0,
        "mensagem": (
            "🍄 *ALERTA PHOMA — top1nfo*\n"
            "Temperatura: {temp}°C | Umidade: {umi}%\n"
            "Clima favorável à Phoma (doença de altitude)\n\n"
            "📋 *Plano de Ação:*\n"
            "• Vistorie as brotações novas nas próximas 48h\n"
            "• Lesões escuras nos ramos = aplique cúpricos\n"
            "• Priorize lavouras em encosta com menor circulação de ar"
        )
    },
    "ferrugem": {
        "cooldown": 43200,  # 12 horas
        "condicao": lambda t, h: 18.0 <= t <= 24.0 and h > 90.0,
        "mensagem": (
            "🟠 *ALERTA FERRUGEM TARDIA — top1nfo*\n"
            "Temperatura: {temp}°C | Umidade: {umi}%\n"
            "Clima de estufa pós-chuva — risco elevado na baixada\n\n"
            "📋 *Plano de Ação:*\n"
            "• Realize amostragem foliar em 20 plantas representativas\n"
            "• Se incidência > 5%, aplique fungicida sistêmico\n"
            "• Registre data e produto para rastreabilidade"
        )
    },
    "escaldadura": {
        "cooldown": 1800,   # 30 min
        "condicao": lambda t, h: t >= 32.0,
        "mensagem": (
            "🔥 *ALERTA ESCALDADURA — top1nfo*\n"
            "Temperatura: {temp}°C | Umidade: {umi}%\n"
            "Calor crítico — risco na face oeste (sol da tarde)\n\n"
            "📋 *Plano de Ação:*\n"
            "• NÃO roçar a braquiária nas entrelinhas agora\n"
            "• Cobertura vegetal protege a raiz do calor excessivo\n"
            "• Se houver irrigação, priorize o período da manhã"
        )
    }
}

# Cooldown em memória por regra
# Reseta no reinício — aceitável: melhor alertar demais que silenciar após crash
ultimo_alerta = {nome: 0.0 for nome in REGRAS_AGRONOMICAS}

# ============================================================
# WHATSAPP
# ============================================================
def enviar_alerta_whatsapp(nome_regra, temp, umi):
    """
    Envia alerta com plano de ação agronômico.
    CORREÇÃO: verifica status HTTP da resposta antes de marcar como enviado.
    Sem isso, uma falha da CallMeBot silenciava o alerta por horas.
    """
    regra          = REGRAS_AGRONOMICAS[nome_regra]
    agora          = time.time()
    tempo_restante = regra["cooldown"] - (agora - ultimo_alerta[nome_regra])

    if tempo_restante > 0:
        print(f"⏳ [{nome_regra.upper()}] Cooldown — próximo em {int(tempo_restante)}s")
        return

    try:
        msg = regra["mensagem"].format(temp=round(temp, 1), umi=round(umi, 1))
        url = (
            f"https://api.callmebot.com/whatsapp.php"
            f"?phone={WHATSAPP_PHONE}"
            f"&text={urllib.parse.quote(msg)}"
            f"&apikey={WHATSAPP_APIKEY}"
        )
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            ultimo_alerta[nome_regra] = agora  # Marca como enviado SÓ se sucesso
            print(f"📱 [WHATSAPP] Alerta '{nome_regra}' enviado com sucesso.")
        else:
            print(f"⚠️ [WHATSAPP] CallMeBot retornou HTTP {resp.status_code} — NÃO marcado como enviado, tentará novamente.")
    except requests.exceptions.Timeout:
        print(f"⚠️ [WHATSAPP] Timeout ao enviar '{nome_regra}' — tentará novamente.")
    except Exception as e:
        print(f"❌ [WHATSAPP] Erro inesperado '{nome_regra}': {e}")


def avaliar_e_alertar(temp, umi):
    """Avalia todas as regras e dispara os alertas necessários."""
    for nome_regra, regra in REGRAS_AGRONOMICAS.items():
        if regra["condicao"](temp, umi):
            print(f"⚠️  [RISCO] {nome_regra.upper()} — T:{temp}°C U:{umi}%")
            enviar_alerta_whatsapp(nome_regra, temp, umi)

# ============================================================
# BANCO DE DADOS
# ============================================================
def inicializar_banco():
    """Cria tabelas se não existirem. Seguro chamar múltiplas vezes."""
    try:
        conn   = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS leituras_cafe (
                id          SERIAL PRIMARY KEY,
                data_hora   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                temperatura FLOAT       NOT NULL,
                umidade     FLOAT       NOT NULL,
                sensor_id   TEXT        NOT NULL DEFAULT 'ESP32_Fazenda'
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS controle_sistema (
                chave TEXT PRIMARY KEY,
                valor TEXT NOT NULL
            )
        """)
        cursor.execute("""
            INSERT INTO controle_sistema (chave, valor)
            VALUES ('ultimo_cleanup', '2000-01-01T00:00:00')
            ON CONFLICT (chave) DO NOTHING
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ [DATABASE] Banco inicializado.")
    except Exception as e:
        print(f"❌ [DATABASE INIT] {e}")


def salvar_no_banco(temp, umi, sensor_id="ESP32_Fazenda"):
    """Salva leitura e executa cleanup diário (controle por timestamp no banco)."""
    try:
        conn   = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        cursor.execute(
            "INSERT INTO leituras_cafe (temperatura, umidade, sensor_id) VALUES (%s, %s, %s)",
            (temp, umi, sensor_id)
        )

        cursor.execute("SELECT valor FROM controle_sistema WHERE chave = 'ultimo_cleanup'")
        row = cursor.fetchone()
        # CORREÇÃO: datetime.now(timezone.utc) em vez de utcnow() (depreciado no Python 3.12)
        agora          = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        ultimo_cleanup = datetime.datetime.fromisoformat(row[0]) if row else datetime.datetime(2000, 1, 1)

        if (agora - ultimo_cleanup).total_seconds() >= 86400:
            cursor.execute(
                "DELETE FROM leituras_cafe WHERE data_hora < NOW() - INTERVAL '30 days'"
            )
            deletados = cursor.rowcount
            cursor.execute(
                "UPDATE controle_sistema SET valor = %s WHERE chave = 'ultimo_cleanup'",
                (agora.isoformat(),)
            )
            print(f"🧹 [CLEANUP] {deletados} registros com +30 dias removidos.")

        conn.commit()
        cursor.close()
        conn.close()
        print(f"☕ [DATABASE] {temp}°C | {umi}% | {sensor_id}")
    except Exception as e:
        print(f"❌ [DATABASE ERROR] {e}")

# ============================================================
# CALLBACKS MQTT
# ============================================================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ [MQTT] Conectado ao HiveMQ!")
        client.subscribe(TOPIC)  # Dentro do on_connect = re-inscrição automática
        print(f"📡 [MQTT] Inscrito: {TOPIC}")
    else:
        print(f"❌ [MQTT] Falha de conexão. Código: {rc}")


def on_message(client, userdata, msg):
    print(f"\n📩 [MQTT] Tópico: {msg.topic}")
    try:
        payload = msg.payload.decode()
        print(f"📦 [PAYLOAD] {payload}")
        data    = json.loads(payload)

        # CORREÇÃO CRÍTICA: converte para float explicitamente
        # Sem isso, se o ESP32 enviar "25.5" como string (em vez de 25.5 numérico),
        # a comparação lambda t <= 4.0 lança TypeError e silencia todos os alertas
        try:
            temp = float(data.get("temp"))
            umi  = float(data.get("umi"))
        except (TypeError, ValueError):
            print("⚠️  [PAYLOAD] 'temp' ou 'umi' ausentes ou não numéricos — mensagem ignorada.")
            return

        sensor_id = str(data.get("sensor_id", "ESP32_Fazenda"))

        salvar_no_banco(temp, umi, sensor_id)
        avaliar_e_alertar(temp, umi)

    except json.JSONDecodeError:
        print("⚠️  [PAYLOAD] JSON inválido recebido.")
    except Exception as e:
        print(f"⚠️  [PROCESSAMENTO] Erro inesperado: {e}")


def on_disconnect(client, userdata, rc):
    if rc != 0:
        print(f"🔌 [MQTT] Desconectado inesperadamente (rc={rc}). Aguardando reconexão...")

# ============================================================
# INICIALIZAÇÃO
# ============================================================
inicializar_banco()

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="Railway_Worker_top1nfo")
client.username_pw_set(MQTT_USER, MQTT_PASS)

context                = ssl.create_default_context()
context.check_hostname = False
context.verify_mode    = ssl.CERT_NONE
client.tls_set_context(context)

client.on_connect    = on_connect
client.on_message    = on_message
client.on_disconnect = on_disconnect

print("🚀 [WORKER] Sistema top1nfo iniciando...")
print(f"   Broker  : {MQTT_BROKER}:8883")
print(f"   Tópico  : {TOPIC}")
print(f"   Regras  : {len(REGRAS_AGRONOMICAS)} regras agronômicas ativas")
for nome, r in REGRAS_AGRONOMICAS.items():
    print(f"             {nome} (cooldown: {r['cooldown']//60} min)")

try:
    client.connect(MQTT_BROKER, 8883, keepalive=60)
    client.loop_forever()
except Exception as e:
    print(f"🔴 [FATAL] {e}")
