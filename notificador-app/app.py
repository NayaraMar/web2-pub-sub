import json
from kafka import KafkaConsumer
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import telegram

# Função para enviar notificação via e-mail
def send_email(to_email, file_name, operation):
    from_email = 'detesteemail2@gmail.com'
    password = 'analisesistemas'

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = 'Notificação de Operação Concluída'

    body = f'A imagem {file_name} foi {operation}.'
    msg.attach(MIMEText(body, 'plain'))

    server = smtplib.SMTP('smtp.example.com', 587)
    server.starttls()
    server.login(from_email, password)
    text = msg.as_string()
    server.sendmail(from_email, to_email, text)
    server.quit()

# Função para enviar notificação via Telegram
def send_telegram(telegram_id, file_name, operation):
    bot = telegram.Bot(token='your_telegram_token')
    message = f'O arquivo {file_name} foi {operation}.'
    bot.send_message(chat_id=telegram_id, text=message)

# Consumidor Kafka
def consume_notifications():
    consumer = KafkaConsumer(
        'notificacao',  # Tópico para consumir
        bootstrap_servers='localhost:9092',
        group_id='notif-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    for message in consumer:
        # A mensagem do Kafka será em formato JSON
        notification = message.value
        file_name = notification['file_name']
        operation = notification['operation']
        
        # Aqui você pode escolher como enviar a notificação (e-mail ou Telegram)
        # Exemplo: Se o e-mail for configurado
        send_email('lightsboreale@gmail.com', file_name, operation)
        
        # Ou, caso o Telegram esteja configurado:
        #send_telegram('your_telegram_id', file_name, operation)
