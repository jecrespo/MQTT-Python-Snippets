#!/usr/bin/python3
# -*- coding: utf-8 -*-

'''
Script para documentar uso de librería PAHO y los callbacks

Script genérico suscrito a los topics de de una tabla de una BBDD

Se publica en el topic servidor/BBDD/Tabla/Campo
'''

# --------------------------------------------------------------------------- # 
# Conexión con MQTT
# pip install paho-mqtt
# https://www.eclipse.org/paho/clients/python/
# https://pypi.org/project/paho-mqtt/
# --------------------------------------------------------------------------- # 
import paho.mqtt.client as mqtt

broker_address="10.1.1.17"

# --------------------------------------------------------------------------- #
# configure BBDD
# instalar como administrador: pip install mysql-connector-python (https://pypi.org/project/mysql-connector-python/)
# conector: https://dev.mysql.com/downloads/connector/python/8.0.html
# documentación: https://dev.mysql.com/doc/connector-python/en/
# --------------------------------------------------------------------------- #
import mysql.connector as my_dbapi
db_server = "mysql.aprendiendoarduino.com"
db = "database"
tabla = "table"
campo_tiempo = "date"

#Conectar a la BBDD y recoger los datos
conn = my_dbapi.connect(user='usuario', password='password', host=db_server,database=db)
cursor = conn.cursor(dictionary=True) #cursor diccionario
cursor.execute("SELECT * FROM " + tabla)
campos = cursor.column_names

print(campos)

#funciones de callback para MQTT
def on_connect(mqttc, obj, flags, rc):
    print("rc: " + str(rc))

def on_message(mqttc, obj, msg):    #When the client receives messages it generate the on_message callback.
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))

def on_subscribe(mqttc, obj, mid, granted_qos):
    print(" - Subscribed: " + str(mid) + " " + str(granted_qos))

def on_log(mqttc, obj, level, string):
    print(string)

def on_disconnect(mqttc, userdata,rc=0):
    print("Desconectado")
    print("DisConnected result code "+str(rc))

#Conectar en mosquitto
client = mqtt.Client() #create new instance
client.enable_logger()
client.username_pw_set("usuario", "contraseña")

#activo las funciones de callback
client.on_message = on_message
client.on_connect = on_connect
client.on_subscribe = on_subscribe
client.on_disconnect = on_disconnect
# Uncomment to enable debug messages
client.on_log = on_log

try:
    #client.loop_start() #si lo uso en combinación con el loop_forever falla NO USAR con loop_forever
    print("Conectando al broker...")
    client.connect(broker_address) #connect to broker

    print("Suscribiendo a los topics...")
    for campo in campos:
        topic = db_server + "/" + db + "/" + tabla + "/" + campo
        print(topic)
        client.subscribe(topic)

    #ejecuto el loop y empiezan a ejecutarse las funciones de callback
    print("Ejecutando loop para escuchar en topics suscrito, pulsar crt+c para salir")
    client.loop_forever() #The loop_forever call must be placed at the very end of the main script code as it doesn’t progress beyond it.
except:
    print("Has salido del programa")
    client.disconnect() #mando disconect
    client.loop_stop() #stop the loop

print("Script finalizado...")
