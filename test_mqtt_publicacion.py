#!/usr/bin/python3
# -*- coding: utf-8 -*-

'''
Script para documentar uso de librería PAHO y los callbacks

Script genérico para coger la ultima serie temporal insertada en una tabla de una BBDD MySQL y mandar broker MQTT

Se publica en el topic servidor/BBDD/Tabla/Campo
'''
import time

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
db_server = "aprendiendoarduino.com"
db = "database"
tabla = "table"
campo_tiempo = "date"

#Conectar a la BBDD y recoger los datos

conn = my_dbapi.connect(user='usuario', password='contraseña', host=db_server,database=db)
cursor = conn.cursor(dictionary=True) #cursor diccionario
query = "SELECT * FROM " + tabla + " ORDER BY " + campo_tiempo + " DESC LIMIT 1"
cursor.execute(query)
row = cursor.fetchall() #row ya tiene un formato de diccionario
datos = row[0]

print(datos)

#funciones de callback para MQTT
def on_connect(mqttc, obj, flags, rc):
    print("rc: " + str(rc))

def on_publish(mqttc, obj, mid):
    print("mid: " + str(mid))

def on_log(mqttc, obj, level, string):
    print(string)

def on_disconnect(mqttc, userdata,rc=0):
    print("Desconectado")
    mqttc.loop_stop()

#publicar en mosquitto
client = mqtt.Client() #create new instance
client.enable_logger()
client.username_pw_set("ususario", "contraseña")

#activo las funciones de callback
client.on_connect = on_connect
client.on_publish = on_publish
client.on_disconnect = on_disconnect
# Uncomment to enable debug messages
client.on_log = on_log

client.loop_start() #comienza el loop y lo paro en el disconnect

print("Conectando al broker...")
client.connect(broker_address) #connect to broker
time.sleep(2)   #espero a conectarse

for dato in datos:
    topic = db_server + "/" + db + "/" + tabla + "/" + dato
    client.publish(topic,str(datos[dato]),qos=0,retain=False)   #publish When you publish a message the publish method returns a tuple (result, mid).
    time.sleep(0.5) #espero entre publicación
    
time.sleep(5) #espero a que se publiquen los datos

client.disconnect() #mando disconect y paro el loop
time.sleep(2) #espero a la desconexión

print("Script finalizado...")
