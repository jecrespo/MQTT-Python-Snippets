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
tabla = "tabla"
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
    publicados[mid] = 1 #mensaje publicado

def on_log(mqttc, obj, level, string):
    print(string)

def on_disconnect(mqttc, userdata,rc=0):
    print("Desconectado")
    mqttc.loop_stop()

#publicar en mosquitto
client = mqtt.Client() #create new instance
client.enable_logger()
client.username_pw_set("usuario", "contraseña")

#activo las funciones de callback
client.on_connect = on_connect
client.on_publish = on_publish
client.on_disconnect = on_disconnect
# Uncomment to enable debug messages
client.on_log = on_log

publicados = {} #guardo los mensajes publicados 0 enviado, 1 publicado, 9 error

print("Conectando al broker...")
client.connect(broker_address) #connect to broker

client.loop_start() #comienza el loop y lo paro en el disconnect

for dato in datos:
    topic = db_server + "/" + db + "/" + tabla + "/" + dato
    j = client.publish(topic,str(datos[dato]),qos=0,retain=False)   #publish When you publish a message the publish method returns a tuple (result, mid).
    m_id = j[1] #mid
    if (j[0] == 0): #enviado con exito
        publicados[m_id] = 0
        print(publicados)
    else:
        publicados[m_id] = 9 #error

timeout = time.time() + 5   # 5 seconds from now

while True: #mientras no se han publicado todos no salgo y pongo un timeout
    if all(value == 1 for value in publicados.values()):
        print("todos los mensajes enviados con éxito")
        break
    if time.time() > timeout :
        print("Algunos mensajes no se han enviado")
        print(publicados)
        break

client.disconnect() #mando disconect y paro el loop
time.sleep(2) #espero a la desconexión

print("Script finalizado...")
