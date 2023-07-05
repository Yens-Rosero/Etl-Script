from database import Database
from logzero import logger
import sys
import argparse
from datetime import datetime, date, timedelta
import requests, json, time
import base64
import calendar
from zeep import Client
from zeep.transports import Transport
from requests.auth import HTTPBasicAuth
import pandas as pd
import openpyxl
import re


def limpiar(valores):
    return valores.strip("[").strip("]").replace("'", "")


def homologar_valor(valor):
    qs = database.query(f"SELECT valor_final FROM homologacion WHERE valor_inicial = '{valor}'")
    return qs.fetchone()[0]

def convert_date3(date):
    date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    anio = date.year
    dia = date.day
    mes = date.month
    return date.strftime("%Y-%m-%d %H:%M:%S.%f")

def convert_date(date):
    date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    anio = date.year
    dia = date.day
    mes = date.month
    return date.strftime("%Y-%m-%d %H:%M:%S.%f")

def convert_date2(date):
    date = datetime.strptime(date, "%b %d %Y %I:%M%p")
    anio = date.year
    dia = date.day
    mes = date.month
    return date.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def convert_date_epoch(epoch_date):
    # print("epoch", epoch_date)
    datetime_obj=datetime.utcfromtimestamp(epoch_date/1000)
    print("datetime_obj", datetime_obj)
    return datetime_obj.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
def valid_date(date):
    """Convierte un string en un objeto de tipo Datetime

    Parameters
    ----------
    date : str
        String que contiene fecha

    Returns
    -------
    datetime
        Objeto de tipo Datetime
    """
    try:
        parts = date.split(" ")
        if len(parts) == 1:
            date += " 00:00:00"
        else:
            parts = parts[1].split(":")
            for _ in range(3-len(parts)):
                date += ":00"
        return datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(date)
        raise argparse.ArgumentTypeError(msg)
 
if __name__ == '__main__':
    """
    STEP #1.  Leer en tablas la metrica.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--tenant', help='User Tenant', type=str, required=False)
    parser.add_argument(
        '--token', help='Bearer Token for Ibisa authorization', type=str, required=True)
    parser.add_argument(
        '--db', help='MySQL connection info', type=str, required=False)
    parser.add_argument(
        '--start', help='Initial date to query shifts data [yyyy-MM-dd HH:mm:ss]', type=valid_date, required=True)
    parser.add_argument(
        '--end', help='Final date to query shifts data [yyyy-MM-dd HH:mm:ss]', type=valid_date, required=True)


    args = parser.parse_args()
    db = "devteam,Devteam,kuramstock.cpe5k0wndcqw.us-east-2.rds.amazonaws.com,devteam"
    db_conn = db.replace(" ", "").split(",")
    token = args.token
    if len(db_conn) < 4:
        logger.critical("Not enough db parameters")
        sys.exit()
    try:
        database = Database(db_conn)
        sql_metricas = 'SELECT nombre_metrica, codigo_titular, producto, unidad_medida, tipo_variable, serial_link FROM metricas WHERE sincronizar = "SI"'
        metricas_qs = database.query(sql_metricas)
        metricas_ = metricas_qs.fetchall()
        for metricas in metricas_:
            print(f" - Datos: {metricas[0]}, {metricas[1]}, {metricas[2]}, {metricas[3]}, {metricas[4]}, {metricas[5]}")
            start = calendar.timegm(args.start.timetuple())
            print(start*1000)
            end = calendar.timegm(args.end.timetuple())
            print(end*1000)
            inicio = args.start
            fin = args.end

            print (f"FECHA INICIO: {inicio}")
            print (f"FECHA FIN: {fin}")
            end
            
            finProcess = fin
            delta = timedelta(hours=1)

            # Crear un nuevo libro de Excel
            libro_excel = openpyxl.Workbook()
            hoja_activa = libro_excel.active

            # Agregar encabezados de columna a la hoja de Excel
            encabezados = ["Link", "Punto de Control", "Producto", "Unidad de Medida", "Fecha", "Valor", "Tipo de Variable", "Tipo de Material"]
            hoja_activa.append(encabezados)

            while inicio <= finProcess:

                fin = inicio + delta 
                start = calendar.timegm(inicio.timetuple())
                print(start*1000)
                end = calendar.timegm(fin.timetuple())
                print(end*1000)
                
                print(f"FECHA INICIO REAL: {inicio} - {start}")
                print(f"FECHA FIN    REAL: {fin} - {end}")
            
                """
                STEP #2. Leer de Space la metrica
                """
                values = []
                query = {
                    "tenant": "anm",
                    "metrics": [{"tags": {},"name": metricas[0]}],
                    "plugins": [],
                    "cache_time": 0,
                    "start_absolute": start*1000,
                    "end_absolute": end*1000
                }
                query_str =  json.dumps(query)
                print("QUERY ----> " + query_str);
                message_bytes = query_str.encode('utf-8')
                encodedQuery = str(base64.b64encode(message_bytes).decode('utf-8'))
                # print("encodedQuery: ", encodedQuery)
                url = f"https://thingsback.ibisagroup.com/api/v1/metrics?query={encodedQuery}"
                print("URL: ", url)
                req = requests.get(url,headers={"Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ik1FVTNSRVk1Tmprek9UVTFRVVl3T1VJd01rTkZNVFkyTkRReE1UUXpOekZDTXpZMFEwUkVSQSJ9.eyJodHRwczovL2NsYWltcy5pYmlzYS5jby9ncm91cCI6WyJwb3dlcnVzZXIiXSwiaHR0cHM6Ly9jbGFpbXMuaWJpc2EuY28vdGVuYW50IjpbImFubSJdLCJodHRwczovL2NsYWltcy5pYmlzYS5jby90ZW5hbnRBZG1pbiI6dHJ1ZSwiaXNzIjoiaHR0cHM6Ly9pYmlzYS5hdXRoMC5jb20vIiwic3ViIjoiYXV0aDB8NjA3ZGNlMGNjMjgyZmMwMDZhYmIxYjQyIiwiYXVkIjpbImh0dHBzOi8vaWJpc2EuY28vYXBpIiwiaHR0cHM6Ly9pYmlzYS5hdXRoMC5jb20vdXNlcmluZm8iXSwiaWF0IjoxNjg3NTU5NjI4LCJleHAiOjE2OTAxNTE2MjgsImF6cCI6IkQxT241YmF5aWhnRmxIQXZQN1FZclFqc0kzMHNvVHlzIiwic2NvcGUiOiJvcGVuaWQgcHJvZmlsZSBlbWFpbCJ9.UiZYw9v3g3iD3Gav6xcs1dTFpuWhDdY9xDrfrkXCs-tHPaGCucJ7MTy0QWmieREkpXTOYgjfCKz3vaZnQ_zLZRW09kk8DjpJm6yYoYp8SQ5Uz-UnGSmlPsPLTh8xILIKpLoSOtIhTQekhrW6f672EvHPUED5t7mUK5evmBS4HObxIDggUqsPRJSebHR9QQ913JKCIUZlZ_x8IJCEX8nTj5j44dLw9PWLuthVItT04jXPp07mI-urBIpUQtWFi_mITnu-fQUJSzFyEqxjRKTItAtNBoEQK8BCf-uwFlGsNSDvP8Py65bBtfw85nkAI6QXAHVChWRLlJz0_uHjkQdMaQ"}, verify = False)
                metricas_resultados = req.json()
                print(req.status_code , "Respuesta de Space")
                print("metricas_resultados", metricas_resultados)
                if req.status_code < 400:
                    valores_finales = []
                    sample = metricas_resultados["queries"][0]["sample_size"];
                    if sample:
                        print("NO ENTRA", metricas_resultados)
                        mr = metricas_resultados["queries"][0]["results"][0]["values"]
                        # print("mr ", mr)
                        arr_variables_link = []
                        arr_variables_punto_control = []
                        arr_variables_producto = []
                        arr_variables_unidad_medida = []
                        arr_variables_fecha = []
                        arr_variables_valor = []
                        arr_variables_tipo_material = []
                        arr_variables_tipo_variable = []
                        for idx, r in enumerate(mr, start=1):
                            print(" ")
                            print("----------------------------------------------------")
                            print(f"processing {idx}/{len(mr)}")
                            print("----------------------------------------------------")
                            if idx == 200:
                                break
                            # print("-----------------r", r)
                            if "albania" in metricas[0]:
                                # print(r)
                                """
                                STEP #3. Enviar al servicio de ANM
                                """
                                arr_variables_link.append(metricas[5])
                                consolidado = r[1].split("|");
                                # print("consolidado", consolidado)
                                arr_variables_punto_control.append(metricas[1]);
                                print("PUNTO CONTROL: ", metricas[1])
                                
                                arr_variables_producto.append(homologar_valor(consolidado[3]))
                                print("PRODUCTO: ", consolidado[3])
                                print("PRODUCTO HOMOLOGADO: ", homologar_valor(consolidado[3]))
                                
                                arr_variables_unidad_medida.append(metricas[3])
                                print("UNIDAD MEDIDA: ", metricas[3])
                                
                                arr_variables_fecha.append(convert_date(consolidado[4])[:-3])
                                print("FECHA: ", consolidado[4])
                                print("FECHA HOMOLOGADA: ", convert_date(consolidado[4])[:-3])
                                    
                                arr_variables_valor.append(consolidado[1])
                                print("VALOR: ", consolidado[1])
                                
                                arr_variables_tipo_variable.append(consolidado[5])
                                print("TIPO VARIABLE: ", consolidado[5])
                                
                                arr_variables_tipo_material.append("PESO")
                            
                            if "triar" in metricas[0]:
                                arr_variables_link.append(metricas[5])
                                consolidado = r[1].split("|");
                                # print("consolidado", consolidado)
                                arr_variables_punto_control.append(metricas[1]);
                                print("PUNTO CONTROL: ", metricas[1])
                                
                                producto = consolidado[3].replace("\\", "").replace("\"", "")
                                producto = homologar_valor(producto) if producto != "ARENA" else producto

                                arr_variables_producto.append(producto)
                                print("PRODUCTO: ", producto)
                                
                                arr_variables_unidad_medida.append(metricas[3])
                                print("UNIDAD MEDIDA: ", metricas[3])
                                
                                arr_variables_fecha.append(convert_date(consolidado[4])[:-3])
                                print("FECHA: ", consolidado[4])
                                print("FECHA HOMOLOGADA: ", convert_date(consolidado[4])[:-3])
                                    
                                arr_variables_valor.append(consolidado[1])
                                print("VALOR: ", consolidado[1])
                                
                                arr_variables_tipo_variable.append(consolidado[6])
                                print("TIPO VARIABLE: ", consolidado[6])
                                
                                arr_variables_tipo_material.append("PESO")
                                
                            if "hatillo" in metricas[0]:
                                print("---- R: ", r[0])
                                arr_variables_link.append(metricas[5])
                                # print("consolidado", consolidado)
                                arr_variables_punto_control.append(metricas[1]);
                                print("PUNTO CONTROL: ", metricas[1])

                                arr_variables_producto.append(metricas[2])
                                print("PRODUCTO: ", metricas[2])
                                
                                arr_variables_unidad_medida.append(metricas[3])
                                print("UNIDAD MEDIDA: ", metricas[3])
                                
                                arr_variables_fecha.append(convert_date_epoch(r[0]))
                                print("FECHA: ", r[0])
                                print("FECHA HOMOLOGADA: ", convert_date_epoch(r[0]))
                                    
                                arr_variables_valor.append(r[1])
                                print("VALOR: ", r[1])
                                
                                arr_variables_tipo_variable.append(metricas[4])
                                print("TIPO VARIABLE: ", metricas[4])
                                
                                arr_variables_tipo_material.append("PESO")
                            
                            if "francia" in metricas[0]:
                                print("---- R: ", r[0])
                                arr_variables_link.append(metricas[5])
                                arr_variables_punto_control.append(metricas[1]);
                                print("PUNTO CONTROL: ", metricas[1])

                                arr_variables_producto.append(metricas[2])
                                print("PRODUCTO: ", metricas[2])
                                
                                arr_variables_unidad_medida.append(metricas[3])
                                print("UNIDAD MEDIDA: ", metricas[3])
                                
                                arr_variables_fecha.append(convert_date_epoch(r[0]))
                                print("FECHA: ", r[0])
                                print("FECHA HOMOLOGADA: ", convert_date_epoch(r[0]))
                                    
                                arr_variables_valor.append(r[1])
                                print("VALOR: ", r[1])
                                
                                arr_variables_tipo_variable.append(metricas[4])
                                print("TIPO VARIABLE: ", metricas[4])
                                
                                arr_variables_tipo_material.append("PESO")
                                
                            if "3100" in metricas[0]:
                                print("---- R: ", r[0])
                                arr_variables_link.append(metricas[5])
                                arr_variables_punto_control.append(metricas[1]);
                                print("PUNTO CONTROL: ", metricas[1])

                                arr_variables_producto.append(metricas[2])
                                print("PRODUCTO: ", metricas[2])
                                
                                arr_variables_unidad_medida.append(metricas[3])
                                print("UNIDAD MEDIDA: ", metricas[3])
                                
                                arr_variables_fecha.append(convert_date_epoch(r[0]))
                                print("FECHA: ", r[0])
                                print("FECHA HOMOLOGADA: ", convert_date_epoch(r[0]))
                                    
                                arr_variables_valor.append(r[1])
                                print("VALOR: ", r[1])
                                
                                arr_variables_tipo_variable.append(metricas[4])
                                print("TIPO VARIABLE: ", metricas[4])
                                
                                arr_variables_tipo_material.append("PESO")
                                
                            if "3200" in metricas[0]:
                                print("---- R: ", r[0])
                                arr_variables_link.append(metricas[5])
                                arr_variables_punto_control.append(metricas[1]);
                                print("PUNTO CONTROL: ", metricas[1])

                                arr_variables_producto.append(metricas[2])
                                print("PRODUCTO: ", metricas[2])
                                
                                arr_variables_unidad_medida.append(metricas[3])
                                print("UNIDAD MEDIDA: ", metricas[3])
                                
                                arr_variables_fecha.append(convert_date_epoch(r[0]))
                                print("FECHA: ", r[0])
                                print("FECHA HOMOLOGADA: ", convert_date_epoch(r[0]))
                                    
                                arr_variables_valor.append(r[1])
                                print("VALOR: ", r[1])
                                
                                arr_variables_tipo_variable.append(metricas[4])
                                print("TIPO VARIABLE: ", metricas[4])
                                
                                arr_variables_tipo_material.append("PESO")

                            if "corame" in metricas[0]:
                                arr_variables_link.append(metricas[5])
                                consolidado = r[1].split("|");
                                print("consolidado", consolidado)
                                arr_variables_punto_control.append(metricas[1]);
                                print("PUNTO CONTROL: ", metricas[1])
                                
                                producto = consolidado[3].replace("\\", "").replace("\"", "").strip()
                                producto = homologar_valor(producto) if producto != "ARENA" else producto

                                arr_variables_producto.append(producto)
                                print("PRODUCTO: ", producto)
                                
                                arr_variables_unidad_medida.append(metricas[3])
                                print("UNIDAD MEDIDA: ", metricas[3])
                                
                                arr_variables_fecha.append(convert_date(consolidado[4])[:-3])
                                print("FECHA: ", consolidado[4])
                                print("FECHA HOMOLOGADA: ", convert_date(consolidado[4])[:-3])
                                    
                                arr_variables_valor.append(consolidado[1])
                                print("VALOR: ", consolidado[1])
                                
                                arr_variables_tipo_variable.append(consolidado[6])
                                print("TIPO VARIABLE: ", consolidado[6])
                                
                                arr_variables_tipo_material.append("PESO")

                            if "bc408" in metricas[0]:
                                print("---- R: ", r[0])
                                arr_variables_link.append(metricas[5])
                                arr_variables_punto_control.append(metricas[1]);
                                print("PUNTO CONTROL: ", metricas[1])

                                arr_variables_producto.append(metricas[2])
                                print("PRODUCTO: ", metricas[2])
                                
                                arr_variables_unidad_medida.append(metricas[3])
                                print("UNIDAD MEDIDA: ", metricas[3])
                                
                                arr_variables_fecha.append(convert_date_epoch(r[0]))
                                print("FECHA: ", r[0])
                                print("FECHA HOMOLOGADA: ", convert_date_epoch(r[0]))
                                    
                                arr_variables_valor.append(r[1])
                                print("VALOR: ", r[1])
                                
                                arr_variables_tipo_variable.append(metricas[4])
                                print("TIPO VARIABLE: ", metricas[4])
                                
                                arr_variables_tipo_material.append("PESO")
                                
                            if "bc508" in metricas[0]:
                                print("---- R: ", r[0])
                                arr_variables_link.append(metricas[5])
                                arr_variables_punto_control.append(metricas[1]);
                                print("PUNTO CONTROL: ", metricas[1])

                                arr_variables_producto.append(metricas[2])
                                print("PRODUCTO: ", metricas[2])
                                
                                arr_variables_unidad_medida.append(metricas[3])
                                print("UNIDAD MEDIDA: ", metricas[3])
                                
                                arr_variables_fecha.append(convert_date_epoch(r[0]))
                                print("FECHA: ", r[0])
                                print("FECHA HOMOLOGADA: ", convert_date_epoch(r[0]))
                                    
                                arr_variables_valor.append(r[1])
                                print("VALOR: ", r[1])
                                
                                arr_variables_tipo_variable.append(metricas[4])
                                print("TIPO VARIABLE: ", metricas[4])
                                
                                arr_variables_tipo_material.append("PESO")
                                
                            if "testing2" in metricas[0]:
                                # print(r)
                                arr_variables_link.append(metricas[5])
                                consolidado = r[1].split("|");
                                print("consolidado", consolidado)
                                arr_variables_punto_control.append(metricas[1]);
                                print("PUNTO CONTROL: ", metricas[1])
                                
                                arr_variables_producto.append(metricas[2])
                                print("PRODUCTO: ", metricas[2])
                                
                                arr_variables_unidad_medida.append(metricas[3])
                                print("UNIDAD MEDIDA: ", metricas[3])
                                
                                arr_variables_fecha.append(convert_date2(consolidado[2]))
                                print("FECHA: ", consolidado[2])
                                print("FECHA HOMOLOGADA: ", convert_date2(consolidado[2]))
                                    
                                arr_variables_valor.append(consolidado[3])
                                print("VALOR: ", consolidado[3])
                                
                                arr_variables_tipo_variable.append(consolidado[0])
                                print("TIPO VARIABLE: ", consolidado[0])
                                
                                arr_variables_tipo_material.append("PESO")
                                
                            if "testing3" in metricas[0]:
                                # print(r)
                                arr_variables_link.append(metricas[5])
                                consolidado = r[1].split("|");
                                print("consolidado", consolidado)
                                arr_variables_punto_control.append(metricas[1]);
                                print("PUNTO CONTROL: ", metricas[1])
                                
                                arr_variables_producto.append(metricas[2])
                                print("PRODUCTO: ", metricas[2])
                                
                                arr_variables_unidad_medida.append(metricas[3])
                                print("UNIDAD MEDIDA: ", metricas[3])
                                
                                arr_variables_fecha.append(convert_date2(consolidado[2]))
                                print("FECHA: ", consolidado[2])
                                print("FECHA HOMOLOGADA: ", convert_date2(consolidado[2]))
                                    
                                arr_variables_valor.append(consolidado[3])
                                print("VALOR: ", consolidado[3])
                                
                                arr_variables_tipo_variable.append(consolidado[0])
                                print("TIPO VARIABLE: ", consolidado[0])
                                
                                arr_variables_tipo_material.append("PESO")
                                
                            if "testing4" in metricas[0]:
                                # print(r)
                                arr_variables_link.append(metricas[5])
                                consolidado = r[1].split("|");
                                print("consolidado", consolidado)
                                arr_variables_punto_control.append(metricas[1]);
                                print("PUNTO CONTROL: ", metricas[1])
                                
                                arr_variables_producto.append(metricas[2])
                                print("PRODUCTO: ", metricas[2])
                                
                                arr_variables_unidad_medida.append(metricas[3])
                                print("UNIDAD MEDIDA: ", metricas[3])
                                
                                arr_variables_fecha.append(convert_date2(consolidado[2]))
                                print("FECHA: ", consolidado[2])
                                print("FECHA HOMOLOGADA: ", convert_date2(consolidado[2]))
                                    
                                arr_variables_valor.append(consolidado[3])
                                print("VALOR: ", consolidado[3])
                                
                                arr_variables_tipo_variable.append(consolidado[0])
                                print("TIPO VARIABLE: ", consolidado[0])
                                
                                arr_variables_tipo_material.append("PESO")


                        # Enviar a servicio

                        cantidad = len(arr_variables_link)
                        print("Total de valores a enviar: ", cantidad +1)
                        cantidad_iteraciones = cantidad / 100

                        aux_init= 0
                        aux_fin = 99
                        
                        
                        valores_arreglo = lambda arreglo: arreglo[aux_init:cantidad]  


                        # Recorrer los datos y agregar cada fila a la hoja de Excel
                        for idx, v in enumerate(valores_arreglo(arr_variables_link)):
                            fila = [
                                v,
                                arr_variables_punto_control[idx],
                                arr_variables_producto[idx],
                                arr_variables_unidad_medida[idx],
                                arr_variables_fecha[idx],
                                arr_variables_valor[idx],
                                arr_variables_tipo_variable[idx],
                                arr_variables_tipo_material[idx]
                            ]
                            hoja_activa.append(fila)

                            print(f"Iteracion desde la {aux_init} hasta maximo {aux_fin}")
                            print(" ")
                            print("----------------------------------------------------")
                            print(f"sending {idx+1}/{cantidad} de la iteracion")
                            print("----------------------------------------------------")
                            print(fila)
                            print(" ")

                        # Guardar el libro de Excel
                    nombre_archivo = "cnrFrancia03.04_07.05_23.xlsx"
                    libro_excel.save(nombre_archivo)

                       # Cerrar el libro de Excel
                    libro_excel.close()

                inicio += delta
    except Exception as e:
        print(e)
        database.close()
    
    """
    STEP #4. Actualiza Metrica en Space con fecha de NOW
    """
    # anm.sync.lastexecution
    
    
    """
    STEP #5. Actualiza campo de sincronizacion en Tablas. Opcional
    """
