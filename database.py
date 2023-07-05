# -*- coding: utf-8 -*-

"""Database Manager

Este script contiene la clase que administra la conexiÃ³n a la base de datos.
"""

import sys
from logzero import logger
import mysql.connector
from mysql.connector import errorcode


class Database:

    """
    Clase para manejar la conexiÃ³n a la base de datos


    Attributes
    ----------
    config : dict
        configuraciÃ³n de la conexiÃ³n a la base de datos.
    __cnx : MySQLConnection
        conexiÃ³n a la base de datos.
    __cursor : MySQLCursor
        cursor para realizar consultas a la base de datos.

    Methods
    -------
    query(query, params=None)
        Ejecuta una consulta para lectura de datos, la consulta puede no tener parametros.
    insert(query, data)
        Ejecuta una consulta para la inserciÃ³n de nuevos registros.
    last_inserted()
        Devuelve el Ãºltimo id creado por una consulta de inserciÃ³n.
    close()
        Cierra el cursor y la conexiÃ³n a la base de datos.
    """

    def __init__(self, args):
        """
        Parameters
        ----------
        args: list
            lista de strings necesarios para conectarse a la base de datos
        """
        try:
            self.config = {
                'user': args[0],
                'password': args[1],
                'host': args[2],
                'database': args[3]
            }
            self.__cnx = mysql.connector.connect(**self.config)
            self.__cursor = self.__cnx.cursor(buffered=True)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logger.critical(
                    "Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.critical("Database does not exist")
            else:
                logger.critical(err)
            sys.exit()

    def insert(self, query, data):
        """
        Inserta nuevos registros en la base de datos

        Parameters
        ----------
        query: str
            Consulta SQL
        data: tuple
            Registros a ser insertados
        """
        self.__cursor.executemany(query, data)
        self.__cnx.commit()

    def query(self, query, params=None):
        """
        Lectura de informaciÃ³n de la base de datos

        Parameters
        ----------
        query: str
            Consulta SQL
        params: tuple, optional
            Parametros que podrÃ­a tener la consulta SQL

        Returns
        -------
        cursor: MySQLCursor
            Cursor para acceder a los resultados de la consulta
        """
        self.__cursor.execute(query, params)
        return self.__cursor

    def last_inserted(self):
        """
        Devuelve el Ãºltimo id insertado en la base de datos

        Returns
        -------
        row_id: int
            id del Ãºltimo registro insertado en la base de datos
        """
        return self.__cursor.lastrowid

    def close(self):
        """
        Cierra el cursor y la conexiÃ³n a la base de datos
        """
        self.__cursor.close()
        self.__cnx.close()