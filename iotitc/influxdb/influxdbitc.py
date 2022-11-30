"""
Modulo orientado a la realizacion de operaciones en InfluxDB
"""

from typing import Any
import pandas as pd
from influxdb import DataFrameClient
import influxdb


class ToolInflux:
    def __init__(
        self,
        ip_address: str,
        database: str,
        port: int = 8086,
        user: str = "root",
        password: str = "root",
    ) -> influxdb.dataframe_client.DataFrameClient:
        """Devuelve un objeto el cual ha establecido conexion con una base de datos
        que se encuentra en InfluxDB

        :param ip_address: IP de la base de datos
        :type ip_address: str
        :param database: Nombre de la base de datos
        :type database: str
        :param port: Puerto de conexion con la base de datos, por defecto es 8086
        :type port: int, optcional
        :param user: Nombre de usuario de la base de datos, por defecto es "root"
        :type user: str, opcional
        :param password: Contraseña de la base de datos, por defecto es "root"
        :type password: str, opcional
        :return: Conexion con la base de datos
        :rtype: influxdb.dataframe_client.DataFrameClient
        """
        self.ip_address = ip_address
        self.port = port
        self.database = database
        self.user = user
        self.password = password

        self.client = self.connect_to_table()

    def connect_to_table(self) -> influxdb.dataframe_client.DataFrameClient:
        """Devuelve la conexion establecida con una tabla perteneciente a la base de datos
        especificada al iniciar la clase

        :return: Conexion con la tabla de una base de datos
        :rtype: influxdb.dataframe_client.DataFrameClient
        """
        client = DataFrameClient(
            self.ip_address, self.port, self.user, self.password, self.database
        )
        return client

    def get_table(
        self,
        measure: str = None,
        table: str = None,
        window_time: str = None,
        group_by: str = None,
        query: bool | str = False,
    ) -> pd.DataFrame:
        """Devuelve un DataFrame según la query especificada

        :param measure: , defaults to None
        :type measure: str, optional
        :param table: _description_, defaults to None
        :type table: str, optional
        :param window_time: _description_, defaults to None
        :type window_time: str, optional
        :param group_by: _description_, defaults to None
        :type group_by: str, optional
        :param query: _description_, defaults to False
        :type query: bool | str, optional
        :return: _description_
        :rtype: pd.DataFrame
        """
        if isinstance(query, bool):
            if measure == "*":
                query_string = f'SELECT mean({measure}) FROM "{table}" WHERE time >= now() - {window_time} and time <= now() GROUP BY time({group_by}) fill(null)'
            else:
                query_string = f'SELECT mean("{measure}") FROM "{table}" WHERE time >= now() - {window_time} and time <= now() GROUP BY time({group_by}) fill(null)'
        else:
            query_string = query

        return self.client.query(query_string)[table]


if __name__ == "__main__":
    shelly_test = ToolInflux("10.141.188.140", "shelly_test")
    df = shelly_test.read_table(
        measure="*", table="data_shelly", window_time="2d", group_by="1m"
    )
    print(df)
