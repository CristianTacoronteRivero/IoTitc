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
        measure: str = "*",
        table: str = None,
        window_time: str = "24h",
        group_by: str = "2m",
        query: bool | str = False,
    ) -> pd.DataFrame:
        """Devuelve un DataFrame según la query especificada

        :param measure: Medidas de las tabla instanciada, por defecto "*".
                        Nota: solo admite por ahora una medida o "*"
        :type measure: str, opcional
        :param table: Nombre de la tabla, por defecto None
        :type table: str, opcional
        :param window_time: Periodo de tiempo que se desea seleccionar, por defecto 24h.
                            - s: segundos
                            - m: minutos
                            - h: horas
                            - w: semana
                            - M: mes
                            - y: Año
        :type window_time: str, opcional
        :param group_by: Configura la agrupación de los datos, por defecto 2m.
                            - s: segundos
                            - m: minutos
                            - h: horas
                            - w: semana
                            - M: mes
                            - y: Año
        :type group_by: str, opcional
        :param query: Si no se especifica una query se aplica la selección que
                    se encuentra por defecto. Si se quiere especificar una query
                    se debe de introducir la consulta formato cadena; por defecto False
        :type query: bool | str, opcional
        :return: DataFrame que contiene los datos y las medidas
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
    df = shelly_test.get_table(
        measure=["temperature"], table="data_shelly", window_time="2d", group_by="1m"
    )
    print(df)
