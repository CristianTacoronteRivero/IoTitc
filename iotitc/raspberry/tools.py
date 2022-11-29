"""
Modulo orientado a la creacion de funciones para ser ejecutadas en Raspberry
"""

import psutil


def status_raspberry():
    """
    :return: Devuelve tres variables donde
    - cpu: representa el % de uso de CPU
    - ram: diccionario que contiene la memoria libre, total y porcentaje ocupada de RAM en MB.
      La estrcutura del diccionario correspondiente a la memoria RAM es la siguiente:
      {avaliable_ram, total_ram, percentage_busy_ram}
    - disk: diccionario que contiene la memoria libre, total y porcentaje ocupada
      en el disco duro en GB. La estrcutura del diccionario correspondiente a la memoria
      del disco duro es la siguiente:
      {avaliable_disk, total_disk, percentage_busy_disk}
    :rtype: float, dict, dict

    >>> from iotitc.raspberry import tools
    >>> cpu, ram, disk = status_raspberry()
    >>> disk["total_disk"]
    183.4
    """
    # porcentaje de uso de cpu
    cpu_percentage = psutil.cpu_percent()

    memory = psutil.virtual_memory()
    # memoria ram disponible
    available_ram = round(memory.available / 1024.0 / 1024.0, 1)
    # memoria ram total
    total_ram = round(memory.total / 1024.0 / 1024.0, 1)
    #  % ocupado
    mem_info = available_ram / total_ram * 100

    local_disk = psutil.disk_usage("/")
    # espacio libre en el disco
    avaliable_disk = round(local_disk.free / 1024.0 / 1024.0 / 1024.0, 1)
    # espacio total del disco
    total_disk = round(local_disk.total / 1024.0 / 1024.0 / 1024.0, 1)
    # % ocupado
    disk_info = avaliable_disk / total_disk * 100

    return (
        cpu_percentage,
        {
            "avaliable_ram": available_ram,
            "total_ram": total_ram,
            "percentage_busy_info": round(mem_info, 2),
        },
        {
            "avaliable_disk": avaliable_disk,
            "total_disk": total_disk,
            "percentage_bussy_disk": round(disk_info, 2),
        },
    )


if __name__ == "__main__":
    cpu, ram, disk = status_raspberry()
    print(disk["total_disk"])
