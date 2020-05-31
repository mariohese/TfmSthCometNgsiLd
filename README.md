# TfmSthCometNgsiLd

# THADS (Time Historic Aggregated Data System)

Este proyecto es el resultado del Trabajo Fin de Máster "Diseño e implementación de un sistema de gestión de información de contexto para el estándar NGSI-LD".

Este componente realiza una conexión a la imagen de Docker de Orion-LD para implementar las funciones de los componentes STH-Comet y Draco de FIWARE.

Para ejecutar este desarrollo hay que incluir por línea de argumentos un parámetro denominado --persistence, que puede adquirir los siguientes valores:
- --persistence mysql.
- --persistence postgres.
- --persistence hdfs
