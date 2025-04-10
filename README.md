# Data Ingestion Tool

Una solución metadata-driven para la ingesta, validación y transformación de datos utilizando Flask y PySpark.

## Índice

- [Introducción](#introducción)
- [Características](#características)
- [Requerimientos](#requerimientos)
- [Uso](#uso)

## Introducción

**Data Ingestion Tool** es una aplicación desarrollada en Python que utiliza Flask para la interfaz web y PySpark para el procesamiento distribuido de datos.  
El objetivo principal es demostrar un enfoque metadata-driven para la ingesta, validación y transformación de datos sin desarrollar soluciones ad-hoc. La configuración y el comportamiento del pipeline se definen mediante un archivo JSON de metadata.

## Características

- **Metadata-Driven:** Configuración dinámica de fuentes, validaciones, transformaciones y sinks mediante metadata.
- **Modularidad:** Código organizado en módulos (configuración, engine, ingestión, transformaciones y utilidades).
- **Escalabilidad:** Uso de PySpark para procesar grandes volúmenes de datos.
- **Interfaz Web:** Aplicación Flask que permite subir archivos, ejecutar el pipeline y ver resultados y logs en tiempo real.
- **Dockerización:** Capacidad para empaquetar y desplegar la aplicación en contenedores Docker.

## Requerimientos

- **Python 3.10**
- **Java** (requerido para PySpark)
- **Flask**
- **PySpark**

## Uso
Ejecutando la aplicación localmente
Inicia la aplicación desde la raíz del proyecto:

`
python main.py
Accede a la interfaz:
`

Abre tu navegador en http://127.0.0.1:5000
