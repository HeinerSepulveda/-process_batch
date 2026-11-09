# Tarea 3 Procesamiento de datos con Apache Spark

Autor: Edwin Castro . Curso Big Data, UNAD, 202016911_28.

## Contenido

- `batch_taxis.py`: descarga oficial, limpieza, análisis y agregados del mes completo de enero de 2025 de TLC.
- `kafka_producer.py`: publica eventos simulados en `sensor_data`.
- `spark_streaming_consumer.py`: valida JSON, convierte segundos Unix, calcula promedios por sensor y ventana de un minuto y guarda las actualizaciones.
- `ejecutar_practica_vm.py`: automatiza la prueba aislada en la VM Linux; apaga sus procesos al terminar.
- `verificar_streaming.py`: contrasta los eventos enviados con los últimos resultados por ventana y sensor.
- `graficar_resultados.py`: reconstruye las figuras desde los agregados verificados.

## Datos y alcance

Fuente: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Descarga: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet

Archivo verificado: 59.158.238 bytes, 3.475.226 filas, 20 columnas.

SHA256: `9af277e4c0d3f9deb30644da822981e1e7df6af58313170fd3aa8a474485488a`.

Los datos se ofrecen bajo las condiciones de uso de NYC, enlazadas desde el registro oficial de AWS; no se les atribuye una licencia Creative Commons no comprobada. Los gráficos y estadísticas corresponden al subconjunto analítico de 3.314.425 viajes. Los registros excluidos no se declaran universalmente erróneos: algunos pueden representar reembolsos u operaciones atípicas válidas.

## Entorno Linux

La VM disponible tiene Spark 3.5.8, Scala 2.12.18 y Java 11. El conector debe ser `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8`. El anexo utiliza Spark 3.5.3: si se usa esa versión, cambiar también el conector a 3.5.3. Kafka 3.9.2 conserva el modo ZooKeeper empleado en el anexo. Kafka 4 no utiliza ese modo.

Desde la carpeta raíz del proyecto, con estos archivos bajo `codigo/`:

```bash
python3 -m venv .venv
.venv/bin/pip install kafka-python==2.2.15
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
/opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 1g \
  codigo/batch_taxis.py --output resultados
```

El programa descarga los datos si no existen en `datos/`. También acepta `--input` para rutas locales o montadas; la comprobación del archivo y su huella requiere acceso desde el driver. Para HDFS o S3, adaptar la descarga y la obtención de la huella antes de utilizar una URI distribuida. El ejemplo local no es una medición de escalabilidad de un clúster.

## Kafka con ZooKeeper

Descarga reproducible para este laboratorio: https://archive.apache.org/dist/kafka/3.9.2/kafka_2.12-3.9.2.tgz

Extraer el archivo en la raíz del proyecto, donde debe aparecer `kafka_2.12-3.9.2/`. El ejecutor usa carpetas propias `runtime/` y `resultados_streaming/`; necesita libres los puertos locales 2181 y 9092. La IP de la VM usada en la práctica está en `SPARK_LOCAL_IP` en el ejecutor. Cambiarla si el equipo tiene otra dirección.

```bash
.venv/bin/python codigo/ejecutar_practica_vm.py
```

El ejecutor crea el topic si no existe, inicia el productor de 150 eventos a un evento por segundo y después inicia el consumidor. Usa el conector Maven de Spark 3.5.8; necesita internet en la primera ejecución para descargarlo. Los JSON `batch_*.json` contienen actualizaciones, no conjuntos independientes: para reconstruir el resultado final se conserva la última versión por `(window, sensor_id)`, sin sumar varias veces actualizaciones del mismo grupo.

Para una demostración manual, iniciar ZooKeeper y broker usando los archivos generados bajo `runtime/` en terminales independientes. Crear el topic y ejecutar:

```bash
kafka_2.12-3.9.2/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server localhost:9092 --replication-factor 1 \
  --partitions 1 --topic sensor_data
.venv/bin/python codigo/kafka_producer.py
```

En otra terminal:

```bash
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
/opt/spark/bin/spark-submit --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
  codigo/spark_streaming_consumer.py
```

Abrir `http://192.168.1.29:4040` mientras la aplicación esté activa. Revisar Jobs, Stages, Environment y Executors. En ejecuciones simultáneas Spark puede utilizar 4041 u otro puerto siguiente. Detener las terminales con Ctrl+C.

## Decisiones de procesamiento

Batch: duplicados exactos sobre todas las columnas; seis campos necesarios completos; inicio dentro de enero de 2025; distancia mayor que cero y hasta 100 millas; duración mayor que cero y hasta 180 minutos; importe positivo y hasta USD 500; zona de recogida entre 1 y 263. No se exige `passenger_count`, porque no es necesario para las preguntas investigadas. Los límites son decisiones de alcance, no reglas oficiales de TLC.

Streaming: `timestamp` es `LongType` y se convierte con `timestamp_seconds`. Se usa una sola SparkSession, watermark de dos minutos, salida `update`, trigger de cinco segundos y checkpoint persistente. `earliest` solo se aplica al inicio sin checkpoint. La consola y el archivo de cada microbatch sirven para demostración; no se afirma entrega exactamente una vez en un sistema de producción.

Los datos emitidos no incluyen una unidad declarada en el anexo. Los valores de temperatura y humedad se interpretan como escala simulada del ejemplo; no sirven para inferencias ambientales reales.

## Validación

Comparar el resumen de filas con `resultados/resultados.json`. Inspeccionar los logs antes de aceptar una ejecución. Verificar códigos de salida de productor y consumidor y comparar, por ventana y sensor, conteos y promedios de los eventos enviados con la última actualización del consumidor. No sumar actualizaciones repetidas.

Las evidencias entregadas están bajo `evidencias_vm/`. Para comprobarlas:

```bash
python3 codigo/verificar_streaming.py evidencias_vm
```

Para reconstruir las gráficas, instalar matplotlib y ejecutar desde la raíz:

```bash
python3 -m pip install matplotlib==3.11.2
python3 codigo/graficar_resultados.py --root .
```

La reproducción batch en la VM obtuvo los mismos balances y estadísticas que el entorno Windows, dentro de la precisión numérica. Las salidas originales de la VM están bajo `evidencias_vm/batch/`. La prueba de sensores verificó 150 eventos y 35 grupos, con 20 archivos de actualizaciones. La API de Spark registró 40 trabajos, 80 etapas y un executor local durante la observación.

Una repetición sobre el mismo topic conserva eventos previos. Para analizar una sesión concreta, filtrar sus timestamps/offsets o crear una carpeta de laboratorio y un broker nuevos; no eliminar automáticamente datos anteriores. Mantener el checkpoint para reanudar la misma consulta. Una consulta modificada de forma incompatible necesita otro directorio de checkpoint.

## Publicación

Este directorio contiene código preparado para revisión y publicación. No contiene contraseñas. Los scripts de conexión SSH y las dependencias portátiles son herramientas locales de trabajo y no forman parte del repositorio académico. Los datos originales pueden descargarse desde TLC; no es necesario subirlos a Git.
