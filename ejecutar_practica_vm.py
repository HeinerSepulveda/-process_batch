"""Práctica aislada en Linux. Ejecutar desde la raíz de tarea3_codex."""
import json
import os
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

root=Path.cwd()
kafka=root/'kafka_2.12-3.9.2'
out=root/'resultados_streaming';out.mkdir(exist_ok=True)
runtime=root/'runtime';runtime.mkdir(exist_ok=True)
env=os.environ.copy()
env['PYSPARK_PYTHON']=str(root/'.venv/bin/python')
env['SPARK_LOCAL_IP']='192.168.1.29'
(runtime/'zookeeper.properties').write_text(
    f'dataDir={runtime}/zkdata\nclientPort=2181\nclientPortAddress=127.0.0.1\nadmin.enableServer=false\n')
(runtime/'server.properties').write_text(
    f'broker.id=0\nlisteners=PLAINTEXT://127.0.0.1:9092\n'
    f'advertised.listeners=PLAINTEXT://127.0.0.1:9092\nlog.dirs={runtime}/kafkadata\n'
    'zookeeper.connect=127.0.0.1:2181\noffsets.topic.replication.factor=1\n'
    'transaction.state.log.replication.factor=1\ntransaction.state.log.min.isr=1\n'
    'group.initial.rebalance.delay.ms=0\n')
processes=[];handles=[]

def launch(name,cmd):
    f=(out/(name+'.log')).open('w');handles.append(f)
    p=subprocess.Popen(list(map(str,cmd)),stdout=f,stderr=subprocess.STDOUT,env=env)
    processes.append(p);return p

def waitport(port):
    for _ in range(60):
        try:
            with socket.create_connection(('127.0.0.1',port),timeout=1):return
        except OSError:time.sleep(1)
    raise TimeoutError(f'Puerto {port}')

try:
    launch('zookeeper',[kafka/'bin/zookeeper-server-start.sh',runtime/'zookeeper.properties'])
    waitport(2181);print('ZOOKEEPER_READY',flush=True)
    launch('broker',[kafka/'bin/kafka-server-start.sh',runtime/'server.properties'])
    waitport(9092);print('KAFKA_READY',flush=True)
    from kafka.admin import KafkaAdminClient,NewTopic
    admin=KafkaAdminClient(bootstrap_servers='127.0.0.1:9092')
    if 'sensor_data' not in admin.list_topics():
        admin.create_topics([NewTopic('sensor_data',1,1)])
    (out/'topic.json').write_text(json.dumps(admin.describe_topics(['sensor_data']),indent=2))
    admin.close()
    producer=launch('producer',[root/'.venv/bin/python',root/'codigo/kafka_producer.py','--events','150'])
    time.sleep(3)
    consumer=launch('consumer',['/opt/spark/bin/spark-submit','--master','local[2]',
        '--driver-memory','1g','--packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8',
        '--conf',f'spark.jars.ivy={runtime}/ivy',root/'codigo/spark_streaming_consumer.py',
        '--checkpoint',runtime/'checkpoint','--output',out,'--seconds','135'])
    for _ in range(60):
        if consumer.poll() is not None:break
        time.sleep(5)
        try:
            with urllib.request.urlopen('http://192.168.1.29:4040/api/v1/applications',timeout=2) as r:
                apps=json.load(r)
            app_id=apps[0]['id']
            (out/'spark-ui-applications.json').write_text(json.dumps(apps,indent=2))
            for part in ['jobs','stages','environment','executors']:
                url='http://192.168.1.29:4040/api/v1/applications/'+app_id+'/'+part
                with urllib.request.urlopen(url,timeout=3) as r:
                    (out/('spark-ui-'+part+'.json')).write_bytes(r.read())
        except Exception:pass
    if consumer.poll() is None:raise TimeoutError('Consumidor')
    producer.wait(timeout=90)
    status={'consumer_exit':consumer.returncode,'producer_exit':producer.returncode,
        'hostname':socket.gethostname(),'python':sys.version,'spark':'3.5.8',
        'kafka':'3.9.2','master':'local[2]','events_requested':150}
    (out/'ejecucion.json').write_text(json.dumps(status,indent=2))
    print(json.dumps(status),flush=True)
finally:
    for p in reversed(processes):
        if p.poll() is None:p.terminate()
    for p in processes:
        try:p.wait(timeout=15)
        except subprocess.TimeoutExpired:p.kill()
    for h in handles:h.close()
