"""Contraste independiente de eventos enviados y últimas actualizaciones."""
import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def verify(folder):
    folder=Path(folder)
    sent=[json.loads(line) for line in (folder/'producer.log').read_text().splitlines()
          if line.startswith('{"event":')]
    expected=defaultdict(list)
    for obj in sent:
        event=obj['event']
        minute=event['timestamp']//60*60
        expected[(minute,event['sensor_id'])].append(event)
    latest={}
    for f in sorted(folder.glob('batch_*.json')):
        for row in json.loads(f.read_text()):
            stamp=datetime.fromisoformat(row['window']['start']).replace(tzinfo=timezone.utc)
            latest[(int(stamp.timestamp()),row['sensor_id'])]=row
    errors=[];average_error=0.0
    for key,events in expected.items():
        actual=latest.get(key)
        if actual is None:
            errors.append('Grupo ausente '+str(key));continue
        if actual['events']!=len(events):errors.append('Conteo '+str(key))
        for field in ['temperature','humidity']:
            mean=sum(e[field] for e in events)/len(events)
            err=abs(mean-actual[field+'_avg'])
            average_error=max(average_error,err)
            if err>1e-9:errors.append('Promedio '+str(key))
    if set(latest)!=set(expected):errors.append('Conjunto de grupos diferente')
    status=json.loads((folder/'ejecucion.json').read_text())
    offsets=[e['offset'] for e in sent]
    if len(set(offsets))!=len(offsets):errors.append('Offsets repetidos')
    progress=json.loads((folder/'progress.json').read_text())
    input_rows=sum(x['numInputRows'] for x in progress)
    if input_rows!=len(sent):errors.append('Filas de progreso diferentes')
    verified=(not errors and status['consumer_exit']==0 and status['producer_exit']==0)
    rows=[]
    for (minute,sensor),row in sorted(latest.items()):
        rows.append({'window':datetime.fromtimestamp(minute,timezone.utc).strftime('%Y-%m-%d %H:%M'),
                     'sensor_id':sensor,'events':row['events'],
                     'temperature_avg':row['temperature_avg'],'humidity_avg':row['humidity_avg']})
    ui={}
    if (folder/'spark-ui-jobs.json').exists():
        ui={k:len(json.loads((folder/f'spark-ui-{k}.json').read_text()))
            for k in ['jobs','stages','executors']}
    summary={'verified':verified,'errors':errors,'events_sent':len(sent),
        'events_consumed':sum(x['events'] for x in latest.values()),'progress_input_rows':input_rows,
        'groups':len(latest),'max_average_error':average_error,'sample':rows[:5],
        'groups_final':rows,'ui':ui,'batch_updates':len(list(folder.glob('batch_*.json')))}
    (folder/'resumen_verificacion.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    print(json.dumps({k:v for k,v in summary.items() if k not in ['groups_final','sample']},indent=2))
    if not verified:raise SystemExit(1)


if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('folder');args=ap.parse_args();verify(args.folder)
