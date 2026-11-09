"""Gráficos desde agregados; no requiere cargar viajes individuales."""
import argparse
import json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


def main(root):
    p=Path(root)
    r=json.loads((p/'resultados/resultados.json').read_text(encoding='utf-8'))
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':11,
        'axes.spines.top':False,'axes.spines.right':False})
    for name,title,xlabel in [
        ('por_hora','Viajes válidos por hora de inicio','Hora local de Nueva York'),
        ('por_dia','Viajes válidos por día de enero de 2025','Día del mes')]:
        rows=r[name]
        x=[a['hora'] if name=='por_hora' else int(a['fecha'][-2:]) for a in rows]
        y=[a['viajes'] for a in rows]
        fig,ax=plt.subplots(figsize=(8.2,3.7),layout='constrained')
        ax.bar(x,y,color='#176b87',width=.78)
        ax.set(title=title,xlabel=xlabel,ylabel='Número de viajes')
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v,pos:f'{v/1000:g} mil'))
        ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True);ax.set_xticks(x[::2])
        fig.savefig(p/'resultados'/f'{name}.png',dpi=220);plt.close(fig)
    source=p/'evidencias_vm/resumen_verificacion.json'
    if source.exists():
        s=json.loads(source.read_text())
        rows=s['groups_final'];windows=sorted(set(x['window'] for x in rows))
        fig,axes=plt.subplots(1,2,figsize=(9,3.8),layout='constrained')
        for ax,field,label in zip(axes,['temperature_avg','humidity_avg'],
                                 ['Temperatura simulada','Humedad simulada']):
            for w in windows:
                group=sorted([x for x in rows if x['window']==w],key=lambda x:x['sensor_id'])
                ax.plot([x['sensor_id'] for x in group],[x[field] for x in group],
                    marker='o',label=w[-5:]+' UTC')
            ax.set(xlabel='Sensor',ylabel=label,xticks=range(1,11))
            ax.grid(alpha=.2)
        axes[1].legend(fontsize=8,loc='upper center',bbox_to_anchor=(.5,-.2),ncol=2)
        fig.savefig(p/'resultados/streaming_promedios.png',dpi=220);plt.close(fig)


if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('--root',default='.')
    args=ap.parse_args();main(args.root)
