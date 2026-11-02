import plotly.graph_objects as go

COLORS={'pv':'#3159D6','buy':'#B66A16','cart':'#16838B','fav':'#7765AE'}
LABELS={'pv':'浏览','buy':'购买','cart':'加购','fav':'收藏'}


def finish(fig,height=320):
    fig.update_layout(height=height,paper_bgcolor='rgba(0,0,0,0)',plot_bgcolor='rgba(0,0,0,0)',
                      font=dict(family='Retail Numerals, Retail Chinese, sans-serif',size=12,color='#1D2B45'),
                      margin=dict(l=8,r=16,t=30,b=12),legend=dict(orientation='h',y=1.13,x=0),
                      hovermode='x unified',dragmode=False)
    fig.update_xaxes(showgrid=False,zeroline=False,automargin=True)
    fig.update_yaxes(gridcolor='#EAF0F6',zeroline=False,automargin=True)
    return fig


def trends(rows):
    fig=go.Figure()
    for kind in COLORS:
        data=[r for r in rows if r['behavior_type']==kind]
        fig.add_trace(go.Scatter(x=[r['bucket_start'] for r in data],y=[int(r['event_count']) for r in data],
                                name=LABELS[kind],mode='lines',line=dict(color=COLORS[kind],width=2),
                                hovertemplate='%{y:,} 次<extra>'+LABELS[kind]+'</extra>'))
    return finish(fig)


def heatmap(rows):
    dates=sorted({r['bucket_start'][:10] for r in rows});values={}
    for row in rows:
        key=(row['bucket_start'][:10],int(row['bucket_start'][11:13]))
        values[key]=values.get(key,0)+int(row['event_count'])
    fig=go.Figure(go.Heatmap(x=list(range(24)),y=dates,z=[[values.get((d,h),0) for h in range(24)] for d in dates],
                            colorscale=[[0,'#F0F4FB'],[.45,'#A5B9EE'],[1,'#3159D6']],
                            hovertemplate='%{y} %{x}:00<br>%{z:,} 次<extra></extra>',colorbar=dict(title='次数',thickness=10)))
    finish(fig,310);fig.update_layout(hovermode='closest');fig.update_yaxes(autorange='reversed')
    return fig


def ranking(rows):
    data=rows[:15][::-1]
    fig=go.Figure(go.Bar(x=[int(r['event_count']) for r in data],y=[r['entity_id'] for r in data],orientation='h',
                        marker_color='#3159D6',hovertemplate='ID %{y}<br>%{x:,} 次<extra></extra>'))
    finish(fig,max(240,len(data)*24+50));fig.update_yaxes(type='category');fig.update_layout(hovermode='closest')
    return fig


def lines(rows,x,series,height=310):
    fig=go.Figure()
    for key,label,color in series:
        fig.add_trace(go.Scatter(x=[r[x] for r in rows],y=[float(r[key]) if r.get(key) is not None else None for r in rows],
                                name=label,mode='lines+markers',line=dict(color=color,width=2),connectgaps=False))
    return finish(fig,height)
