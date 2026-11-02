import base64
from datetime import date
from html import escape
from pathlib import Path
import streamlit as st

def install_style():
    assets=Path(__file__).parent/'assets'
    css=(assets/'theme.css').read_text()
    for name in ('plex','noto'):css=css.replace('{{'+name+'}}',base64.b64encode((assets/(name+'.woff2')).read_bytes()).decode())
    st.html('<style>'+css+'</style>')

def h(value):return escape(str(value),quote=True)

def empty(title,message):
    st.html(f'<div class="empty-state"><h3>{h(title)}</h3><p>{h(message)}</p></div>')

def section(title,note=''):
    st.html(f'<div class="section-title"><h2>{h(title)}</h2><p>{h(note)}</p></div>')

def metrics(items):
    st.html('<div class="metric-strip">'+''.join(f'<div class="metric"><span>{h(label)}</span><strong>{h(value)}</strong><small>{h(note)}</small></div>' for label,value,note in items)+'</div>')

def number(value):return f'{int(value):,}' if value is not None else '—'
def ratio(value):return f'{float(value):.2%}' if value is not None else '无可计算样本'

def filters(page):
    with st.form(page+'_dates',border=False):
        a,b,c=st.columns([2,2,1],vertical_alignment='bottom')
        with a:start=st.date_input('开始日期',date(2017,11,25),min_value=date(2017,11,25),max_value=date(2017,12,3),key=page+'_start')
        with b:end=st.date_input('结束日期（不含）',date(2017,12,4),min_value=date(2017,11,26),max_value=date(2017,12,4),key=page+'_end')
        with c:submitted=st.form_submit_button('应用范围',use_container_width=True,type='primary')
    if submitted:
        if start>=end:st.error('开始日期必须早于结束日期。')
        else:st.session_state[page+'_window']=(start.isoformat(),end.isoformat())
    start,end=st.session_state.get(page+'_window',('2017-11-25','2017-12-04'))
    return dict(start=start,end=end)

def table(rows,labels=None):
    if not rows:empty('没有匹配结果','请检查查询条件，或切换到其他日期范围。');return
    st.dataframe(rows,use_container_width=True,hide_index=True,column_config={key:st.column_config.Column(label) for key,label in (labels or {}).items()})

def pagination(api,path,page,params,labels=None):
    identity=str(sorted(params.items()))+str(api.release['release_id'])
    if st.session_state.get(page+'_identity')!=identity:
        st.session_state[page+'_identity']=identity;st.session_state[page+'_cursors']=[None]
    cursors=st.session_state[page+'_cursors']
    result=api.get(path,**params,cursor=cursors[-1],limit=20)
    table(result['data'],labels)
    a,b,c=st.columns([1,2,1])
    with a:
        if st.button('上一页',disabled=len(cursors)==1,key=page+'_previous',use_container_width=True):cursors.pop();st.rerun()
    with b:st.caption(f'第 {len(cursors)} 页；每页最多 20 条。')
    with c:
        if st.button('下一页',disabled=not result['meta']['has_more'],key=page+'_next',use_container_width=True):cursors.append(result['meta']['next_cursor']);st.rerun()
    return result['data']
