"""Seven release-scoped pages, following the original Streamlit workbench style."""
import os
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import streamlit as st
from app.client import BehaviorAPI,ApiFault
from app.components import install_style,h,empty
from app import views

PAGES={
 'overview':('行为总览','观察行为规模与时间节律',views.overview),
 'entities':('商品与类目','按行为记录比较关注度',views.entities),
 'activity':('用户活跃','观察活跃、首次出现与次日复访',views.activity),
 'predictions':('行为预测','过去两天的行为，下一天的购买记录',views.predictions),
 'experiments':('模型实验','公平比较算法效果与计算资源',views.experiments),
 'quality':('数据质量与任务','从原始记录到真实作业证据',views.quality),
 'versions':('版本与方法','理解数据来源、窗口与方法边界',views.versions),
}

def changed():st.query_params['page']=st.session_state['page']

def main():
    st.set_page_config(page_title='零售观察：Hadoop 电商用户行为分析与预测系统',page_icon='◫',layout='wide',initial_sidebar_state='auto')
    install_style()
    requested=st.query_params.get('page','overview');requested=requested if requested in PAGES else 'overview'
    if st.session_state.get('page')!=requested:st.session_state['page']=requested
    api=BehaviorAPI(os.environ.get('BEHAVIOR_API_URL','http://127.0.0.1:8010/api/v1'))
    with st.sidebar:
        st.html('<div class="brand"><div class="brand-mark" aria-hidden="true"><i></i><i></i><i></i></div><div><strong>零售观察</strong><p>Behavior Analytics</p></div></div>')
        with st.container(key='navigation'):
            st.radio('页面',list(PAGES),format_func=lambda v:PAGES[v][0],key='page',on_change=changed,label_visibility='collapsed')
        st.html('<div class="sidebar-source"><strong>从行为到判断</strong><p>历史用户行为的全量分析<br>与购买行为预测工作台</p><p>2017 年历史日志<br>单机伪分布式 · HDFS 副本 1</p></div>')
    page=st.session_state['page'];title,subtitle,view=PAGES[page]
    a,b=st.columns([4,1.2],vertical_alignment='center')
    with a:st.html(f'<div class="page-crumb">工作台 / {h(title)}</div><div class="page-heading"><h1>{h(title)}</h1><p>{h(subtitle)}</p></div>')
    with b:switch=st.button('切换到当前版本',use_container_width=True)
    if 'fixed_release' not in st.session_state or switch:
        try:st.session_state['fixed_release']=api.get('/releases/current')['data'];st.session_state.pop('release_error',None)
        except ApiFault as fault:st.session_state['fixed_release']=None;st.session_state['release_error']=fault
    release=st.session_state.get('fixed_release');api.release=release
    if release:
        st.html('<div class="context-bar"><span class="context-status"><i class="context-dot"></i>数据版本已固定</span>'+f'<span>版本 <b>{h(release["release_id"])}</b></span><span>北京时间</span><span>2017-11-25 至 2017-12-04（不含）</span><span>购买行为预测</span></div>')
        try:
            view(api,release)
            st.html(f'<span data-behavior-page-ready="{h(page)}" hidden></span>')
        except ApiFault as fault:
            st.error(fault.message)
            if fault.request_id:st.caption('请求编号：'+fault.request_id)
        except (ValueError,KeyError):st.error('查询条件或响应格式不符合要求，请检查后重试。')
    else:
        st.html('<div class="context-bar"><span>尚未固定可用版本</span><span>北京时间</span><span>2017 年历史行为日志</span></div>')
        fault=st.session_state.get('release_error')
        if fault and fault.code!='NO_ACTIVE_RELEASE':st.error(fault.message)
        empty('尚无可用数据版本','完整分析、模型评价与未来评分发布后，点击“切换到当前版本”。当前不展示演示数据。')

if __name__=='__main__':main()
