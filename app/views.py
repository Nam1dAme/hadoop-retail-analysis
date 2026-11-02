import json
import streamlit as st
from . import charts
from .components import section,metrics,number,ratio,filters,table,pagination,empty

BEHAVIORS={'pv':'浏览','buy':'购买','cart':'加购','fav':'收藏'}

def overview(api,release):
    params=filters('overview');data=api.get('/overview',**params)['data']
    metrics([(BEHAVIORS[k]+'行为',number(data[k+'_count']),'行为记录次数') for k in BEHAVIORS]+
            [('独立用户',number(data['uv']),'在所选区间内精确去重'),('浏览用户购买比例',ratio(data['pv_buy_ratio']),'同一区间内同时记录浏览与购买')])
    left,right=st.columns([2.1,1])
    with left,st.container(key='panel_trend'):
        section('行为随时间的变化','购买为 buy 行为记录，不等同于订单数。')
        granularity=st.selectbox('时间粒度',['day','hour'],format_func=lambda v:'按日' if v=='day' else '按小时')
        st.plotly_chart(charts.trends(api.get('/trends',**params,granularity=granularity)['data']),use_container_width=True)
    with right,st.container(key='panel_overview_note'):
        section('如何阅读这些数据')
        st.write('数据来自 2017 年历史用户行为日志。这里的变化描述该样本中的行为，不代表当前全站经营情况。')
        st.write('独立用户按整个查询区间去重，不能把每日用户数直接相加。')
        st.write('浏览用户购买比例只表示两类行为都被记录，不代表先浏览后购买的顺序漏斗。')
    with st.container(key='panel_heat'):
        section('一天中的活跃时间','北京时间；颜色表示全部四类行为的记录次数。')
        st.plotly_chart(charts.heatmap(api.get('/trends',**params,granularity='hour')['data']),use_container_width=True)


def entities(api,release):
    params=filters('entities');a,b=st.columns(2)
    with a:kind=st.selectbox('分析对象',['ITEM','CATEGORY'],format_func=lambda v:'商品' if v=='ITEM' else '类目')
    with b:behavior=st.selectbox('排行依据',list(BEHAVIORS),format_func=BEHAVIORS.get)
    with st.container(key='panel_entities'):
        section('关注度排行','仅显示原始 ID；数据没有商品或类目名称。次数相同时按 ID 排序。')
        rows=pagination(api,'/entities','entity_rank',dict(**params,entity_type=kind,behavior=behavior),{'entity_id':'编号','event_count':'行为次数','rank':'排名'})
        if rows:st.plotly_chart(charts.ranking(rows),use_container_width=True)
    with st.form('entity_search'):
        entity=st.text_input('商品或类目 ID',placeholder='输入完整数字编号')
        submitted=st.form_submit_button('查看趋势')
    if submitted:st.session_state['entity_detail']=entity
    if st.session_state.get('entity_detail'):
        value=st.session_state['entity_detail'];rows=api.get('/entities/'+value+'/trend',**params,entity_type=kind)['data']
        with st.container(key='panel_entity_detail'):
            section('编号 '+value+' 的每日行为')
            if rows:st.plotly_chart(charts.lines(rows,'date',[(k+'_count',BEHAVIORS[k],charts.COLORS[k]) for k in BEHAVIORS]),use_container_width=True)
            else:empty('未找到该编号','请确认分析对象、日期范围及完整编号。')


def activity(api,release):
    params=filters('activity');rows=api.get('/activity',**params)['data']
    with st.container(key='panel_activity'):
        section('活跃与首次观察','首次观察仅指在本数据观察期第一次出现，不代表真实注册日期。')
        st.plotly_chart(charts.lines(rows,'date',[('active_users','活跃用户','#3159D6'),('first_observed_users','首次观察用户','#16838B')]),use_container_width=True)
    with st.container(key='panel_retention'):
        section('次日复访','观察期最后一天没有可观察的次日，复访率留空。')
        st.plotly_chart(charts.lines(rows,'date',[('return_next_rate','次日复访率','#16838B'),('pv_buy_ratio','浏览用户购买比例','#B66A16')]),use_container_width=True)
        table(rows,{'date':'日期','active_users':'活跃用户','first_observed_users':'首次观察用户','return_next_users':'次日复访用户','return_next_rate':'次日复访率','pv_buy_ratio':'浏览用户购买比例'})
    with st.form('user_search'):
        user=st.text_input('用户 ID',placeholder='按字符串查询，保留原始编号')
        submit=st.form_submit_button('查询用户活动')
    if submit:st.session_state['active_user']=user
    if st.session_state.get('active_user'):
        table(api.get('/users/'+st.session_state['active_user']+'/activity',**params)['data'])


def predictions(api,release):
    experiment=api.get('/experiments')['data'];models=experiment['models']
    mode=st.radio('预测模式',['TEST','FORECAST'],format_func=lambda v:'历史测试（结果已观察）' if v=='TEST' else '未来窗口（结果未观察）',horizontal=True)
    if mode=='FORECAST':
        st.info('使用 2017-12-02 至 12-04 零点前的行为，预测 2017-12-04 全天。未观察到目标日结果，不能据此计算准确率；这不是对当前日期的预测。')
        model=release['selected_model_id']
    else:
        model=st.selectbox('模型',[r['model_run_id'] for r in models],format_func=lambda v:next(r['algorithm'] for r in models if r['model_run_id']==v))
        st.caption('历史测试目标窗口为北京时间 2017-12-02 全天。模型与阈值在查看测试结果前已固定。')
    with st.form('prediction_filter',border=False):
        user=st.text_input('用户 ID（留空查看分数排行）')
        submit=st.form_submit_button('应用查询',type='primary')
    if submit:st.session_state['prediction_user']=user
    with st.container(key='panel_prediction'):
        section('用户预测结果','分数为模型估计，不能直接解释为经过校准的真实购买概率。')
        pagination(api,'/predictions','prediction_rows',dict(mode=mode,model_run_id=model,user_id=st.session_state.get('prediction_user')),
                   {'user_id':'用户 ID','score':'预测分数','predicted_positive':'超过阈值','observed_label':'已观察标签','label_status':'标签状态','history_event_count':'历史行为数','as_of':'预测起点','window_end':'目标窗口结束','model_run_id':'模型版本'})


def experiments(api,release):
    data=api.get('/experiments')['data'];models=data['models']
    st.caption('预测目标：过去 48 小时有活动的用户，未来 24 小时是否记录 buy。')
    with st.container(key='panel_model_comparison'):
        section('三种算法的实测对照','仅验证集决定算法、参数与阈值；测试集用于最终检查。')
        rows=[]
        for model in models:
            row=dict(算法=model['algorithm'],设备=model['device'],参数量=model['parameter_count'],最佳轮次=model['best_epoch'] if model['best_epoch'] is not None else '无',阈值=model['threshold'],已选定=model['model_run_id']==data['selected_model_id'])
            for metric in model['metrics']:
                if metric['cohort']=='ALL' and metric['name'] in ('ap','roc_auc','logloss','f1'):row[metric['split']+' '+metric['name']]=metric['value']
            rows.append(row)
        table(rows)
        st.caption(data['selection_reason'])
    selected_index=next((i for i,row in enumerate(models) if row['model_run_id']==data['selected_model_id']),0)
    model=st.selectbox('查看实验详情',models,index=selected_index,format_func=lambda r:r['algorithm'])
    with st.container(key='panel_calibration'):
        section('预测分数与实际购买比例','每个概率桶同时报告样本数；空桶不当作零购买率。测试结果只用于检查，不改变已固定阈值。')
        split=st.selectbox('评价集合',['VALIDATION','TEST'],format_func=lambda v:'验证集' if v=='VALIDATION' else '测试集')
        cohort=st.selectbox('用户人群',['ALL','SEEN_TRAIN','UNSEEN_TRAIN'],format_func=lambda v:{'ALL':'全部用户','SEEN_TRAIN':'训练时已出现','UNSEEN_TRAIN':'训练时未出现'}[v])
        bins=model.get('calibration',{}).get(split,{}).get(cohort,[])
        if bins:
            a,b=st.columns(2)
            with a:st.plotly_chart(charts.lines(bins,'mean_score',[('observed_rate','实际购买比例','#16838B')]),use_container_width=True)
            with b:table(bins,{'start':'分数下界','end':'分数上界','count':'样本数','mean_score':'平均分数','observed_rate':'实际购买比例'})
        else:empty('没有可展示的校准分桶','发布时接入已封存的真实分桶结果；没有样本的人群留空。')
        values={m['name']:m['value'] for m in model['metrics'] if m['split']==split and m['cohort']==cohort}
        if all(values.get(k) is not None for k in ('tp','fp','fn','tn')):
            table([{'实际情况':'没有购买','预测没有购买':int(values['tn']),'预测购买':int(values['fp'])},
                   {'实际情况':'有购买','预测没有购买':int(values['fn']),'预测购买':int(values['tp'])}])
    with st.container(key='panel_training'):
        section('训练过程','CONSTANT 没有可训练参数，因此没有训练曲线。')
        if model['history']:
            a,b=st.columns(2)
            with a:st.plotly_chart(charts.lines(model['history'],'epoch',[('train_loss','训练损失','#3159D6'),('validation_logloss','验证损失','#16838B')]),use_container_width=True)
            with b:st.plotly_chart(charts.lines(model['history'],'epoch',[('validation_ap','验证 AP','#3159D6')]),use_container_width=True)
        else:empty('固定分数基线','所有用户使用训练集购买正例率作为分数，不进行参数训练。')
    with st.container(key='panel_model_resources'):
        section('资源与公平速度对照','FP32 CPU/GPU 对照与正式 AMP 训练分别记录。GPU 更慢也如实展示。')
        metrics([('训练耗时',f"{float(model['training_seconds'] or 0):,.1f} 秒",'包含训练与验证'),('显存分配峰值',f"{float(model['peak_allocated_mib'] or 0):,.1f} MiB",'不等于整张显卡占用'),('进程 RSS 峰值',f"{float(model['rss_mib'] or 0):,.1f} MiB",'本进程测量')])
        table(model['benchmarks'])
        with st.expander('参数与分人群评价'):
            st.json(model['config']);table(model['metrics'])


def quality(api,release):
    data=api.get('/quality')['data']
    metrics([('原始记录',number(data['raw_rows']),'完整来源文件'),('合法记录',number(data['accepted_rows']),'通过字段解析'),('拒绝记录',number(data['rejected_rows']),'保留原因审计'),('观察期内',number(data['in_scope_rows']),'用于正式分析'),('观察期外',number(data['outside_rows']),'不混入分析窗口'),('批次状态',data['status'],'已发布版本关联批次')])
    with st.container(key='panel_quality'):
        section('质量与来源','重复候选保留；异常日期单独计数。')
        st.warning('来源许可文字与链接仍有差异；当前用于本地研究，不对外分发原始数据。')
        table(data['metrics'],{'name':'检查项','dimension':'维度','value_count':'数量'})
    with st.container(key='panel_jobs'):
        section('Hadoop 与控制任务','带 application ID 的行是真实 YARN 作业；其他行为是控制、审计或导出阶段。')
        rows=pagination(api,'/tasks','task_rows',{}, {'stage':'阶段','attempt':'尝试次数','status':'状态','application_id':'YARN 应用','started_at':'开始时间','finished_at':'结束时间','error_code':'错误代码','run_id':'运行编号','performance':'Hadoop 计数器'})
        applications=[r for r in rows if r['application_id']]
        if applications:
            with st.expander('查看 YARN 作业证据'):
                task=st.selectbox('作业',applications,format_func=lambda r:r['stage']+' / '+r['application_id'])
                if task.get('performance'):st.json(task['performance'])
                st.link_button('打开 YARN 应用详情','http://127.0.0.1:18088/cluster/app/'+task['application_id'])
        st.caption('HDFS 负责归档；YARN 管理作业资源；MapReduce 负责清洗、统计和时间样本。GPU 训练由独立 PyTorch 进程执行。')


def versions(api,release):
    with st.container(key='panel_versions'):
        section('数据版本','一次查询固定在同一个完整版本；需要时可切换到当前版本。')
        pagination(api,'/releases','release_rows',{}, {'release_id':'版本','published_at':'发布时间','selected_model_id':'选定模型','is_current':'当前版本','status':'状态'})
    with st.container(key='panel_method'):
        section('范围与方法')
        st.write('观察期：北京时间 2017-11-25 至 2017-12-04，结束边界不包含在内。数据为抽样历史日志，不代表全站或长期情况。')
        st.write('训练集：11-27 至 11-30；验证集：12-01；测试集：12-02。每个样本使用前 48 小时行为，观察随后 24 小时是否购买。')
        st.write('没有金额、订单号、商品名称或曝光信息，不能计算 GMV、客单价或真实曝光点击率。用户可跨日期出现，这里检验时间泛化，另报告是否在训练集中出现。')
        st.write('当前为单机伪分布式，HDFS 副本 1，不提供硬件故障高可用。当前没有独立备份介质；本地恢复演练与磁盘故障保护分别验收。')
    report=api.get('/reports/summary')['data']
    st.download_button('下载汇总报告',json.dumps(report,ensure_ascii=False,indent=2),file_name='behavior-summary.json',mime='application/json')
