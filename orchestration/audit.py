"""与Java归并逻辑独立的有界审计；只读小聚合及每用户9位位图。"""
from datetime import date,timedelta
from .common import ConfigError,ManifestError,DATASET_DAYS,dataset_window
from .pipeline import files,stage_run
from .storage import Hdfs

# 窗口和锚点来自配置单一来源；v2 切分（2..9 相对日、8 个锚点）固定依赖 9 天位图。
WINDOW=dataset_window()
START=WINDOW['start']
DAYS=WINDOW['days']
ANCHORS=tuple(range(2,DAYS+1))
SPLITS=('TRAIN','TRAIN','TRAIN','TRAIN','VALIDATION','TEST','TEST','FORECAST')
RANGE_COUNT=DAYS*(DAYS+1)//2
if DAYS!=DATASET_DAYS or ANCHORS[-1]!=DAYS or len(ANCHORS)!=len(SPLITS):
    raise ConfigError('Fixed sample split requires a 9-day window with 8 anchors')

def populations(storage,users,window=None):
    window=window or WINDOW
    start=window['start'];days=window['days']
    counts={s:dict(rows=0,positive_count=0,negative_count=0,seen=0,unseen=0) for s in set(SPLITS)}
    ranges={(str(start+timedelta(days=a)),str(start+timedelta(days=b))):[0,0,0] for a in range(days) for b in range(a+1,days+1)}
    intervals=[(key,sum(1<<d for d in range((date.fromisoformat(key[0])-start).days,(date.fromisoformat(key[1])-start).days))) for key in ranges]
    active=[0]*days;first=[0]*days;returns=[0]*(days-1);total=0
    for line in storage.dump(files(users,'user_masks')):
        user,a,p,b=line.split('\t');a,p,b=map(int,(a,p,b));total+=1
        if a<=0 or a>=1<<days or p&~a or b&~a:raise ManifestError('Invalid user-day bitmap')
        alive=[bool(a&(1<<d)) for d in range(days)];first[alive.index(True)]+=1
        for d in range(days):active[d]+=alive[d]
        for d in range(days-1):returns[d]+=alive[d] and alive[d+1]
        for key,mask in intervals:
            v=ranges[key];v[0]+=bool(a&mask);v[1]+=bool(p&mask);v[2]+=bool(p&mask) and bool(b&mask)
        seen=any(a&((1<<(d-1))|(1<<(d-2))) for d in ANCHORS[:4])
        # 位图9位：bit 0..8 对应观察期各日；锚点8（观察期最后一天）的标签位是 bit 8。
        for d,split in zip(ANCHORS,SPLITS):
            if a&((1<<(d-1))|(1<<(d-2))):
                v=counts[split];v['rows']+=1;v['seen' if seen else 'unseen']+=1
                if split!='FORECAST':v['positive_count' if b&(1<<d) else 'negative_count']+=1
    return dict(samples=counts,ranges=ranges,active=active,first=first,returns=returns,users=total)

def analysis(lease,batch,stages):
    window=dataset_window(lease.config);days=window['days'];range_count=days*(days+1)//2
    storage=Hdfs(lease.config,lease);clean=stages['J01']['counters'].get('BEHAVIOR',{});n=clean.get('in_scope_rows',0)
    totals=stages['J03']['counters'].get('ANALYTICS_TOTAL',{})
    if totals.get('cube',0)!=n or totals.get('entity',0)!=2*n:raise ManifestError('Cube/entity total conservation failed')
    cube={};by_type={k:0 for k in ('pv','buy','cart','fav')}
    for line in storage.dump(files(stages['J03'],'event_cube')):
        day,hour,kind,count=line.split('\t');count=int(count)
        if not 0<=int(hour)<24 or count<0:raise ManifestError('Invalid cube bucket')
        by_type[kind]+=count;cube[day]=cube.get(day,0)+count
    if any(v!=clean.get('type_'+k,0) for k,v in by_type.items()):raise ManifestError('Type totals differ from independent clean counters')
    pop=populations(storage,stages['J04users'],window)
    ranges={}
    for line in storage.dump(files(stages['J04summary'],'range_summary')):
        a,b,*v=line.split('\t');ranges[(a,b)]=list(map(int,v))
    if len(ranges)!=range_count:raise ManifestError(f'Expected all {range_count} date ranges')
    for key,expected in pop['ranges'].items():
        counts=ranges[key]
        if counts[:3]!=expected or counts[3]!=sum(v for d,v in cube.items() if key[0]<=d<key[1]):raise ManifestError('Range summary differs from bitmaps or cube')
    daily={}
    for line in storage.dump(files(stages['J04summary'],'activity_daily')):
        d,*v=line.split('\t');daily[d]=list(map(int,v))
    for d in range(days):
        expected=[pop['active'][d],pop['first'][d],pop['returns'][d] if d<days-1 else -1]
        if daily.get(str(window['dates'][d]))!=expected:raise ManifestError('Activity/retention summary differs')
    if sum(f['rows'] for f in files(stages['J04users'],'user_daily'))!=sum(pop['active']):raise ManifestError('User-day row count differs')
    report={'status':'PASS','scoped_rows':n,'users':pop['users'],'event_counts':by_type,'date_ranges':range_count,'activity_days':days,'expected_samples':pop['samples']}
    def emit(storage,temp,local,run_id):
        storage.write_json(temp+'/analysis.json',report,local/'analysis.json');return {'analysis':report}
    bundle=stage_run(lease,batch,'A01',files(stages['J03'])+files(stages['J04users'])+files(stages['J04summary']),{'audit_version':'bitmap-and-cube-v1'},emit)
    return bundle['analysis']
