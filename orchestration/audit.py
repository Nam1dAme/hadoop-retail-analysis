"""与Java归并逻辑独立的有界审计；只读小聚合及每用户9位位图。"""
from datetime import date,timedelta
from .common import ManifestError
from .pipeline import files,stage_run
from .storage import Hdfs
START=date(2017,11,25)
ANCHORS=(2,3,4,5,6,7,9)
SPLITS=('TRAIN','TRAIN','TRAIN','TRAIN','VALIDATION','TEST','FORECAST')

def populations(storage,users):
    counts={s:dict(rows=0,positive_count=0,negative_count=0,seen=0,unseen=0) for s in set(SPLITS)}
    ranges={(str(START+timedelta(days=a)),str(START+timedelta(days=b))):[0,0,0] for a in range(9) for b in range(a+1,10)}
    intervals=[(key,sum(1<<d for d in range((date.fromisoformat(key[0])-START).days,(date.fromisoformat(key[1])-START).days))) for key in ranges]
    active=[0]*9;first=[0]*9;returns=[0]*8;total=0
    for line in storage.dump(files(users,'user_masks')):
        user,a,p,b=line.split('\t');a,p,b=map(int,(a,p,b));total+=1
        if a<=0 or a>=512 or p&~a or b&~a:raise ManifestError('Invalid user-day bitmap')
        alive=[bool(a&(1<<d)) for d in range(9)];first[alive.index(True)]+=1
        for d in range(9):active[d]+=alive[d]
        for d in range(8):returns[d]+=alive[d] and alive[d+1]
        for key,mask in intervals:
            v=ranges[key];v[0]+=bool(a&mask);v[1]+=bool(p&mask);v[2]+=bool(p&mask) and bool(b&mask)
        seen=any(a&((1<<(d-1))|(1<<(d-2))) for d in ANCHORS[:4])
        for d,split in zip(ANCHORS,SPLITS):
            if a&((1<<(d-1))|(1<<(d-2))):
                v=counts[split];v['rows']+=1;v['seen' if seen else 'unseen']+=1
                if split!='FORECAST':v['positive_count' if b&(1<<d) else 'negative_count']+=1
    return dict(samples=counts,ranges=ranges,active=active,first=first,returns=returns,users=total)

def analysis(lease,batch,stages):
    storage=Hdfs(lease.config,lease);clean=stages['J01']['counters'].get('BEHAVIOR',{});n=clean.get('in_scope_rows',0)
    totals=stages['J03']['counters'].get('ANALYTICS_TOTAL',{})
    if totals.get('cube',0)!=n or totals.get('entity',0)!=2*n:raise ManifestError('Cube/entity total conservation failed')
    cube={};by_type={k:0 for k in ('pv','buy','cart','fav')}
    for line in storage.dump(files(stages['J03'],'event_cube')):
        day,hour,kind,count=line.split('\t');count=int(count)
        if not 0<=int(hour)<24 or count<0:raise ManifestError('Invalid cube bucket')
        by_type[kind]+=count;cube[day]=cube.get(day,0)+count
    if any(v!=clean.get('type_'+k,0) for k,v in by_type.items()):raise ManifestError('Type totals differ from independent clean counters')
    pop=populations(storage,stages['J04users'])
    ranges={}
    for line in storage.dump(files(stages['J04summary'],'range_summary')):
        a,b,*v=line.split('\t');ranges[(a,b)]=list(map(int,v))
    if len(ranges)!=45:raise ManifestError('Expected all 45 date ranges')
    for key,expected in pop['ranges'].items():
        counts=ranges[key]
        if counts[:3]!=expected or counts[3]!=sum(v for d,v in cube.items() if key[0]<=d<key[1]):raise ManifestError('Range summary differs from bitmaps or cube')
    daily={}
    for line in storage.dump(files(stages['J04summary'],'activity_daily')):
        d,*v=line.split('\t');daily[d]=list(map(int,v))
    for d in range(9):
        expected=[pop['active'][d],pop['first'][d],pop['returns'][d] if d<8 else -1]
        if daily.get(str(START+timedelta(days=d)))!=expected:raise ManifestError('Activity/retention summary differs')
    if sum(f['rows'] for f in files(stages['J04users'],'user_daily'))!=sum(pop['active']):raise ManifestError('User-day row count differs')
    report={'status':'PASS','scoped_rows':n,'users':pop['users'],'event_counts':by_type,'date_ranges':45,'activity_days':9,'expected_samples':pop['samples']}
    def emit(storage,temp,local,run_id):
        storage.write_json(temp+'/analysis.json',report,local/'analysis.json');return {'analysis':report}
    bundle=stage_run(lease,batch,'A01',files(stages['J03'])+files(stages['J04users'])+files(stages['J04summary']),{'audit_version':'bitmap-and-cube-v1'},emit)
    return bundle['analysis']
