"""All business reads are scoped to an immutable READY release; no GPU or HDFS reads."""
from collections import OrderedDict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import threading
import time
from typing import Annotated, Literal
from uuid import UUID, uuid4
import pymysql
from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator
from orchestration.common import ROOT, BASELINE, fingerprint, load
from .database import Pool
from .ranking import rank_entities
from .security import Cursors, CursorError

COUNT_FIELDS = {'event_count','pv_count','buy_count','cart_count','fav_count','uv','pv_users','pv_buy_users',
                'active_users','first_observed_users','return_next_users','history_event_count','raw_rows',
                'accepted_rows','rejected_rows','in_scope_rows','outside_rows','sample_count','positive_count',
                'negative_count','parameter_count','value_count','source_bytes','rows','seen_count','unseen_count'}


def public(value, key=''):
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, dict):
        return {k:public(v,k) for k,v in value.items()}
    if isinstance(value, (list,tuple)):
        return [public(x,key) for x in value]
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc).isoformat().replace('+00:00','Z')
    if isinstance(value,date):
        return value.isoformat()
    if key in COUNT_FIELDS:
        return str(int(value))
    if key.isupper() and isinstance(value,int):
        return str(value)
    return float(value) if isinstance(value,Decimal) else value


class Fault(Exception):
    def __init__(self, status, code, message):
        self.status,self.code,self.message=status,code,message


class Params(BaseModel):
    model_config=ConfigDict(extra='forbid')


class ReleaseParams(Params):
    release_id: UUID


class WindowParams(ReleaseParams):
    start: date=date(2017,11,25)
    end: date=date(2017,12,4)

    @model_validator(mode='after')
    def valid_window(self):
        if not date(2017,11,25)<=self.start<self.end<=date(2017,12,4):
            raise ValueError('Date range must be inside the observation window; end is exclusive')
        return self


class PageParams(Params):
    cursor: str|None=None
    limit: int=Field(default=20,ge=1,le=100)


class TrendsParams(WindowParams):
    granularity: Literal['day','hour']='day'


class EntityParams(WindowParams,PageParams):
    entity_type: Literal['ITEM','CATEGORY']='ITEM'
    behavior: Literal['pv','buy','cart','fav']='pv'


class EntityTrendParams(WindowParams):
    entity_type: Literal['ITEM','CATEGORY']='ITEM'


class PredictionParams(ReleaseParams,PageParams):
    mode: Literal['TEST','FORECAST']='FORECAST'
    model_run_id: UUID|None=None
    user_id: str|None=Field(default=None,pattern=r'^\d{1,20}$')


class TasksParams(ReleaseParams,PageParams):
    pass


class Cache:
    def __init__(self):
        self.lock=threading.Lock();self.values=OrderedDict()

    def get(self,key,producer):
        with self.lock:
            cached=self.values.get(key)
            if cached and cached[0]>time.monotonic():
                self.values.move_to_end(key)
                return cached[1]
            value=producer()
            self.values[key]=(time.monotonic()+60,value)
            while len(self.values)>256:self.values.popitem(last=False)
            return value


def create_app(config=None,pool=None,cursor_key=None):
    config=config or load(ROOT/'config/development.toml')
    db=pool or Pool(config,ROOT/'var/secrets/reader.cnf')
    if cursor_key is None:
        cursor_key=(ROOT/'var/secrets/cursor.key').read_bytes()
    signer=Cursors(cursor_key);cache=Cache()
    app=FastAPI(title='行为观察只读 API',version='1.0.0',openapi_url='/api/v1/openapi.json',docs_url='/api/v1/docs')

    @app.middleware('http')
    async def envelope(request:Request,call_next):
        request.state.request_id=str(uuid4())
        try:response=await call_next(request)
        except (pymysql.MySQLError,TimeoutError,OSError):
            response=JSONResponse({'error':{'code':'DEPENDENCY_UNAVAILABLE','message':'结果库暂不可用，请稍后重试。','request_id':request.state.request_id}},status_code=503)
        except Exception:
            response=JSONResponse({'error':{'code':'INTERNAL_ERROR','message':'查询未完成，请保留请求编号重试。','request_id':request.state.request_id}},status_code=500)
        response.headers['X-Request-ID']=request.state.request_id
        response.headers['X-Content-Type-Options']='nosniff'
        return response

    @app.exception_handler(Fault)
    async def fault_handler(request,exc):
        return JSONResponse({'error':{'code':exc.code,'message':exc.message,'request_id':request.state.request_id}},status_code=exc.status)

    @app.exception_handler(CursorError)
    async def cursor_handler(request,exc):
        return await fault_handler(request,Fault(422,'INVALID_QUERY','分页游标无效或查询条件已经变化。'))

    @app.exception_handler(RequestValidationError)
    async def invalid_handler(request,exc):
        return await fault_handler(request,Fault(422,'INVALID_QUERY','请检查日期、编号、枚举值和查询参数。'))

    def ready(release):
        row=db.one('SELECT * FROM data_release WHERE release_id=%s',(str(release),))
        if not row:raise Fault(404,'NOT_FOUND','数据版本不存在。')
        if row['status']=='RETIRED':raise Fault(410,'RELEASE_RETIRED','数据版本已停用。')
        if row['status']!='READY':raise Fault(409,'RELEASE_NOT_READY','数据版本尚未发布。')
        return row

    def response(request,data,release=None,next_cursor=None,has_more=False):
        result=public(dict(data=data,meta=dict(release_id=str(release) if release else None,baseline_version=BASELINE,
                      request_id=request.state.request_id,next_cursor=next_cursor,has_more=has_more,business_timezone='Asia/Shanghai')))
        if len(json.dumps(result,ensure_ascii=False,allow_nan=False).encode())>2*1024**2:
            raise Fault(422,'INVALID_QUERY','结果过大，请缩小查询范围或分页。')
        return result

    def context(route,p,extra=None):
        values=p.model_dump(mode='json');values.pop('cursor',None)
        return fingerprint(dict(route=route,params=values,extra=extra))

    def paged(request,p,route,rows,position,extra=None):
        more=len(rows)>p.limit;rows=rows[:p.limit]
        token=signer.encode(context(route,p,extra),position(rows[-1])) if more else None
        return response(request,rows,getattr(p,'release_id',None),token,more)

    @app.get('/api/v1/health')
    def health(request:Request):
        if request.query_params:raise Fault(422,'INVALID_QUERY','健康接口不接受查询参数。')
        db.one('SELECT 1 AS ok')
        return response(request,dict(status='ok',database_available=True,baseline=BASELINE))

    def release_info(row):
        return {k:row[k] for k in ('release_id','batch_id','experiment_id','selected_model_id','status','published_at','manifest_hash')}

    @app.get('/api/v1/releases/current')
    def current(request:Request):
        if request.query_params:raise Fault(422,'INVALID_QUERY','当前版本接口不接受查询参数。')
        row=db.one('SELECT r.* FROM current_release c JOIN data_release r ON r.release_id=c.release_id WHERE c.singleton_id=1')
        if not row:raise Fault(404,'NO_ACTIVE_RELEASE','尚无可用数据版本。')
        ready(row['release_id'])
        info=release_info(row);info.update(source_start='2017-11-25',source_end='2017-12-04',forecast_as_of=row['forecast_as_of'],forecast_window_end=row['forecast_window_end'])
        return response(request,info,row['release_id'])

    @app.get('/api/v1/releases')
    def releases(request:Request,p:Annotated[PageParams,Query()]):
        conditions="r.status='READY'";args=[]
        if p.cursor:
            at,rid=signer.decode(p.cursor,context('releases',p))
            conditions+=' AND (r.published_at<%s OR (r.published_at=%s AND r.release_id>%s))';args+=[at,at,rid]
        rows=db.all('SELECT r.release_id,r.status,r.published_at,r.selected_model_id,(c.release_id=r.release_id) AS is_current FROM data_release r LEFT JOIN current_release c ON c.singleton_id=1 WHERE '+conditions+' ORDER BY r.published_at DESC,r.release_id ASC LIMIT %s',(*args,p.limit+1))
        for row in rows:row.update(source_start='2017-11-25',source_end='2017-12-04',is_current=bool(row['is_current']))
        return paged(request,p,'releases',rows,lambda r:[r['published_at'].isoformat(sep=' '),r['release_id']])

    def overview_data(p):
        key=('overview',str(p.release_id),p.start,p.end)
        def query():
            result=db.one('SELECT uv,pv_users,pv_buy_users FROM range_summary WHERE release_id=%s AND start_date=%s AND end_date=%s',(str(p.release_id),p.start,p.end))
            if not result:raise Fault(404,'NOT_FOUND','该范围没有已发布统计。')
            counts=db.all('SELECT behavior_type,SUM(event_count) AS event_count FROM event_cube WHERE release_id=%s AND date>=%s AND date<%s GROUP BY behavior_type',(str(p.release_id),p.start,p.end))
            result.update({kind+'_count':0 for kind in ('pv','buy','cart','fav')})
            result.update({r['behavior_type']+'_count':int(r['event_count']) for r in counts})
            result['pv_buy_ratio']=int(result['pv_buy_users'])/int(result['pv_users']) if result['pv_users'] else None
            return result
        return cache.get(key,query)

    @app.get('/api/v1/overview')
    def overview(request:Request,p:Annotated[WindowParams,Query()]):
        ready(p.release_id)
        return response(request,overview_data(p),p.release_id)

    @app.get('/api/v1/trends')
    def trends(request:Request,p:Annotated[TrendsParams,Query()]):
        ready(p.release_id)
        def query():
            rows=db.all('SELECT date,hour,behavior_type,event_count FROM event_cube WHERE release_id=%s AND date>=%s AND date<%s ORDER BY date,hour,behavior_type',(str(p.release_id),p.start,p.end))
            counts={}
            for row in rows:
                bucket=str(row['date'])+f'T{row["hour"]:02}:00:00+08:00' if p.granularity=='hour' else str(row['date'])
                k=(bucket,row['behavior_type']);counts[k]=counts.get(k,0)+int(row['event_count'])
            output=[];day=p.start
            while day<p.end:
                for hour in range(24) if p.granularity=='hour' else (None,):
                    bucket=str(day)+(f'T{hour:02}:00:00+08:00' if hour is not None else '')
                    for kind in ('pv','buy','cart','fav'):output.append(dict(bucket_start=bucket,behavior_type=kind,event_count=counts.get((bucket,kind),0)))
                day+=timedelta(days=1)
            return output
        return response(request,cache.get(('trends',context('trends',p)),query),p.release_id)

    @app.get('/api/v1/entities')
    def entities(request:Request,p:Annotated[EntityParams,Query()]):
        ready(p.release_id);position=signer.decode(p.cursor,context('entities',p)) if p.cursor else None
        def query():
            return rank_entities(db,str(p.release_id),p.start,p.end,p.entity_type,p.behavior,p.limit+1,position)
        rows=cache.get(('entities',context('entities',p),p.cursor),query)
        rows=[dict(r,rank=i+1+(int(position[2]) if position else 0)) for i,r in enumerate(rows)]
        return paged(request,p,'entities',rows,lambda r:[str(r['event_count']),r['entity_id'],r['rank']])

    @app.get('/api/v1/entities/{entity_id}/trend')
    def entity_trend(request:Request,entity_id:Annotated[str,Field(pattern=r'^\d{1,20}$')],p:Annotated[EntityTrendParams,Query()]):
        ready(p.release_id)
        rows=db.all('SELECT date,behavior_type,event_count FROM entity_daily FORCE INDEX (entity_lookup) WHERE release_id=%s AND entity_type=%s AND entity_id=%s AND date>=%s AND date<%s ORDER BY date',(str(p.release_id),p.entity_type,entity_id,p.start,p.end))
        grouped={}
        for row in rows:
            value=grouped.setdefault(str(row['date']),dict(date=str(row['date']),pv_count=0,buy_count=0,cart_count=0,fav_count=0))
            value[row['behavior_type']+'_count']=row['event_count']
        return response(request,list(grouped.values()),p.release_id)

    @app.get('/api/v1/activity')
    def activity(request:Request,p:Annotated[WindowParams,Query()]):
        ready(p.release_id)
        def query():
            rows=db.all('SELECT a.*,r.pv_users,r.pv_buy_users FROM activity_daily a JOIN range_summary r ON r.release_id=a.release_id AND r.start_date=a.date AND r.end_date=DATE_ADD(a.date,INTERVAL 1 DAY) WHERE a.release_id=%s AND a.date>=%s AND a.date<%s ORDER BY a.date',(str(p.release_id),p.start,p.end))
            for r in rows:
                r.pop('release_id',None);r['return_next_rate']=r['return_next_users']/r['active_users'] if r['active_users'] and r['return_next_users'] is not None else None
                r['pv_buy_ratio']=r['pv_buy_users']/r['pv_users'] if r['pv_users'] else None
            return rows
        return response(request,cache.get(('activity',context('activity',p)),query),p.release_id)

    @app.get('/api/v1/users/{user_id}/activity')
    def user_activity(request:Request,user_id:Annotated[str,Field(pattern=r'^\d{1,20}$')],p:Annotated[WindowParams,Query()]):
        ready(p.release_id)
        rows=db.all('SELECT date,pv_count,buy_count,cart_count,fav_count,first_ts,last_ts FROM user_daily WHERE release_id=%s AND user_id=%s AND date>=%s AND date<%s ORDER BY date',(str(p.release_id),user_id,p.start,p.end))
        if not rows:raise Fault(404,'NOT_FOUND','该用户在查询范围内没有活动记录。')
        return response(request,rows,p.release_id)

    @app.get('/api/v1/predictions')
    def predictions(request:Request,p:Annotated[PredictionParams,Query()]):
        release=ready(p.release_id);model=str(p.model_run_id or release['selected_model_id'])
        if p.mode=='FORECAST' and model!=release['selected_model_id']:raise Fault(422,'INVALID_QUERY','未来评分仅提供该版本选定的模型。')
        row=db.one('SELECT model_run_id FROM model_run WHERE model_run_id=%s AND experiment_id=%s',(model,release['experiment_id']))
        if not row:raise Fault(404,'NOT_FOUND','模型不属于该版本。')
        conditions='release_id=%s AND mode=%s AND model_run_id=%s';args=[str(p.release_id),p.mode,model]
        if p.user_id:conditions+=' AND user_id=%s';args.append(p.user_id)
        if p.cursor:
            last_score,last_user=signer.decode(p.cursor,context('predictions',p,model))
            conditions+=' AND (score<%s OR (score=%s AND user_id>%s))';args+=[last_score,last_score,last_user]
        rows=db.all('SELECT user_id,score,predicted_positive,label AS observed_label,history_event_count,as_of,window_end,model_run_id FROM prediction WHERE '+conditions+' ORDER BY score DESC,user_id ASC LIMIT %s',(*args,p.limit+1))
        for row in rows:row.update(predicted_positive=bool(row['predicted_positive']),label_status='UNOBSERVED' if p.mode=='FORECAST' else 'OBSERVED')
        return paged(request,p,'predictions',rows,lambda r:[r['score'],r['user_id']],model)

    def model_data(release,model_id):
        row=db.one('SELECT * FROM model_run WHERE model_run_id=%s AND experiment_id=%s',(str(model_id),release['experiment_id']))
        if not row:raise Fault(404,'NOT_FOUND','模型不属于该版本。')
        result={k:row[k] for k in ('model_run_id','algorithm','status','device','best_epoch','threshold','parameter_count','training_seconds','peak_allocated_mib','peak_reserved_mib','rss_mib')}
        result['config']=json.loads(row['config_json']);result['hardware']=json.loads(row['hardware_json'])
        result['history']=db.all('SELECT epoch,train_loss,validation_ap,validation_logloss,duration_seconds,sample_count,peak_gpu_mib,skipped_updates FROM model_epoch WHERE model_run_id=%s ORDER BY epoch',(str(model_id),))
        result['metrics']=db.all('SELECT split,cohort,name,value,sample_count,positive_count,reason FROM model_metric WHERE model_run_id=%s ORDER BY split,cohort,name',(str(model_id),))
        result['calibration']={}
        for metric in result['metrics']:
            if not metric['name'].startswith('calibration_'):continue
            _,index,key=metric['name'].split('_',2)
            cohort=result['calibration'].setdefault(metric['split'],{}).setdefault(metric['cohort'],{})
            cohort.setdefault(index,dict(start=int(index)/10,end=(int(index)+1)/10))[key]=metric['value']
        for split in result['calibration'].values():
            for cohort,bins in list(split.items()):split[cohort]=[bins[str(i)] for i in range(10) if str(i) in bins]
        result['benchmarks']=db.all('SELECT device,`precision`,repeat_index,steps,elapsed_seconds,throughput,peak_allocated_mib,peak_reserved_mib,rss_mib FROM benchmark WHERE experiment_id=%s ORDER BY device,repeat_index',(release['experiment_id'],))
        return result

    @app.get('/api/v1/models/{model_run_id}')
    def model(request:Request,model_run_id:UUID,p:Annotated[ReleaseParams,Query()]):
        release=ready(p.release_id)
        return response(request,model_data(release,model_run_id),p.release_id)

    @app.get('/api/v1/experiments')
    def experiments(request:Request,p:Annotated[ReleaseParams,Query()]):
        release=ready(p.release_id)
        def query():
            row=db.one('SELECT experiment_id,split_version,status,selected_model_id,test_sealed_at,selection_json FROM experiment WHERE experiment_id=%s',(release['experiment_id'],))
            selection=json.loads(row.pop('selection_json'))
            row.update(selection_reason=selection['reason'],models=[model_data(release,m['model_run_id']) for m in selection['finalists']])
            manifest=json.loads(release['manifest_json']);row['splits']=manifest['samples']
            return row
        return response(request,cache.get(('experiments',str(p.release_id)),query),p.release_id)

    def quality_data(release):
        batch=db.one('SELECT raw_rows,accepted_rows,rejected_rows,in_scope_rows,outside_rows,status FROM data_batch WHERE batch_id=%s',(release['batch_id'],))
        batch['metrics']=db.all('SELECT name,dimension,value AS value_count FROM quality_metric WHERE batch_id=%s ORDER BY name,dimension',(release['batch_id'],))
        batch['source_status']='provenance_pending';batch['scope']='本地研究；许可文字与链接存在差异'
        return batch

    @app.get('/api/v1/quality')
    def quality(request:Request,p:Annotated[ReleaseParams,Query()]):
        release=ready(p.release_id)
        return response(request,quality_data(release),p.release_id)

    @app.get('/api/v1/tasks')
    def tasks(request:Request,p:Annotated[TasksParams,Query()]):
        release=ready(p.release_id);args=[release['batch_id']];condition='batch_id=%s'
        if p.cursor:
            created,run=signer.decode(p.cursor,context('tasks',p));condition+=' AND (created_at<%s OR (created_at=%s AND run_id>%s))';args+=[created,created,run]
        rows=db.all('SELECT run_id,stage,attempt,status,application_id,started_at,finished_at,error_code,created_at FROM task_run WHERE '+condition+' ORDER BY created_at DESC,run_id ASC LIMIT %s',(*args,p.limit+1))
        evidence=json.loads(release['manifest_json']).get('task_evidence',{})
        for row in rows:row['performance']=evidence.get(row['run_id'])
        return paged(request,p,'tasks',rows,lambda r:[r['created_at'].isoformat(sep=' '),r['run_id']])

    @app.get('/api/v1/reports/summary')
    def summary(request:Request,p:Annotated[ReleaseParams,Query()]):
        release=ready(p.release_id)
        return response(request,dict(release=release_info(release),overview=overview_data(WindowParams(release_id=p.release_id)),quality=quality_data(release)),p.release_id)

    return app
