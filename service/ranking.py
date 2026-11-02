"""Exact range TopK via sorted daily streams and a proven unseen-score bound."""
from datetime import timedelta
import time


def rank_entities(db,release,start,end,kind,behavior,limit,position=None,*,chunk=128,max_candidates=100000,budget_seconds=4.5):
    deadline=time.monotonic()+budget_seconds
    days=[];day=start
    while day<end:days.append(day);day+=timedelta(days=1)
    streams={day:dict(cursor=None,upper=None,done=False) for day in days}
    totals={}
    def read(sql,args):
        remaining=deadline-time.monotonic()
        if remaining<=0:raise TimeoutError('Entity ranking exceeded its bounded query budget')
        return db.all(sql,args,timeout=remaining)
    def eligible():
        rows=[dict(entity_id=entity,event_count=count) for entity,count in totals.items()
              if position is None or count<int(position[0]) or (count==int(position[0]) and entity>position[1])]
        rows.sort(key=lambda row:(-row['event_count'],row['entity_id']))
        return rows
    while True:
        new=set()
        for day,stream in streams.items():
            if stream['done']:continue
            args=[release,kind,behavior,day];seek=''
            if stream['cursor']:
                count,entity=stream['cursor'];seek=' AND (event_count<%s OR (event_count=%s AND entity_id>%s))';args.extend((count,count,entity))
            rows=read('SELECT entity_id,event_count FROM entity_daily FORCE INDEX (entity_rank_daily) WHERE release_id=%s AND entity_type=%s AND behavior_type=%s AND date=%s'+seek+' ORDER BY event_count DESC,entity_id ASC LIMIT %s',(*args,chunk))
            for row in rows:
                if row['entity_id'] not in totals:new.add(row['entity_id'])
            stream['done']=len(rows)<chunk
            stream['upper']=0 if stream['done'] else int(rows[-1]['event_count'])
            if rows:stream['cursor']=(int(rows[-1]['event_count']),rows[-1]['entity_id'])
        if len(totals)+len(new)>max_candidates:raise TimeoutError('Entity ranking candidate budget exceeded; no partial result returned')
        # Fetch every candidate's complete range sum, never add only daily TopK fragments.
        ids=sorted(new)
        totals.update((entity,0) for entity in ids)
        for offset in range(0,len(ids),128):
            subset=ids[offset:offset+128];marks=','.join(['%s']*len(subset))
            for day in days:
                rows=read('SELECT entity_id,event_count FROM entity_daily FORCE INDEX (PRIMARY) WHERE release_id=%s AND date=%s AND entity_type=%s AND entity_id IN ('+marks+') AND behavior_type=%s',
                          (release,day,kind,*subset,behavior))
                for row in rows:totals[row['entity_id']]+=int(row['event_count'])
        ordered=eligible();upper=sum(stream['upper'] or 0 for stream in streams.values())
        exhausted=all(stream['done'] for stream in streams.values())
        # Strict > also proves correctness for count ties and ID ordering.
        if exhausted or (len(ordered)>=limit and ordered[limit-1]['event_count']>upper):
            if time.monotonic()>deadline:raise TimeoutError('Entity ranking exceeded its bounded query budget')
            return ordered[:limit]
