import copy
from datetime import datetime, timezone
import math
import numpy as np
import pytest
import torch
from ml.models import BehaviorGRU, GRU_HIDDEN, GRU_LAYERS, Logistic, NUMERIC_DIM, rule_group_codes, rule_scores, seed_all, tensors
from ml.metrics import SCORE_DTYPE, metrics, choose_threshold, descending_groups, evaluate_file
from ml.training import (amp_config, compute_warmup_steps, make_warmup_scheduler,
                         save_checkpoint, scaled_learning_rate, trusted_load)
from ml.experiment import rule_counts, select
from orchestration.common import ManifestError

# Stored behavior codes (PAD=0, pv=1, buy=2, cart=3, fav=4): see jobs/.../Samples.java.
BEHAVIOR_PV = 1
BEHAVIOR_BUY = 2


def test_exact_metrics_and_external_ties(tmp_path):
    p = np.array([.9, .8, .2, .1])
    y = np.array([1, 0, 1, 0])
    m = metrics(p, y, directory=tmp_path, chunk_size=2)
    assert m['ap'] == pytest.approx(5/6)
    assert m['roc_auc'] == .75
    assert m['brier'] == pytest.approx(.325)
    assert (m['tp'], m['fp'], m['fn'], m['tn']) == (1, 1, 1, 1)
    assert m['precision'] == m['recall'] == m['f1'] == .5
    tied = metrics(np.full(4, .5), y, directory=tmp_path, chunk_size=1)
    assert tied['ap'] == tied['roc_auc'] == .5
    assert list(descending_groups(np.full(4,.5), y, tmp_path, 1)) == [(.5, 2, 2)]
    assert metrics(p,np.ones(4),directory=tmp_path)['ap'] is None
    assert metrics(np.array([0.,1.]),np.array([1,0]),directory=tmp_path)['logloss'] < 17
    assert choose_threshold(np.full(4,.5),y) == .5
    assert choose_threshold(np.full(4,.5),np.zeros(4)) == .995
    assert sum(b['count'] for b in m['calibration']) == 4
    rng=np.random.default_rng(42)
    p=rng.integers(0,10,200)/10;y=rng.integers(0,2,200)
    assert metrics(p,y,directory=tmp_path,chunk_size=7) == metrics(p,y,directory=tmp_path,chunk_size=65536)
    expected_ap=0.
    for threshold in np.unique(p):
        expected_ap+=np.count_nonzero((p==threshold)&(y==1))/y.sum()*y[p>=threshold].mean()
    pairs=[1. if a>b else .5 if a==b else 0. for a in p[y==1] for b in p[y==0]]
    actual=metrics(p,y,directory=tmp_path,chunk_size=7)
    assert actual['ap']==pytest.approx(expected_ap)
    assert actual['roc_auc']==pytest.approx(np.mean(pairs))


def fixture_batch(n=16,lengths=None,width=8):
    if lengths is None:
        lengths=np.full(n,3,dtype=np.uint8)
    return dict(category=np.ones((n,width),dtype=np.int32),behavior=np.ones((n,width),dtype=np.uint8),
                hour=np.ones((n,width),dtype=np.uint8),gap=np.zeros((n,width),dtype=np.float32),
                length=np.asarray(lengths,dtype=np.uint8),numeric=np.zeros((n,NUMERIC_DIM),dtype=np.float32),
                label=np.tile([0.,1.],n//2).astype(np.float32))


def overfit(model,x,steps=100):
    """Return the first and last loss of a full-batch AdamW fit."""
    optimizer=torch.optim.AdamW(model.parameters(),lr=.03)
    losses=[]
    for _ in range(steps):
        optimizer.zero_grad()
        loss=torch.nn.functional.binary_cross_entropy_with_logits(model(x),x['label'])
        assert torch.isfinite(loss)
        losses.append(loss.item())
        loss.backward()
        optimizer.step()
    return losses[0],losses[-1]


def check_checkpoint(cls,model,x,tmp_path):
    """A saved state must reload identically, and a bad digest must be refused."""
    model.eval()
    with torch.no_grad():expected=model(x)
    path=tmp_path/'state.pt'
    digest=save_checkpoint(path,model.state_dict())
    loaded=cls()
    loaded.load_state_dict(trusted_load(path,digest,tmp_path))
    loaded.eval()
    with torch.no_grad():
        assert torch.allclose(expected,loaded(x),atol=1e-6,rtol=0)
    with pytest.raises(ManifestError):trusted_load(path,'0'*64,tmp_path)


def test_logistic_overfit_and_checkpoint(tmp_path):
    seed_all()
    model=Logistic()
    batch=fixture_batch()
    batch['numeric'][:,0]=batch['label']*2-1
    x=tensors(batch,torch.device('cpu'),'LOGISTIC')
    initial=copy.deepcopy(model.state_dict())
    first,last=overfit(model,x)
    assert last<.15 and last<first*.2
    assert any(not torch.equal(v,initial[k]) for k,v in model.state_dict().items())
    check_checkpoint(Logistic,model,x,tmp_path)


def test_gru_sequence_overfit_checkpoint(tmp_path):
    seed_all()
    model=BehaviorGRU()
    batch=fixture_batch()
    # numeric is all zero: the only label signal lives in the behavior sequence.
    batch['behavior'][:,0]=np.where(batch['label']==1,BEHAVIOR_BUY,BEHAVIOR_PV).astype(np.uint8)
    x=tensors(batch,torch.device('cpu'),'GRU')
    assert not torch.count_nonzero(x['numeric'])
    initial=copy.deepcopy(model.state_dict())
    first,last=overfit(model,x,steps=120)
    assert last<.15, (first,last)
    assert last<first*.2, (first,last)
    assert any(not torch.equal(v,initial[k]) for k,v in model.state_dict().items())
    assert torch.count_nonzero(model.category.weight[0])==0
    check_checkpoint(BehaviorGRU,model,x,tmp_path)


def test_gru_padding_ignored_within_max_length():
    seed_all()
    model=BehaviorGRU().eval()
    lengths=np.array([2,4,6,7,2,4,6,7],dtype=np.uint8)
    batch=fixture_batch(n=len(lengths),lengths=lengths,width=8)
    rng=np.random.default_rng(0)
    for i,length in enumerate(lengths):
        batch['category'][i,:length]=rng.integers(1,100,size=length)
        batch['behavior'][i,:length]=rng.integers(1,5,size=length)
        batch['hour'][i,:length]=rng.integers(1,25,size=length)
        batch['gap'][i,1:length]=rng.random(length-1)
    x=tensors(batch,torch.device('cpu'),'GRU')
    max_len=int(x['length'].max().item())
    # Six samples have padding *below* max_len, i.e. inside the columns the model reads.
    assert sum(length<max_len for length in lengths)==6

    with torch.no_grad():expected=model(x)
    padded={k:v.clone() for k,v in x.items()}
    for i,length in enumerate(lengths.tolist()):
        if length<max_len:
            padded['category'][i,length:max_len]=10
            padded['behavior'][i,length:max_len]=2
            padded['hour'][i,length:max_len]=3
            padded['gap'][i,length:max_len]=.5
    with torch.no_grad():
        assert torch.allclose(model(padded),expected,atol=1e-6,rtol=0)

    # Sanity check: editing the same columns *inside* each true length changes the output.
    inside={k:v.clone() for k,v in x.items()}
    for i,length in enumerate(lengths.tolist()):
        inside['category'][i,:length]=11
        inside['behavior'][i,:length]=4
        inside['hour'][i,:length]=5
        inside['gap'][i,1:length]=.25
    with torch.no_grad():
        assert not torch.allclose(model(inside),expected,atol=1e-6,rtol=0)


def test_model_widths_follow_numeric_dim():
    assert NUMERIC_DIM == 20
    assert GRU_LAYERS == 2
    assert GRU_HIDDEN == 128
    assert Logistic().linear.in_features == NUMERIC_DIM
    gru = BehaviorGRU()
    assert gru.gru.num_layers == 2
    assert gru.gru.hidden_size == 128
    assert gru.project[0].out_features == 128
    assert gru.head[0].in_features == 128 + NUMERIC_DIM
    assert gru.head[0].out_features == 64
    assert gru.head[-1].in_features == 64
    assert gru.head[-1].out_features == 1
    param_count = sum(p.numel() for p in gru.parameters())
    assert param_count > 400_000


def test_seed_all_deterministic_toggle():
    seed_all(42, deterministic=False)
    assert not torch.are_deterministic_algorithms_enabled()
    assert torch.backends.cudnn.benchmark is True
    assert torch.backends.cudnn.deterministic is False
    assert torch.backends.cuda.matmul.allow_tf32 is True
    assert torch.backends.cudnn.allow_tf32 is True

    seed_all(42, deterministic=True)
    assert torch.are_deterministic_algorithms_enabled()
    assert torch.backends.cudnn.benchmark is False
    assert torch.backends.cudnn.deterministic is True
    assert torch.backends.cuda.matmul.allow_tf32 is False
    assert torch.backends.cudnn.allow_tf32 is False
    # Restore default high-throughput mode for subsequent tests.
    seed_all(42, deterministic=False)


def test_large_batch_lr_scaling_and_warmup():
    assert scaled_learning_rate(0.001, 512) == pytest.approx(0.001)
    assert scaled_learning_rate(0.001, 4096) == pytest.approx(0.001 * math.sqrt(8.0))
    assert compute_warmup_steps(409600, 512) == 0
    assert compute_warmup_steps(409600, 4096) == 20

    param = torch.nn.Parameter(torch.zeros(1))
    opt = torch.optim.AdamW([param], lr=0.004)
    sched = make_warmup_scheduler(opt, warmup_steps=4)
    lrs = []
    for _ in range(6):
        lrs.append(opt.param_groups[0]['lr'])
        opt.step()
        sched.step()
    assert lrs == pytest.approx([0.001, 0.002, 0.003, 0.004, 0.004, 0.004])


def test_amp_config_selection(monkeypatch):
    assert amp_config(torch.device('cpu')) == (False, torch.float32, False, 'FP32')
    monkeypatch.setattr(torch.cuda, 'is_bf16_supported', lambda: True)
    assert amp_config(torch.device('cuda')) == (True, torch.bfloat16, False, 'BF16_AMP')
    monkeypatch.setattr(torch.cuda, 'is_bf16_supported', lambda: False)
    assert amp_config(torch.device('cuda')) == (True, torch.float16, True, 'FP16_AMP')


def test_validation_only_model_selection():
    def row(ap,ll,params):return dict(validation_metrics={'ALL':{'ap':ap,'logloss':ll}},parameter_count=params)
    simple=row(.5,.4,15);deep=row(.5+5e-7,.4,1000)
    assert select([simple,deep]) is simple
    assert select([simple,row(.6,.5,1000)])['validation_metrics']['ALL']['ap']==.6


def test_top_k_metrics_hand_calculated(tmp_path):
    scores=np.linspace(1.,0.,100)
    labels=np.tile([1,0],50)
    keys=np.array([f'{i:03d}'.encode() for i in range(100)],dtype='S64')
    m=metrics(scores,labels,sample_keys=keys,directory=tmp_path,chunk_size=7)
    assert m['precision_at_1pct']==1. and m['recall_at_1pct']==pytest.approx(1/50)
    assert m['lift_at_1pct']==pytest.approx(2.)
    assert m['precision_at_5pct']==pytest.approx(.6)
    assert m['recall_at_5pct']==pytest.approx(3/50)
    assert m['lift_at_5pct']==pytest.approx(1.2)
    assert m['precision_at_10pct']==pytest.approx(.5)
    assert m['recall_at_10pct']==pytest.approx(.1)
    assert m['lift_at_10pct']==pytest.approx(1.)
    tied=metrics(np.array([.9,.9,.8,.7]),np.array([1,0,1,0]),
                 sample_keys=np.array([b'b',b'a',b'c',b'd']),directory=tmp_path,chunk_size=1)
    assert tied['precision_at_10pct']==0.  # 'a' sorts before 'b' among tied .9 scores
    small=metrics(np.array([.9,.9,.8]),np.array([1,0,1]),sample_keys=np.array([b'a',b'b',b'c']),directory=tmp_path)
    assert all(small[f'precision_at_{suffix}'] is not None for suffix in ('1pct','5pct','10pct'))
    assert metrics(np.array([]),np.array([]),directory=tmp_path)['precision_at_1pct'] is None


def test_choose_threshold_uses_five_thousandth_grid():
    assert choose_threshold(np.array([.145,.144]),np.array([1,0]))==pytest.approx(.145)
    assert choose_threshold(np.full(4,.5),np.zeros(4))==.995


def test_day_groups_from_as_of(tmp_path):
    first=int(datetime(2017,12,1,16,tzinfo=timezone.utc).timestamp())
    second=int(datetime(2017,12,2,16,tzinfo=timezone.utc).timestamp())
    data=np.zeros(4,dtype=SCORE_DTYPE)
    data['score']=[.9,.1,.8,.2]
    data['label']=[1,0,1,0]
    data['cohort']=[1,0,1,0]
    data['sample_key']=[b'1',b'2',b'3',b'4']
    data['as_of']=[first,first,second,second]
    path=tmp_path/'scores.npy'
    np.save(path,data,allow_pickle=False)
    values=evaluate_file(path,.5)
    assert {'ALL','SEEN_TRAIN','UNSEEN_TRAIN','DAY:2017-12-02','DAY:2017-12-03'} <= set(values)
    assert values['DAY:2017-12-02']['sample_count']==2
    assert values['DAY:2017-12-02']['positive_count']==1
    assert values['DAY:2017-12-03']['sample_count']==2
    assert values['DAY:2017-12-03']['precision_at_10pct']==1.


def test_rule_table_from_synthetic_train():
    mean=np.full(NUMERIC_DIM,1.);std=np.full(NUMERIC_DIM,2.)
    raw=np.zeros((4,NUMERIC_DIM))
    raw[0,10]=3  # cart only (code 1000)
    raw[1,10]=3;raw[1,9]=3  # cart + buy (1100)
    raw[2,11]=3;raw[2,18]=3  # fav + other (0011)
    labels=np.array([1,0,1,0],dtype=np.int8)
    numeric=(raw-mean)/std
    codes=rule_group_codes(numeric,mean,std)
    assert codes.tolist()==[8,12,3,0]

    class SmallDataset:
        def batches(self,batch_size=512,seed=42,epoch=0,shuffle=True,columns=None):
            yield {k:v for k,v in dict(numeric=numeric,label=labels).items() if k in columns}

    counts,positives=rule_counts(SmallDataset(),mean,std)
    table={f'{code:04b}':(int(positives[code])+1)/(int(counts[code])+2) for code in range(16)}
    assert table['1000']==pytest.approx(2/3)
    assert table['1100']==pytest.approx(1/3)
    assert table['0011']==pytest.approx(2/3)
    assert table['0000']==pytest.approx(1/3)
    assert table['1111']==.5  # Laplace smoothing on empty groups
    assert rule_scores(numeric,dict(table=table,mean=mean.tolist(),std=std.tolist())).tolist()==pytest.approx([table['1000'],table['1100'],table['0011'],table['0000']])


def test_select_handles_rule_baseline():
    def row(ap,logloss,params,algorithm):
        return dict(algorithm=algorithm,validation_metrics={'ALL':{'ap':ap,'logloss':logloss}},parameter_count=params)
    constant=row(.5,.4,0,'CONSTANT')
    rule=row(.5,.4,0,'RULE')
    assert select([constant,rule]) is constant  # predefined order breaks exact ties
    assert select([rule,row(.6,.5,1000,'LOGISTIC')])['algorithm']=='LOGISTIC'
    assert select([row(None,.4,0,'RULE'),rule]) is rule


def test_rule_groups_survive_float32_standardization_round_trip():
    from ml.models import rule_group_codes, NUMERIC_DIM
    rng=np.random.default_rng(1)
    mean=rng.normal(size=NUMERIC_DIM);std=rng.uniform(.1,3,size=NUMERIC_DIM)
    raw=np.zeros((4,NUMERIC_DIM))
    raw[1,10]=np.log1p(1);raw[2,9]=np.log1p(3);raw[3,[10,9,11,18]]=np.log1p(1)
    stored=((raw-mean)/std).astype(np.float32)
    assert rule_group_codes(stored,mean,std).tolist()==[0,8,4,15]
