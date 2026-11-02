import copy
import numpy as np
import pytest
import torch
from ml.models import BehaviorGRU, Logistic, seed_all, tensors
from ml.metrics import metrics, choose_threshold, descending_groups
from ml.training import save_checkpoint, trusted_load
from ml.experiment import select


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
    assert choose_threshold(np.full(4,.5),np.zeros(4)) == .99
    assert sum(b['count'] for b in m['calibration']) == 4
    rng=np.random.default_rng(42)
    p=rng.integers(0,10,200)/10;y=rng.integers(0,2,200)
    assert metrics(p,y,directory=tmp_path,chunk_size=7) == metrics(p,y,directory=tmp_path,chunk_size=65536)


def fixture_batch(n=16):
    return dict(category=np.ones((n,6),dtype=np.int32),behavior=np.ones((n,6),dtype=np.uint8),
                hour=np.ones((n,6),dtype=np.uint8),gap=np.zeros((n,6),dtype=np.float32),
                length=np.full(n,3,dtype=np.uint8),numeric=np.zeros((n,14),dtype=np.float32),
                label=np.tile([0.,1.],n//2).astype(np.float32))


@pytest.mark.parametrize('cls',[Logistic,BehaviorGRU])
def test_real_gradient_overfit_checkpoint_and_padding(cls,tmp_path):
    seed_all()
    model=cls();batch=fixture_batch();batch['numeric'][:,0]=batch['label']*2-1
    x=tensors(batch,torch.device('cpu'),'GRU' if cls is BehaviorGRU else 'LOGISTIC')
    opt=torch.optim.AdamW(model.parameters(),lr=.03)
    initial=copy.deepcopy(model.state_dict())
    for _ in range(70):
        opt.zero_grad();loss=torch.nn.functional.binary_cross_entropy_with_logits(model(x),x['label'])
        assert torch.isfinite(loss);loss.backward();opt.step()
    assert loss.item()<.15
    assert any(not torch.equal(v,initial[k]) for k,v in model.state_dict().items())
    model.eval()
    with torch.no_grad():expected=model(x)
    if cls is BehaviorGRU:
        changed={k:v.clone() for k,v in x.items()};changed['category'][:,3:]=10;changed['behavior'][:,3:]=2
        with torch.no_grad():assert torch.equal(model(changed),expected)
        assert torch.count_nonzero(model.category.weight[0])==0
    p=tmp_path/'state.pt';digest=save_checkpoint(p,model.state_dict());loaded=cls()
    loaded.load_state_dict(trusted_load(p,digest,tmp_path));loaded.eval()
    with torch.no_grad():assert torch.allclose(expected,loaded(x),atol=1e-6,rtol=0)
    with pytest.raises(Exception):trusted_load(p,'0'*64,tmp_path)


def test_validation_only_model_selection():
    def row(ap,ll,params):return dict(validation_metrics={'ALL':{'ap':ap,'logloss':ll}},parameter_count=params)
    simple=row(.5,.4,15);deep=row(.5+5e-7,.4,1000)
    assert select([simple,deep]) is simple
    assert select([simple,row(.6,.5,1000)])['validation_metrics']['ALL']['ap']==.6
