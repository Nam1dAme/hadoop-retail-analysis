import hashlib
import json
import pytest
from orchestration.sampling import record_samples
from orchestration.common import ManifestError

def test_exact_nested_hash_samples_and_source_integrity(tmp_path):
    source=tmp_path/'source.csv';lines=[f'{i},10,20,pv,1511539200\n'.encode() for i in range(1000)];source.write_bytes(b''.join(lines))
    digest=hashlib.sha256(source.read_bytes()).hexdigest()
    result=record_samples(source,tmp_path/'a',digest,1000,(20,100))
    repeated=record_samples(source,tmp_path/'b',digest,1000,(20,100))
    small=(tmp_path/'a/records-20.csv').read_bytes().splitlines();large=(tmp_path/'a/records-100.csv').read_bytes().splitlines()
    assert len(small)==20 and len(large)==100 and set(small)<=set(large)
    assert result['samples']['20']['sha256']==repeated['samples']['20']['sha256']
    assert result['samples']['100']['sha256']==repeated['samples']['100']['sha256']
    assert result['model_training_allowed'] is False
    assert record_samples(source,tmp_path/'a',digest,1000,(20,100))==result
    source.write_bytes(source.read_bytes()+b'changed')
    with pytest.raises(ManifestError):record_samples(source,tmp_path/'a',digest,1000,(20,100))
