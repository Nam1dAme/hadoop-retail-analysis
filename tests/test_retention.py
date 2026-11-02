import pytest
from orchestration.common import ManifestError
from publication.retention import protected_path

def test_reference_protection_respects_path_components():
    roots={'/behavior/v1/source/batch-a','/behavior/v1/models/experiment-a'}
    assert protected_path('/behavior/v1/source/batch-a/raw.csv',roots)
    assert protected_path('/behavior/v1/models/experiment-a',roots)
    assert not protected_path('/behavior/v1/source/batch-ab/tmp',roots)
    with pytest.raises(ManifestError):protected_path('/behavior/v1/source/batch-a/../batch-b',roots)
