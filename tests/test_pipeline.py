import pytest

from renewflow.replay import replay_path


def test_benchmark_path_cannot_escape_its_directory(tmp_path):
    with pytest.raises(ValueError, match="paths inside the replay directory"):
        replay_path(tmp_path, "../outside.json")
