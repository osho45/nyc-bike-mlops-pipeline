from jobs.ml_utils import load_config

def test_utf8_bom_config_keeps_environment_key(tmp_path):
    path=tmp_path/'config.yml';path.write_bytes(b'\xef\xbb\xbfenvironment: local\n')
    assert load_config(path)=={'environment':'local'}
