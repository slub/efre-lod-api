# for example, documentation see
# https://docs.pytest.org/en/latest/parametrize.html

import lod_api
from lod_api.cli import read_config


def pytest_addoption(parser):
    parser.addoption("--config", nargs=1, help="configuration file",
                     type=str)


def pytest_generate_tests(metafunc):

    config = metafunc.config.getoption("config")

    if config:
        read_config(config[0])
    else:
        if len(lod_api.__path__) == 1:
            print("take standard config \'apiconfig.yml\' from "
                  "project\'s root directory.")
            read_config(lod_api.__path__[0] + "/../apiconfig.yml")

    # provide parameter configuration for test that rely on them
    # as function parameter
    if "entity" in metafunc.fixturenames:
        metafunc.parametrize("entity", lod_api.CONFIG.get("indices_list"))
    if "source" in metafunc.fixturenames:
        metafunc.parametrize("source", lod_api.CONFIG.get("sources_list"))
    if "authority" in metafunc.fixturenames:
        authority_kv_pairs = []

        authorities = lod_api.CONFIG.get("authorities")
        for auth_key in authorities.keys():
            authority_kv_pairs.append([auth_key, authorities[auth_key]])

        metafunc.parametrize("authority", authority_kv_pairs)
    if "index_URI" in metafunc.fixturenames:
        metafunc.parametrize("index_URI", lod_api.CONFIG.get("indices").keys())
    if "test_count" in metafunc.fixturenames:
        metafunc.parametrize("test_count", [1])
