# for example, documentation see 
# https://docs.pytest.org/en/latest/parametrize.html

from .http_status import HttpStatusBase
import lod_api


def pytest_generate_tests(metafunc):
    testClass = HttpStatusBase()
    testClass.setup()                 # to read CONFIG file

    if "entity" in metafunc.fixturenames:
        metafunc.parametrize("entity", lod_api.CONFIG.get("indices_list"))
    if "source" in metafunc.fixturenames:
        metafunc.parametrize("source", lod_api.CONFIG.get("sources_list"))
    if "authority" in metafunc.fixturenames:
        metafunc.parametrize("authority", lod_api.CONFIG.get("authorities"))
    if "test_count" in metafunc.fixturenames:
        metafunc.parametrize("test_count", [1])


