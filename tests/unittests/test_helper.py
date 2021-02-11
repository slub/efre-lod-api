import pytest
import hypothesis
from hypothesis import strategies as st
import lod_api.tools.helper as helper

@pytest.mark.unit
@pytest.mark.helper
@hypothesis.given(st.one_of(st.integers(),
                            st.booleans(),
                            st.sampled_from([1.0, "0"])
                            )
                 )
def test_isint_true(x):
    assert helper.isint(x)

@pytest.mark.unit
@pytest.mark.helper
@pytest.mark.xfail(strict=False)
@hypothesis.given(st.one_of(st.functions(),
                            st.characters()
                            )
                 )
def test_isint_false(x):
    assert not helper.isint(x)


if __name__ == "__main__":
    pytest.main()
