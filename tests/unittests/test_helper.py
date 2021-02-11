import pytest
import hypothesis
from hypothesis import strategies as st
import lod_api.tools.helper as helper
import elasticsearch

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

#####################################################################

@pytest.fixture()
def nestedObject():
    dictionary = { 
            "name": "test",
            "list": ["1", "2", "3"],
            "emptylist": [],
            "singlelist": ["alone"],
            "intlist": [1, 2, 3],
            }
    dictionary["deeper"] = dictionary.copy()
    return dictionary

@pytest.mark.unit
@pytest.mark.helper
def test_getNestedJsonObject(nestedObject):
    print(nestedObject)
    assert helper.getNestedJsonObject(nestedObject, "name") == "test"
    assert helper.getNestedJsonObject(nestedObject, "deeper>name") == "test"
    assert helper.getNestedJsonObject(nestedObject, "list>0") == "1"
    assert helper.getNestedJsonObject(nestedObject, "emptylist") == []
    assert helper.getNestedJsonObject(nestedObject, "singlelist>0") == "alone"
    assert helper.getNestedJsonObject(nestedObject, "intlist>0") == 1
    assert helper.getNestedJsonObject(nestedObject, "deeper>intlist>0") == 1


@pytest.mark.unit
@pytest.mark.helper
def test_getNestedJsonObject_Fail(nestedObject):
    # attribute does not exist:
    with pytest.raises(KeyError):
        helper.getNestedJsonObject(nestedObject, "bogus")
    # attribute list does not exist in a deeper level:
    with pytest.raises(KeyError):
        helper.getNestedJsonObject(nestedObject, "deeper>boguslist>0")
    with pytest.raises(KeyError):
        helper.getNestedJsonObject(nestedObject, "name>0")
    with pytest.raises(IndexError):
        helper.getNestedJsonObject(nestedObject, "emptylist>0")
    with pytest.raises(IndexError):
        helper.getNestedJsonObject(nestedObject, "emptylist>1")

#####################################################################

@pytest.mark.unit
@pytest.mark.helper
@pytest.mark.xfail(strict=False)
def test_get_fields_with_subfields():
    # TODO: implement test
    helper.get_fields_with_subfields("test", {"test": "dunno"})
    assert false

#####################################################################
    
class elasticsearch6_fake:
    "fakes elasticsearch 6 functionality"
    class indices:
        def get_mapping(index):
            # return mapping with doctype included
            return {index: {"mappings": {"old_doc_type": {"properties": "some"}}}}

    def __init__(self):
        indices = self.indices()

    def info(self):
        return {"version": {"number": [6]}} # version of running es instance
    # def search(self, **kwargs):
    #     if "_source_excludes" in kwargs:
    #         raise Exception("not implemented in pre es7 yet")
    #     if "_source_includes" in kwargs:
    #         raise Exception("not implemented in pre es7 yet")

class elasticsearch7_fake:
    "fakes elasticsearch 7 functionality"
    class indices:
        def get_mapping(index):
            # return mapping with doctype included
            return {index: {"mappings": {"properties": "someother"}}}

    def __init__(self):
        indices = self.indices()

    def info(self):
        return {"version": {"number": [7]}}


@pytest.mark.unit
@pytest.mark.helper
def test_ES_wrapper_get_mapping_props():
    es6 = elasticsearch6_fake()
    assert helper.ES_wrapper.get_mapping_props(es6, index="test", doc_type="old_doc_type") == "some"
    with pytest.raises(KeyError):
        assert helper.ES_wrapper.get_mapping_props(es6, index="test", doc_type="false_doc_type")
    with pytest.raises(KeyError):
        assert helper.ES_wrapper.get_mapping_props(es6, index="test")

    es7 = elasticsearch7_fake()
    assert helper.ES_wrapper.get_mapping_props(es7, index="test", doc_type="old_doc_type") == "someother"
    assert helper.ES_wrapper.get_mapping_props(es7, index="test") == "someother"


if __name__ == "__main__":
    pytest.main()
