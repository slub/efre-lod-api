from flask import Response

# to register this mediatype also to the swagger frontend
# you would have to add the annotation to the processing
# function: e.g.
#
# @api.representation("text/html")
def data_to_preview(self, data, request):
    """ Takes `data` as a dictionary and generates a html
        preview with the most important values out off `data`
        deciding on its entity type. 
        The preview contains:
          - The ID of the dataset [`@id`]
          - The name or title of the dataset [`name`]/[`dct:title`]
          - The entity type of the dataset [`@type`]/[`rdfs:ch_type`]
        as well as additional information in the `free_content`-field, e.g.
          - `birthDate` if the type is a person
    """
    elem = data[0]
    
    _id=elem.get("@id")
    endpoint = _id.split("/")[-2] + "/" + _id.split("/")[-1]
    
    if "name" in elem:
        title = elem.get("name")
    else:
        title = elem.get("dct:title")
    
    if elem.get("@type"):
        typ = elem.get("@type")
    elif elem.get("rdfs:ch_type"):
        typ=elem.get("rdfs:ch_type")["@id"]
    
    free_content = ""
    
    if typ == "http://schema.org/Person":
        free_content = elem.get("birthDate")
    elif typ == "http://schema.org/CreativeWork" or typ.startswith("bibo"):
        if "author" in elem:
            free_content = elem.get("author")[0]["name"]
        elif not "author" in elem and "contributor" in elem:
            free_content = elem.get("contributor")[0]["name"]
        elif "bf:contribution" in elem:
            free_content = elem.get("bf:contribution")[0]["bf:agent"]["rdfs:ch_label"]
    elif typ == "http://schema.org/Place":
        free_content = elem.get("adressRegion")
    elif typ == "http://schema.org/Organization":
        free_content = elem.get("location").get("name")
    html = """<html><head><meta charset=\"utf-8\" /></head>
              <body style=\"margin: 0px; font-family: Arial; sans-serif\">
              <div style=\"height: 100px; width: 320px; overflow: hidden; font-size: 0.7em\">
                  <div style=\"margin-left: 5px;\">
                      <a href=\"{id}\" target=\"_blank\" style=\"text-decoration: none;\">{title}</a>
                      <span style=\"color: #505050;\">({endpoint})</span>
                      <p>{content}</p>
                      <p>{typ}</p>
                  </div>
              </div>
              </body>
              </html>
           """.format(id=_id, title=title, endpoint=endpoint, content=free_content, typ=typ)
    response = Response(html ,mimetype='text/html; charset=UTF-8')
    
    # send the Response through _encode() fo the the Output class to 
    # be enable gzip-compression if defined in the request header
    return self._encode(request, response)

