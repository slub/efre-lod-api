from flask import Blueprint

swagger_ui = Blueprint("swagger_ui", __name__,
                       static_folder='doc',
                       template_folder='templates')
