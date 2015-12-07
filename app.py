from flask import Blueprint
main = Blueprint('main', __name__)
 
import json
from engine import RecommendationEngine
 
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
from flask import Flask, request
 
@main.route("/<int:user_id>/reco", methods=["GET"])
def movie_ratings(user_id):
    reco = recommendation_engine.get_recommendations(user_id)
    return json.dumps(reco)
 
def create_app(spark_context):
    global recommendation_engine 

    recommendation_engine = RecommendationEngine(spark_context)
    
    app = Flask(__name__)
    app.register_blueprint(main)
    return app 
