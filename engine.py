import os
from pyspark.sql import SQLContext

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RecommendationEngine:
    """A travel recommendation engine
    """
    def get_recommendations(self, user_id):
        """Recommends travel for user
        """
        data = (1,2,3,4,5)
        even_rdd = self.sc.parallelize(data)
        #ratings = even_rdd.collect()
        reco = self.sqlContext.sql("SELECT c.contact_id, o.prod_id  FROM contacts c , offres o WHERE  o.continent_offre = c.continent and o.envie_offre = c.envie and o.moyen_offre = c.moyen").collect()
        return reco

    def __init__(self, sc):
        """Init the recommendation engine given a Spark context and a dataset path
        """
        logger.info("Starting up the Recommendation Engine: ")
        self.sc = sc
        self.sqlContext = SQLContext(sc)

        path_contacts = "data_v3/contacts/attempt_contactV3_perfect_match.json"
        df_contacts = self.sqlContext.jsonFile(path_contacts)

        df_contacts.registerTempTable("contacts")

        path_offres = "data_v3/offres/attempt_productV3_perfect_match.json"
        df_offres = self.sqlContext.jsonFile(path_offres)
        df_offres.registerTempTable("offres")

