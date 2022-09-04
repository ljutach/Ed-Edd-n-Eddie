from impl import GenericQueryProcessor, RelationalDataProcessor, RelationalQueryProcessor, TriplestoreQueryProcessor, TriplestoreDataProcessor
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from extraclassesandfunctions import AddToSparqlStore,  CleanSparqlStore
from extraclassesandfunctions import CleanRelationaldatabase

jsngraph = "./graph_db/graph_other_data.json"
csvgraph = "./graph_db/graph_publications.csv"

jsnrel = "./relational_db/relational_other_data.json"
csvrel  = "./relational_db/relational_publication.csv"
dbpath = "publications.db"


#RELATIONAL DATABASE STEPS 
cleanRelational = CleanRelationaldatabase(dbpath)
obj = RelationalDataProcessor() 
obj.setDbPath(dbpath) 
obj.uploadData(jsnrel)
obj.uploadData(csvrel)
rqp = RelationalQueryProcessor()
rqp.setDbPath(dbpath)

"""
#GRAPH DATABASE STEPS 
endpointUrl = 'http://127.0.0.1:9999/blazegraph/sparql'
cleansparql = CleanSparqlStore(endpointUrl)
obj = TriplestoreDataProcessor()
obj.setEndpointUrl(endpointUrl)
obj.uploadData(jsngraph)
obj.uploadData(csvgraph)
tqp = TriplestoreQueryProcessor()
tqp.setEndpointUrl(endpointUrl)
"""

"""
#GENERIC DATABASE STEP 

gqp = GenericQueryProcessor() 
gqp.addQueryProcessor(rqp)
gqp.addQueryProcessor(tqp)
"""
