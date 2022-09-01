import json
from logging import raiseExceptions
import pandas as pd
from json import load
from pandas import DataFrame
import os.path
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

class DataCSV(object): 
    def __init__(self, csv):

        if os.path.exists(csv):
            PublicationsDF = pd.read_csv(csv , keep_default_na= False,
                        dtype={
                                    "id": "string",
                                    "title": "string",
                                    "type": "string",
                                    "publication_year": "string",
                                    "issue": "string",
                                    "volume": "string",
                                    "chapter": "string",
                                    "publication_venue": "string",
                                    "venue_type": "string",
                                    "publisher": "string",
                                    "event": "string"
                        },encoding="utf-8")\
                .rename(columns={"publication_year" : "publicationYear"})


            not_unq = PublicationsDF.filter(items=['publication_venue', 'venue_type', 'publisher'])\
            .drop_duplicates()\
            .groupby("publication_venue")\
            .count()\
            .query('venue_type >1 or publisher >1')
            if len(not_unq) > 0:
                print(" *** WARNING - 'venue_type' or 'publisher' are not unique for each 'publication_venue'")
                print(not_unq)
            else:
                print(" --- OK: 'publication_venue's have unique 'venue_type's and 'publisher'")

            # DATAFRAME FROM CSV

            #VENUE DATAFRAME FROM CSV
            VenueDF =  PublicationsDF[["publication_venue", 'venue_type', 'publisher']]\
                .drop_duplicates()\
                .dropna()
            VenueDF.insert(0, 'id', range(0, VenueDF.shape[0]))
            VenueDF['id'] = VenueDF['id'].apply(lambda x: 'venue-' + str(int(x)))
            self.Venue_DF = VenueDF.rename(columns={"publication_venue" : "venueName", "publisher" : "publisherId"})

            #PUBLICATION DATAFRAME 
            
            Publication_df = pd.merge(
            PublicationsDF[["id", "publicationYear", "title", "type", "event", "publication_venue"]],
            self.Venue_DF.rename(columns={"id" : "publicationVenueId", "venueName" : "publication_venue"}), 
            on="publication_venue")\
            .drop(columns=["publication_venue", "publisherId"])
            self.Publications_DF = Publication_df[["id", "title", "type", "publicationYear", "venue_type", "publicationVenueId"]]

            #BOOK CHAPTER DATAFRAME
            book_chapter_df = PublicationsDF.query("type == 'book-chapter'")
            self.Book_chapter_DF = book_chapter_df[["id", "chapter"]]
            
            #JOURNAL ARTICLE DATAFRAME
            journal_article_df = PublicationsDF.query("type == 'journal-article'")
            self.Journal_article_DF = journal_article_df[["id", "issue", "volume"]]

            #PROCEEDINGS PAPER DATAFRAME 
            proceedings_paper_df = PublicationsDF.query("type == 'proceeding-paper'")
            self.Proceedings_paper_DF = proceedings_paper_df[["id"]]
            
            #BOOK DATAFRAME
            book_df = self.Publications_DF.query("venue_type == 'book'")
            book_df = book_df[["publicationVenueId"]]
            self.Book_DF = book_df.rename(columns={"publicationVenueId":"bookId"}).drop_duplicates(subset=["bookId"])

            #JOURNAL DATAFRAME
            journal_df = self.Publications_DF.query("venue_type == 'journal'")
            journal_df= journal_df[["publicationVenueId"]]
            self.Journal_DF = journal_df.rename(columns={"publicationVenueId":"journalId"}).drop_duplicates(subset=["journalId"])
           
            #PROCEEDINGS DATAFRAME
            proceedings_df= Publication_df.query("venue_type == 'proceedings'")
            proceedings_df = proceedings_df[["publicationVenueId", "event"]]
            self.Proceedings_DF = proceedings_df.rename(columns={"publicationVenueId":"proceedingId"}).drop_duplicates(subset=["proceedingId"])

        else:
            raiseExceptions("CSV file '" + csv + "' does not exist!")

    
    
class DataJSON(object):
    def __init__(self, jsn):

        if os.path.exists(jsn):
        # read JSON 
            with open(jsn, "r", encoding="utf-8") as f:
                json_doc = load(f)

            # DATAFRAME FROM JSON 
        
            #VENUE DATAFRAME
            venues_df = json_doc["venues_id"]
            self.VenuesId_DF = pd.DataFrame(venues_df.items(), columns=['doi', 'issn_isbn']).explode('issn_isbn')
        
            #AUTHOR DATAFRAME
            author = json_doc["authors"]
            author_df=DataFrame(author.items(),columns=['doi','author']).explode('author')
            author_df = pd.json_normalize(json.loads(author_df.to_json(orient="records")))
            author_df.rename(columns={"author.family":"family_name","author.given":"given_name","author.orcid":"orc_id"}, inplace = True)
            author_df.drop("family_name", axis=1, inplace = True)
            author_df.drop("given_name", axis =1, inplace = True)
            self.Author_DF = author_df
           
            #CITES DATAFRAME
            References = json_doc["references"]
            cites_df=pd.DataFrame(References.items(),columns=['citing','cited']).explode('cited')
            cites_df=pd.json_normalize(json.loads(cites_df.to_json(orient="records")))
            cites_df.rename(columns={"References.keys()":"citing","References.values()":"cited"}, inplace = True)
            self.Cites_DF = cites_df   
            

            #PERSON DATAFRAME 
            author = json_doc["authors"]
            person_df=pd.DataFrame(author.items(),columns=['doi','author']).explode('author')
            person_df=pd.json_normalize(json.loads(person_df.to_json(orient="records")))
            person_df.rename(columns={"author.family":"family_name","author.given":"given_name","author.orcid":"orc_id"}, inplace = True)
            person_df.drop("doi", axis =1, inplace = True)
            self.Person_DF = person_df.drop_duplicates(subset = ["orc_id"])
            

            #ORGANIZATION DATAFRAME 
            crossref = json_doc["publishers"]
            id_and_name = crossref.values()
            organization_df = pd.DataFrame(id_and_name)
            self.Organization_DF = organization_df 

        else:
            raiseExceptions("JSON file '" + jsn + "' does not exist!")

def getStringOfPythonObject(gp):
        result=[]
        for el in gp:
            result.append(el.__str__())
        return result  

def CleanSparqlStore(endpoint):
    store = SPARQLUpdateStore()
    store.open((endpoint, endpoint))
    store.remove((None, None, None), context=None)
    store.close()
    print("*** Store at: '" + endpoint + "' has been cleaned!")

def CleanRelationaldatabase(dbpath):
    if os.path.exists(dbpath):
        os.remove(dbpath)
    print("Database at: '" + dbpath + "' has been cleaned!")

def AddToSparqlStore(endpoint, graph):
    store = SPARQLUpdateStore()
    # The URL of the SPARQL endpoint is the same URL of the Blazegraph
    # instance + '/sparql'
    # endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'
    # It opens the connection with the SPARQL endpoint instance
    store.open((endpoint, endpoint))
    for triple in graph.triples((None, None, None)):
        store.add(triple)
    # Once finished, remeber to close the connection
    store.close()        