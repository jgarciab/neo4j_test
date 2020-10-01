# pip install neo4j-driver

from neo4j import GraphDatabase, basic_auth

driver = GraphDatabase.driver(
    "bolt://52.86.81.2:33022", 
    auth=basic_auth("neo4j", "combustion-nameplates-buzzes"))
session = driver.session()

cypher_query = '''
MATCH (n)
RETURN id(n) AS id
LIMIT 10
'''