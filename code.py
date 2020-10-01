# pip install neo4j-driver

from neo4j import GraphDatabase, basic_auth

driver = GraphDatabase.driver(
    "bolt://52.86.81.2:33022", 
    auth=basic_auth("neo4j", "combustion-nameplates-buzzes"))
session = driver.session()

# cypher_query = '''
# MATCH (n)
# RETURN id(n) AS id
# LIMIT 10
# '''


cypher_query = """
USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/jgarciab/neo4j_test/main/data_test.csv' AS row
MERGE (c:Company {companyId: row.bvd_id, companyName: row.company_name})
MERGE (d:Director {directorId: row.dir_id})
MERGE (a:Address {add: row.company_adddress})
MERGE (c)-[ra:REGISTERED_AT]->(a)
MERGE (d)-[rd:DIRECTOR_OF]->(c)
"""

session.run(cypher_query)

for q in ["CREATE INDEX ON :Company(companyName)",]
cypher_query2 = """

"""



