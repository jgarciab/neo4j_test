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
USING PERIODIC COMMIT 5000
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/jgarciab/neo4j_test/main/data_test.csv' AS row
MERGE (c:Company {companyId: row.bvd_id, companyName: row.company_name, nace: row.nace, legal:row.legal})
MERGE (d:Director {directorId: row.dir_id, directorName: row.dir_name, RTO:row.rto_level})
MERGE (a:Address {add: row.company_adddress, addName:add_full, addRTOscore:add_off})
MERGE (c)-[ra:REGISTERED_AT]->(a)
MERGE (d)-[rd:DIRECTOR_OF]->(c)

WITH row
WHERE row.guo <> ""
MERGE (g:GUO {add: row.guo})
MERGE (c)-[rg:OWNED_BY]->(g)

"""

session.run(cypher_query)

for q in ["CREATE INDEX ON :Company(companyName)","CREATE INDEX ON :Director(directorName)","CREATE INDEX ON :Address(add_full)"]:
	session.run(q)




