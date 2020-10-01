# pip install neo4j-driver

from neo4j import GraphDatabase, basic_auth

driver = GraphDatabase.driver(
    "bolt://100.26.250.215:33019", 
    auth=basic_auth("neo4j", "radius-kits-currencies"))
session = driver.session()

# driver = GraphDatabase.driver(
#     "bolt://localhost:7687", 
#     auth=basic_auth("neo4j", "Dotted-Dragging-Moustache9"))
# session = driver.session()


session.run("MATCH (n) DETACH DELETE n")


cypher_query = """
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/jgarciab/neo4j_test/main/n_dirs.csv' AS row
CREATE (d:Director {directorId: row.dir_id, directorName: row.dir_name, RTO:row.rto_level})
"""

session.run(cypher_query)

print("Query 1 completed")

cypher_query = """
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/jgarciab/neo4j_test/main/n_comps.csv' AS row
CREATE (c:Company {companyId: row.bvd_id, companyName: row.company_name, nace: row.nace, legal:row.legal})
"""

session.run(cypher_query)

print("Query 2 completed")

cypher_query = """
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/jgarciab/neo4j_test/main/n_adds.csv' AS row
CREATE (a:Address {addId: row.company_adddress, addName:row.add_full, perc_RTO:toInteger(row.add_off)})
"""

session.run(cypher_query)

print("Query 3 completed")

cypher_query = """
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/jgarciab/neo4j_test/main/n_guos.csv' AS row
CREATE (g:GUO {guoId: row.guo})
"""

session.run(cypher_query)

print("Query 4 completed")



for q in ["CREATE INDEX ON :Company(companyId)",
			"CREATE INDEX ON :GUO(guoId)",
			"CREATE INDEX ON :Director(directorId)",
			"CREATE INDEX ON :Address(addId)"]:
	session.run(q)

print("Index completed")

cypher_query = """
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/jgarciab/neo4j_test/main/r_dirs.csv' AS row
MATCH (c:Company {companyId: row.bvd_id})
WITH c,row
MATCH (d:Director {directorId: row.dir_id})
CREATE (d)-[rd:DIRECTOR_OF]->(c)
"""

session.run(cypher_query)

print("Query 5 completed")

cypher_query = """
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/jgarciab/neo4j_test/main/r_adds.csv' AS row
MATCH (c:Company {companyId: row.bvd_id})
with c,row
MATCH (a:Address {addId: row.company_adddress})
CREATE (c)-[ra:REGISTERED_AT]->(a)
"""

session.run(cypher_query)

print("Query 6 completed")

cypher_query = """
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/jgarciab/neo4j_test/main/r_guos.csv' AS row
MATCH (c:Company {companyId: row.bvd_id})
with c,row
MATCH (g:GUO {guoId: row.guo})
CREATE (c)-[rg:OWNED_BY]->(g)
"""

session.run(cypher_query)

print("Query 7 completed")

# cypher_query = """
# USING PERIODIC COMMIT 1000
# LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/jgarciab/neo4j_test/main/data_test.csv' AS row
# MERGE (c:Company {companyId: row.bvd_id, companyName: row.company_name, nace: row.nace, legal:row.legal})
# MERGE (d:Director {directorId: row.dir_id, directorName: row.dir_name, RTO:row.rto_level})
# MERGE (a:Address {add: row.company_adddress, addName:row.add_full, addRTOscore:row.add_off})
# MERGE (c)-[ra:REGISTERED_AT]->(a)
# MERGE (d)-[rd:DIRECTOR_OF]->(c)

# WITH row
# WHERE row.guo <> ""
# MERGE (g:GUO {guoId: row.guo})
# MERGE (c)-[rg:OWNED_BY]->(g)

# """

# session.run(cypher_query)

for q in ["CREATE INDEX ON :Company(companyName)","CREATE INDEX ON :Director(directorName)","CREATE INDEX ON :Address(add_full)"]:
	session.run(q)



print("QIndex completed")

#C009936572

