import os
from py2neo import Graph

class Neo4jConfig:
    BOLT = 'bolt://25.29.164.152:6687'
    HOST = os.environ.get("NEO4J_HOST") or "25.29.164.152"
    BOLT_PORT = os.environ.get("NEO4J_PORT") or 6687
    HTTP_PORT = os.environ.get("NEO4J_PORT") or 7474
    NEO4J_USERNAME = os.environ.get("NEO4J_USERNAME") or "neo4j_services_db"
    NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD") or "klg_pass"
