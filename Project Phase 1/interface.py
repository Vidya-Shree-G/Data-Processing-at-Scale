from neo4j import GraphDatabase

class Interface:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self._driver.verify_connectivity()

    def close(self):
        self._driver.close()

    def bfs(self, start_node, last_node):
        with self._driver.session() as session: 
            session.run("CALL gds.graph.project('locGraph', 'Location', 'TRIP', {relationshipProperties: 'distance'})")

            query = """
                MATCH (a:Location {name: $start_node}), (d:Location {name: $last_node})
                WITH id(a) AS source, [id(d)] AS targetNodes
                CALL gds.bfs.stream('locGraph', {sourceNode: source, targetNodes: targetNodes})
                YIELD path 
                RETURN path
            """
            result = session.run(query, start_node=start_node, last_node=last_node)
            bfs_result = result.data() 

            session.run("CALL gds.graph.drop('locGraph') YIELD graphName;")

        return bfs_result

    def pagerank(self, max_iterations, weight_property):
        with self._driver.session() as session: 
            session.run("CALL gds.graph.project('locGraph', 'Location', 'TRIP', {relationshipProperties: 'distance'})")

            query = f"""
                CALL gds.pageRank.stream('locGraph', {{
                    maxIterations: {max_iterations}, 
                    dampingFactor: 0.85, 
                    relationshipWeightProperty: '{weight_property}'
                }})
                YIELD nodeId, score
                RETURN gds.util.asNode(nodeId).name AS name, score
                ORDER BY score DESC, name ASC
            """
            result = session.run(query)
            page_rank_results = result.data()

            session.run("CALL gds.graph.drop('locGraph') YIELD graphName;")

        if page_rank_results: 
            max_node = page_rank_results[0]
            min_node = page_rank_results[-1]
            return max_node, min_node
        else: 
            return None, None

