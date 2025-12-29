MATCH (personA:Person)
OPTIONAL MATCH
      (personA:Person)-[:KNOWS]-(personB:Person),
      (personA)<-[:HAS_CREATOR]-(:Message)-[replyOf:REPLY_OF]-(:Message)-[:HAS_CREATOR]->(personB)
    WITH
      count(replyOf) AS numInteractions,personA, personB
    WITH
      CASE WHEN round(40-sqrt(numInteractions)) > 1 THEN round(40-sqrt(numInteractions)) ELSE 1 END AS weight,personA, personB
WITH gds.graph.project(
  'bi19',
  personA,
  personB,
  {
    sourceNodeLabels: 'Person',
    targetNodeLabels: 'Person',
    relationshipProperties: { weight: weight}
  }
) AS g
RETURN
  g.graphName AS graph, g.nodeCount AS nodes, g.relationshipCount AS rels
