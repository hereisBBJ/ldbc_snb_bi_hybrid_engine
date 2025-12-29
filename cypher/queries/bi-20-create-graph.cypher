MATCH (personA:Person)
OPTIONAL MATCH
      (personA:Person)-[:KNOWS]-(personB:Person),
      (personA)-[saA:STUDY_AT]->(u:University)<-[saB:STUDY_AT]-(personB)
    WITH
      min(abs(saA.classYear - saB.classYear)) + 1 AS weight,personA, personB
WITH gds.graph.project(
  'bi20',
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
