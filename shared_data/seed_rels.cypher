// Load relationships using correct column names (start_id, end_id)

LOAD CSV WITH HEADERS FROM 'file:///rels_has_topic.csv' AS row
MATCH (g:Group {id: row.start_id})
MATCH (t:Topic {id: row.end_id})
MERGE (g)-[:HAS_TOPIC]->(t);

LOAD CSV WITH HEADERS FROM 'file:///rels_has_problem.csv' AS row
MATCH (t:Topic   {id: row.start_id})
MATCH (p:Problem {id: row.end_id})
MERGE (t)-[:HAS_PROBLEM]->(p);

LOAD CSV WITH HEADERS FROM 'file:///rels_has_answer.csv' AS row
MATCH (p:Problem {id: row.start_id})
MATCH (a:Answer  {id: row.end_id})
MERGE (p)-[:HAS_ANSWER]->(a);

LOAD CSV WITH HEADERS FROM 'file:///rels_has_answer_supplement.csv' AS row
MATCH (p:Problem {id: row.start_id})
MATCH (a:Answer  {id: row.end_id})
MERGE (p)-[:HAS_ANSWER]->(a);
