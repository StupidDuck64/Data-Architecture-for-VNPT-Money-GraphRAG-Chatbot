// ============================================================
// VNPT Money Platform – Neo4j Seed from shared_data/ CSVs
// ============================================================

// Constraints
CREATE CONSTRAINT group_id   IF NOT EXISTS FOR (g:Group)   REQUIRE g.id IS UNIQUE;
CREATE CONSTRAINT topic_id   IF NOT EXISTS FOR (t:Topic)   REQUIRE t.id IS UNIQUE;
CREATE CONSTRAINT problem_id IF NOT EXISTS FOR (p:Problem) REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT answer_id  IF NOT EXISTS FOR (a:Answer)  REQUIRE a.id IS UNIQUE;

// Indexes
CREATE INDEX problem_status IF NOT EXISTS FOR (p:Problem) ON (p.status);
CREATE INDEX problem_intent IF NOT EXISTS FOR (p:Problem) ON (p.intent);
CREATE FULLTEXT INDEX problem_text IF NOT EXISTS FOR (p:Problem) ON EACH [p.title, p.keywords];
CREATE FULLTEXT INDEX answer_text  IF NOT EXISTS FOR (a:Answer)  ON EACH [a.summary, a.content];

// Load Groups
LOAD CSV WITH HEADERS FROM 'file:///nodes_group.csv' AS row
MERGE (g:Group {id: row.id})
SET g.name        = row.name,
    g.description = coalesce(row.description, ''),
    g.order       = toInteger(coalesce(row.order, '0'));

// Load Topics
LOAD CSV WITH HEADERS FROM 'file:///nodes_topic.csv' AS row
MERGE (t:Topic {id: row.id})
SET t.name     = row.name,
    t.group_id = row.group_id,
    t.keywords = coalesce(row.keywords, ''),
    t.order    = toInteger(coalesce(row.order, '0'));

// Load Problems
LOAD CSV WITH HEADERS FROM 'file:///nodes_problem.csv' AS row
MERGE (p:Problem {id: row.id})
SET p.title            = row.title,
    p.description      = coalesce(row.description, ''),
    p.intent           = coalesce(row.intent, ''),
    p.keywords         = coalesce(row.keywords, ''),
    p.sample_questions = coalesce(row.sample_questions, ''),
    p.status           = coalesce(row.status, 'active');

// Load Problem Supplements
LOAD CSV WITH HEADERS FROM 'file:///nodes_problem_supplement.csv' AS row
MERGE (p:Problem {id: row.id})
SET p.title            = row.title,
    p.description      = coalesce(row.description, ''),
    p.intent           = coalesce(row.intent, ''),
    p.keywords         = coalesce(row.keywords, ''),
    p.sample_questions = coalesce(row.sample_questions, ''),
    p.status           = coalesce(row.status, 'active');

// Load Answers
LOAD CSV WITH HEADERS FROM 'file:///nodes_answer.csv' AS row
MERGE (a:Answer {id: row.id})
SET a.summary = coalesce(row.summary, ''),
    a.content = coalesce(row.content, ''),
    a.steps   = coalesce(row.steps, ''),
    a.notes   = coalesce(row.notes, ''),
    a.status  = coalesce(row.status, 'active');

// Load Answer Supplements
LOAD CSV WITH HEADERS FROM 'file:///nodes_answer_supplement.csv' AS row
MERGE (a:Answer {id: row.id})
SET a.summary = coalesce(row.summary, ''),
    a.content = coalesce(row.content, ''),
    a.steps   = coalesce(row.steps, ''),
    a.notes   = coalesce(row.notes, ''),
    a.status  = coalesce(row.status, 'active');

// Load HAS_TOPIC relationships
LOAD CSV WITH HEADERS FROM 'file:///rels_has_topic.csv' AS row
MATCH (g:Group {id: row.group_id})
MATCH (t:Topic {id: row.topic_id})
MERGE (g)-[:HAS_TOPIC]->(t);

// Load HAS_PROBLEM relationships
LOAD CSV WITH HEADERS FROM 'file:///rels_has_problem.csv' AS row
MATCH (t:Topic   {id: row.topic_id})
MATCH (p:Problem {id: row.problem_id})
MERGE (t)-[:HAS_PROBLEM]->(p);

// Load HAS_ANSWER relationships
LOAD CSV WITH HEADERS FROM 'file:///rels_has_answer.csv' AS row
MATCH (p:Problem {id: row.problem_id})
MATCH (a:Answer  {id: row.answer_id})
MERGE (p)-[:HAS_ANSWER]->(a);

// Load HAS_ANSWER supplement relationships
LOAD CSV WITH HEADERS FROM 'file:///rels_has_answer_supplement.csv' AS row
MATCH (p:Problem {id: row.problem_id})
MATCH (a:Answer  {id: row.answer_id})
MERGE (p)-[:HAS_ANSWER]->(a);
