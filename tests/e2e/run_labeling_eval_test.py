#!/usr/bin/env python3
"""Run a few decisions through the codelist-matching/evaluation job and check the labeling output."""
import sys
from datetime import datetime, timezone

from common import (
    JOBS_GRAPH, LAST_MIGRATION, PUBLIC_GRAPH, STATUS, TASK_OP, ask, new_resource,
    print_errors, run_guarded, sparql, summary, wait_for_migrations, wait_for_pipeline)

JOB_OP = "http://lblod.data.gift/id/jobs/concept/JobOperation/codelist-matching/evaluation"
LABELING_OP = TASK_OP + "codelist-matching/"
CHAIN = [
    "singleton-job",
    "codelist-matching/evaluation-split-tasks",
    "codelist-matching/annotate",
    "codelist-matching/assess-impact",
]
JOB_CREATOR_SELF_SERVICE = "http://lblod.data.gift/services/job-self-service"

CODELIST = "http://data.lblod.gift/id/conceptscheme/sdg-simple"
CODELIST_GRAPH = "http://mu.semte.ch/graphs/public"
NO_MATCH = "http://mu.semte.ch/vocabularies/ext/no-match-found"
IMPACT_PREFIX = "http://mu.semte.ch/vocabularies/ext/impact/"

# The first two clearly relate to a Sustainable Development Goal; the third does not.
DECISIONS = {
    "solar": "The municipal council decides to install solar panels on the roofs of the sports hall "
             "and the library. The panels will cover part of the electricity use of these buildings "
             "with renewable energy and lower the energy costs of the municipality.",
    "meals": "The municipal council approves a subsidy so that children from low-income families "
             "receive a free warm meal every school day in the municipal primary schools.",
    "minutes": "The municipal council approves the minutes of the previous meeting and renames the "
               "committee for general affairs to the committee for internal affairs.",
}
MATCHABLE = ["solar", "meals"]


def seed():
    job, job_id = new_resource("http://redpencil.data.gift/id/job/")
    first_task, task_id = new_resource("http://redpencil.data.gift/id/task/")
    container, container_id = new_resource("http://redpencil.data.gift/id/dataContainers/")
    now = f'"{datetime.now(timezone.utc).isoformat()}"^^xsd:dateTime'
    expressions = {}
    decisions = []
    for key, text in DECISIONS.items():
        expression, expression_id = new_resource("http://data.lblod.info/id/expressions/")
        work, work_id = new_resource("http://data.lblod.info/id/works/")
        expressions[key] = expression
        decisions.append(f"""
    <{work}> a eli:Work ;
      mu:uuid "{work_id}" ;
      eli:is_realized_by <{expression}> .
    <{expression}> a eli:Expression ;
      mu:uuid "{expression_id}" ;
      epvoc:expressionContent "{text}" .""")
    resources = ", ".join(f"<{expression}>" for expression in expressions.values())
    sparql(f"""
INSERT DATA {{
  GRAPH <{PUBLIC_GRAPH}> {{{"".join(decisions)}
  }}
  GRAPH <{JOBS_GRAPH}> {{
    <{job}> a cogs:Job ;
      mu:uuid "{job_id}" ;
      dct:created {now} ;
      dct:modified {now} ;
      dct:creator <{JOB_CREATOR_SELF_SERVICE}> ;
      adms:status <{STATUS}busy> ;
      task:operation <{JOB_OP}> ;
      ext:codelist <{CODELIST}> .
    <{container}> a nfo:DataContainer ;
      mu:uuid "{container_id}" ;
      task:hasResource {resources} .
    <{first_task}> a task:Task ;
      mu:uuid "{task_id}" ;
      dct:created {now} ;
      dct:modified {now} ;
      adms:status <{STATUS}scheduled> ;
      task:operation <{TASK_OP}singleton-job> ;
      task:index "0" ;
      task:inputContainer <{container}> ;
      dct:isPartOf <{job}> .
  }}
}}""", update=True)
    return job, expressions


def pipeline_state(rows):
    statuses = [row["status"] for row in rows]
    if STATUS + "failed" in statuses:
        return "failed"
    annotating = sum(row["op"] == LABELING_OP + "annotate" for row in rows)
    assessing = sum(row["op"] == LABELING_OP + "assess-impact" for row in rows)
    # One annotate task per split batch; done when each batch reached impact assessment.
    if annotating and assessing == annotating and all(s == STATUS + "success" for s in statuses):
        return "done"
    return "running"


def classifying(expression, body_condition):
    return f"""
GRAPH <{PUBLIC_GRAPH}> {{
  ?annotation a oa:Annotation ;
    oa:motivatedBy oa:classifying ;
    oa:hasTarget <{expression}> ;
    oa:hasBody ?body .
}}
FILTER({body_condition})"""


def in_codelist(expression):
    return f"""
GRAPH <{PUBLIC_GRAPH}> {{
  ?annotation a oa:Annotation ;
    oa:motivatedBy oa:classifying ;
    oa:hasTarget <{expression}> ;
    oa:hasBody ?body .
}}
GRAPH <{CODELIST_GRAPH}> {{ ?body skos:inScheme <{CODELIST}> . }}"""


def checks(rows, expressions):
    matchable = [expressions[key] for key in MATCHABLE]
    any_matched = " UNION ".join(f"{{ {in_codelist(expression)} }}" for expression in matchable)
    return {
        "every step of the chain ran": all(
            any(row["op"] == TASK_OP + step for row in rows) for step in CHAIN),
        **{f"annotate labeled the {key} decision (a concept or no-match)": ask(
            classifying(expression, f'?body = <{NO_MATCH}> || EXISTS {{ '
                                    f'GRAPH <{CODELIST_GRAPH}> {{ ?body skos:inScheme <{CODELIST}> }} }}'))
           for key, expression in expressions.items()},
        "annotate matched a codelist concept for at least one SDG-related decision": ask(any_matched),
        "assess-impact added an impact to a matched annotation": ask(f"""
GRAPH <{PUBLIC_GRAPH}> {{
  ?annotation a oa:Annotation ;
    oa:motivatedBy oa:classifying ;
    oa:hasTarget ?expression ;
    oa:hasBody ?impact .
}}
FILTER(STRSTARTS(STR(?impact), "{IMPACT_PREFIX}"))"""),
    }


def run_test():
    print(f"Waiting for migrations to finish, up to {LAST_MIGRATION}")
    if not wait_for_migrations():
        print(f"FAIL: migrations did not reach {LAST_MIGRATION} before the migrations timeout")
        return 1

    job, expressions = seed()
    print(f"Seeded job {job} with {len(expressions)} decisions")

    state, rows = wait_for_pipeline(job, pipeline_state)
    if state != "done":
        print(f"FAIL: pipeline ended as {state}: {summary(rows)}")
        print_errors(job)
        return 1

    failed = False
    for name, passed in checks(rows, expressions).items():
        print(f"{'PASS' if passed else 'FAIL'}: {name}")
        failed = failed or not passed
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(run_guarded(run_test))
