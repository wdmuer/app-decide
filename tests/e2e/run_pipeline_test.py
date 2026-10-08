#!/usr/bin/env python3
"""Run one PDF through the pdf-to-enriched job and check the pipeline output."""
import sys
from datetime import datetime, timezone

from common import (
    JOBS_GRAPH, LAST_MIGRATION, PUBLIC_GRAPH, STATUS, TASK_OP, ask, job_tasks, new_resource,
    print_errors, run_guarded, sparql, summary, wait_for_migrations, wait_for_pipeline)

PDF_URL = "https://www.horebeke.be/src/Frontend/Files/userfiles/files/Notulen%20ocmw%20raad%2024_11_2025.pdf"
MUNICIPALITY = "http://data.lblod.info/id/bestuurseenheden/69af2347b1d0a9a39dbcf0ea4d94b9eaa15edb0c7d2dba56bbd9c39ab1c80e9f"
EXPECTED_LOCATION = "Kerkplein 1"
JOB_CREATOR_SELF_SERVICE = "http://lblod.data.gift/services/job-self-service"

JOB_OP = "http://lblod.data.gift/id/jobs/concept/JobOperation/harvesting/pdf-to-enriched"
CHAIN = [
    "singleton-job",
    "pdf-scraping",
    "pdf-to-eli",
    "split-task-pdf-to-enriched-translating",
    "translating",
    "segmenting",
    "entity-extracting",
    "named-entity-linking",
]


def seed():
    job, job_id = new_resource("http://redpencil.data.gift/id/job/")
    first_task, task_id = new_resource("http://redpencil.data.gift/id/task/")
    remote, remote_id = new_resource("http://lblod.data.gift/id/remote-data-objects/")
    collection, collection_id = new_resource("http://data.lblod.info/id/harvesting-collection/")
    pdf_container, pdf_container_id = new_resource("http://redpencil.data.gift/id/dataContainers/")
    municipality_container, municipality_container_id = new_resource("http://redpencil.data.gift/id/dataContainers/")
    now = f'"{datetime.now(timezone.utc).isoformat()}"^^xsd:dateTime'
    sparql(f"""
INSERT DATA {{
  GRAPH <{JOBS_GRAPH}> {{
    <{job}> a cogs:Job ;
      mu:uuid "{job_id}" ;
      dct:created {now} ;
      dct:modified {now} ;
      dct:creator <{JOB_CREATOR_SELF_SERVICE}> ;
      adms:status <{STATUS}busy> ;
      task:operation <{JOB_OP}> ;
      ext:splitDecisions true .
    <{remote}> a nfo:RemoteDataObject ;
      mu:uuid "{remote_id}" ;
      nie:url <{PDF_URL}> .
    <{collection}> a hrvst:HarvestingCollection ;
      mu:uuid "{collection_id}" ;
      dct:hasPart <{remote}> .
    <{pdf_container}> a nfo:DataContainer ;
      mu:uuid "{pdf_container_id}" ;
      task:hasHarvestingCollection <{collection}> .
    <{municipality_container}> a nfo:DataContainer ;
      mu:uuid "{municipality_container_id}" ;
      task:hasResource <{MUNICIPALITY}> .
    <{first_task}> a task:Task ;
      mu:uuid "{task_id}" ;
      dct:created {now} ;
      dct:modified {now} ;
      adms:status <{STATUS}scheduled> ;
      task:operation <{TASK_OP}singleton-job> ;
      task:index "0" ;
      task:inputContainer <{pdf_container}>, <{municipality_container}> ;
      dct:isPartOf <{job}> .
  }}
}}""", update=True)
    return job


def pipeline_state(rows):
    statuses = [row["status"] for row in rows]
    if STATUS + "failed" in statuses:
        return "failed"
    translating = sum(row["op"] == TASK_OP + "translating" for row in rows)
    linking = sum(row["op"] == TASK_OP + "named-entity-linking" for row in rows)
    # One translating task per decision; done when each chain reached linking.
    if translating and linking == translating and all(s == STATUS + "success" for s in statuses):
        return "done"
    return "running"


def annotation_by(job, operation, condition):
    return f"""
GRAPH <{JOBS_GRAPH}> {{ ?task dct:isPartOf <{job}> ; task:operation <{TASK_OP}{operation}> . }}
GRAPH <{PUBLIC_GRAPH}> {{
  ?task prov:generated ?annotation .
  ?annotation a oa:Annotation ; oa:hasBody ?statement .
  ?statement rdf:predicate ?predicate ; rdf:object ?object .
}}
FILTER({condition})"""


def checks(job, rows):
    return {
        "every step of the chain ran": all(
            any(row["op"] == TASK_OP + step for row in rows) for step in CHAIN),
        "pdf-to-eli created an expression with content": ask(f"""
GRAPH <{PUBLIC_GRAPH}> {{
  ?manifestation a eli:Manifestation ; eli:is_exemplified_by <{PDF_URL}> .
  ?expression a eli:Expression ; eli:is_embodied_by ?manifestation ; epvoc:expressionContent ?text .
}}
FILTER(STRLEN(STR(?text)) > 0)"""),
        "translating created a translated expression": ask(f"""
GRAPH <{PUBLIC_GRAPH}> {{
  ?manifestation eli:is_exemplified_by <{PDF_URL}> .
  ?expression eli:is_embodied_by ?manifestation ; gold:translation ?translated .
  ?translated epvoc:expressionContent ?text .
}}
FILTER(STRLEN(STR(?text)) > 0)"""),
        "segmenting wrote a segment annotation": ask(annotation_by(
            job, "segmenting", 'STRSTARTS(STR(?predicate), "http://mu.semte.ch/vocabularies/ext/")')),
        f"entity-extracting found the location {EXPECTED_LOCATION}": ask(annotation_by(
            job, "entity-extracting",
            f'?predicate = prov:atLocation && EXISTS {{ ?object rdfs:label ?label . '
            f'FILTER(STR(?label) = "{EXPECTED_LOCATION}") }}')),
    }


def run_test():
    print(f"Waiting for migrations to finish, up to {LAST_MIGRATION}")
    if not wait_for_migrations():
        print(f"FAIL: migrations did not reach {LAST_MIGRATION} before the migrations timeout")
        return 1

    job = seed()
    print(f"Seeded job {job}")

    state, rows = wait_for_pipeline(job, pipeline_state)
    if state != "done":
        print(f"FAIL: pipeline ended as {state}: {summary(rows)}")
        print_errors(job)
        return 1

    failed = False
    for name, passed in checks(job, rows).items():
        print(f"{'PASS' if passed else 'FAIL'}: {name}")
        failed = failed or not passed
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(run_guarded(run_test))
