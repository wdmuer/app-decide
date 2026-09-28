#!/usr/bin/env python3
"""Run one PDF through the pdf-to-enriched job and check the pipeline output."""
import json
import os
import re
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

ENDPOINT = os.environ.get("E2E_SPARQL_ENDPOINT", "http://localhost:8891/sparql")
MIGRATIONS_TIMEOUT = int(os.environ.get("E2E_MIGRATIONS_TIMEOUT", "900"))
PIPELINE_TIMEOUT = int(os.environ.get("E2E_PIPELINE_TIMEOUT", "1200"))
POLL_INTERVAL = 15
COMPOSE = os.environ.get(
    "COMPOSE", "docker compose -p app-decide-e2e -f docker-compose.yml -f docker-compose.e2e.yml")
MAX_NEW_RESTARTS = 3

PDF_URL = "https://www.horebeke.be/src/Frontend/Files/userfiles/files/Notulen%20ocmw%20raad%2024_11_2025.pdf"
MUNICIPALITY = "http://data.lblod.info/id/bestuurseenheden/69af2347b1d0a9a39dbcf0ea4d94b9eaa15edb0c7d2dba56bbd9c39ab1c80e9f"
EXPECTED_LOCATION = "Kerkplein 1"
JOB_CREATOR_SELF_SERVICE = "http://lblod.data.gift/services/job-self-service"

JOBS_GRAPH = "http://mu.semte.ch/graphs/harvesting"
PUBLIC_GRAPH = "http://mu.semte.ch/graphs/public/pdf"
MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "config" / "migrations"
MIGRATIONS_GRAPH = "http://mu.semte.ch/application"
MIGRATION_CLASS = "http://mu.semte.ch/vocabularies/migrations/Migration"
MIGRATION_FILENAME = "http://mu.semte.ch/vocabularies/migrations/filename"

STATUS = "http://redpencil.data.gift/id/concept/JobStatus/"
TASK_OP = "http://lblod.data.gift/id/jobs/concept/TaskOperation/"
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

PREFIXES = """
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX cogs: <http://vocab.deri.ie/cogs#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX eli: <http://data.europa.eu/eli/ontology#>
PREFIX epvoc: <https://data.europarl.europa.eu/def/epvoc#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX gold: <http://purl.org/linguistics/gold/>
PREFIX hrvst: <http://lblod.data.gift/vocabularies/harvesting/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
PREFIX oa: <http://www.w3.org/ns/oa#>
PREFIX org: <http://www.w3.org/ns/org#>
PREFIX oslc: <http://open-services.net/ns/core#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
"""


def sparql(text, update=False):
    body = urllib.parse.urlencode({"update" if update else "query": PREFIXES + text}).encode()
    request = urllib.request.Request(ENDPOINT, data=body, headers={
        "mu-auth-sudo": "true",
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/x-www-form-urlencoded",
    })
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = response.read()
    return None if update else json.loads(payload)


def ask(pattern):
    return sparql(f"ASK {{ {pattern} }}")["boolean"]


def select(text):
    return [{key: value["value"] for key, value in row.items()}
            for row in sparql(text)["results"]["bindings"]]


def last_migration_filename():
    """The last migration mu-migrations should run, by ascending leading number in the filename."""
    candidates = []
    for path in MIGRATIONS_DIR.rglob("*"):
        if not path.is_file():
            continue
        match = re.match(r"(\d+).*\.(sparql|ttl)$", path.name)
        if match:
            candidates.append((int(match.group(1)), path.name))
    return max(candidates)[1]


LAST_MIGRATION = last_migration_filename()


class ServiceCrashed(Exception):
    pass


def compose(*args):
    return subprocess.run(shlex.split(COMPOSE) + list(args),
                          capture_output=True, text=True, check=True).stdout


def container_states():
    try:
        containers = compose("ps", "--all", "-q").split()
        if not containers:
            return {}
        inspected = subprocess.run(
            ["docker", "inspect", "--format",
             '{{index .Config.Labels "com.docker.compose.service"}} '
             "{{.RestartCount}} {{.State.Status}} {{.State.ExitCode}}", *containers],
            capture_output=True, text=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"Container check unavailable: {error}")
        return {}
    states = {}
    for line in inspected.splitlines():
        service, restarts, status, exit_code = line.split()
        states[service] = (int(restarts), status, exit_code)
    return states


def restart_counts():
    return {service: state[0] for service, state in container_states().items()}


def check_containers(baseline=None):
    crashing = []
    for service, (restarts, status, exit_code) in container_states().items():
        detail = f"{restarts} restarts, {status}, exit code {exit_code}"
        # migrations has no restart policy, so a crash there shows as a non-zero exit.
        if status == "exited" and exit_code != "0":
            crashing.append((service, detail))
        # Services restart on purpose until the database is up, so only count restarts after that.
        elif baseline is not None and restarts - baseline.get(service, restarts) >= MAX_NEW_RESTARTS:
            crashing.append((service, detail))
    if crashing:
        raise ServiceCrashed(crashing)


def wait_for_migrations():
    deadline = time.monotonic() + MIGRATIONS_TIMEOUT
    while time.monotonic() < deadline:
        check_containers()
        try:
            if ask(f"""GRAPH <{MIGRATIONS_GRAPH}> {{
  ?m a <{MIGRATION_CLASS}> ; <{MIGRATION_FILENAME}> ?f .
}}
FILTER(STR(?f) = "{LAST_MIGRATION}")"""):
                return True
            print(f"Migrations not finished yet, waiting for {LAST_MIGRATION}")
        except (urllib.error.URLError, ConnectionError, TimeoutError) as error:
            print(f"Database not reachable yet: {error}")
        time.sleep(POLL_INTERVAL)
    return False


def new_resource(base):
    resource_id = str(uuid.uuid4())
    return base + resource_id, resource_id


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


def job_tasks(job):
    return select(f"""
SELECT ?task ?op ?status WHERE {{
  GRAPH <{JOBS_GRAPH}> {{
    ?task a task:Task ;
      dct:isPartOf <{job}> ;
      task:operation ?op ;
      adms:status ?status .
  }}
}}""")


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


def summary(rows):
    counts = {}
    for row in rows:
        key = f"{row['op'].removeprefix(TASK_OP)}={row['status'].removeprefix(STATUS)}"
        counts[key] = counts.get(key, 0) + 1
    return ", ".join(f"{key} x{count}" for key, count in sorted(counts.items())) or "no tasks yet"


def wait_for_pipeline(job):
    deadline = time.monotonic() + PIPELINE_TIMEOUT
    rows = []
    baseline = restart_counts()
    while time.monotonic() < deadline:
        check_containers(baseline)
        try:
            rows = job_tasks(job)
            state = pipeline_state(rows)
            print(f"[{state}] {summary(rows)}")
            if state != "running":
                return state, rows
        except (urllib.error.URLError, ConnectionError, TimeoutError) as error:
            print(f"Database not reachable yet: {error}")
        time.sleep(POLL_INTERVAL)
    return "timeout", rows


def print_errors(job):
    rows = select(f"""
SELECT ?op ?message WHERE {{
  GRAPH <{JOBS_GRAPH}> {{ ?task dct:isPartOf <{job}> ; task:operation ?op . }}
  {{
    ?task task:resultsContainer ?container .
    ?container task:hasResource ?error .
    ?error dct:description ?message .
  }} UNION {{
    ?task task:hasError ?error .
    ?error oslc:message ?message .
  }}
}}""")
    for row in rows:
        print(f"ERROR {row['op'].removeprefix(TASK_OP)}: {row['message']}")


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


def main():
    try:
        return run_test()
    except ServiceCrashed as crash:
        for service, detail in crash.args[0]:
            print(f"FAIL: {service} keeps crashing ({detail}). Last log lines:")
            print(compose("logs", "--no-color", "--tail", "30", service))
        return 1


def run_test():
    print(f"Waiting for migrations to finish, up to {LAST_MIGRATION}")
    if not wait_for_migrations():
        print(f"FAIL: migrations did not reach {LAST_MIGRATION} before the migrations timeout")
        return 1

    job = seed()
    print(f"Seeded job {job}")

    state, rows = wait_for_pipeline(job)
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
    sys.exit(main())
