import gc
import json
import os
import re
import tempfile
import csv
import uuid  # added for temp table names
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path

import pytz
import requests
import typer
from loguru import logger
from tqdm import tqdm

from openf1.services.ingestor_livetiming.core.decoding import decode
from openf1.services.ingestor_livetiming.core.objects import (
    Document,
    Message,
    get_collections,
    get_source_topics,
)
from openf1.services.ingestor_livetiming.core.processing.main import process_messages
from openf1.util.db_duckdb import (
    insert_data_sync,
    get_existing_sessions,
    delete_data_by_session,
    _get_duckdb_connection,
    _ensure_table_exists,
)
from openf1.util.duckdb_document import patch_document_class, get_primary_key_fields  # added get_primary_key_fields
from openf1.util.misc import join_url, json_serializer, to_datetime, to_timedelta
from openf1.util.schedule import get_meeting_keys
from openf1.util.schedule import get_schedule as _get_schedule
from openf1.util.schedule import get_session_keys

# Configure logger to WARNING level by default
logger.remove()  # Remove default handler
logger.add(lambda msg: print(msg, end=""), level="WARNING")

# Patch the Document class to support DuckDB
patch_document_class()

cli = typer.Typer()

# Global variables to store CLI options
_global_log_level = "WARNING"
_global_full_refresh = False
_global_use_csv = False

# Flag to determine if the script is being run from the command line
_is_called_from_cli = False


@cli.callback()
def main(
    log_level: str = typer.Option("WARNING", "--log-level", help="Set log level (DEBUG, INFO, WARNING, ERROR)"),
    full_refresh: bool = typer.Option(False, "--full-refresh", help="Force a complete refresh of all data"),
    csv: bool = typer.Option(False, "--csv", help="Load data using CSV in DuckDB instead of individual queries"),
):
    """
    OpenF1 Historical Data Ingestion Tool
    
    Ingest Formula 1 historical timing data into DuckDB.
    """
    global _global_log_level, _global_full_refresh, _global_use_csv
    _global_log_level = log_level.upper()
    _global_full_refresh = full_refresh
    _global_use_csv = csv
    
    # Configure logger based on global log_level parameter
    logger.remove()  # Remove existing handlers
    logger.add(lambda msg: print(msg, end=""), level=_global_log_level)


@cli.command()
def get_schedule(year: int) -> dict:
    schedule = _get_schedule(year)

    if _is_called_from_cli:
        schedule_json = json.dumps(schedule, indent=2, default=json_serializer)
        print(schedule_json)

    return schedule


@lru_cache()
def get_session_url(year: int, meeting_key: int, session_key: int) -> str:
    """Retrieves the URL for downloading raw data of a specific session"""
    BASE_URL = "https://livetiming.formula1.com/static"

    schedule = _get_schedule(year)

    session_url = None
    for meeting in schedule["Meetings"]:
        if meeting["Key"] == meeting_key:
            for session in meeting["Sessions"]:
                if session["Key"] == session_key:
                    if "Path" not in session:
                        continue
                    path = session["Path"]
                    session_url = join_url(BASE_URL, path)

    if session_url is None:
        raise ValueError(
            f"Session not found (year: `{year}`, meeting_key: `{meeting_key}`, "
            f"session_key: `{session_key}`)"
        )

    return session_url


def _list_topics(session_url: str) -> list[str]:
    """Returns all the available raw data filenames for the session"""
    index_url = join_url(session_url, "Index.json")
    index_response = requests.get(index_url)
    index_content = json.loads(index_response.content)

    filenames = [v["StreamPath"] for v in index_content["Feeds"].values()]
    topics = [f[: -len(".jsonStream")] for f in filenames if f.endswith(".jsonStream")]
    topics = sorted(topics)

    return topics


@cli.command()
def list_topics(
    year: int,
    meeting_key: int,
    session_key: int,
) -> list[str]:
    session_url = get_session_url(
        year=year, meeting_key=meeting_key, session_key=session_key
    )
    topics = _list_topics(session_url)

    if _is_called_from_cli:
        print(topics)
    return topics


@lru_cache()
def _get_topic_content(session_url: str, topic: str) -> list[str]:
    topic_filename = f"{topic}.jsonStream"
    url_topic = join_url(session_url, topic_filename)
    topic_content = requests.get(url_topic).text.split("\r\n")
    return topic_content


@cli.command()
def get_topic_content(
    year: int, meeting_key: int, session_key: int, topic: str
) -> list[str]:
    session_url = get_session_url(
        year=year, meeting_key=meeting_key, session_key=session_key
    )
    content = _get_topic_content(session_url=session_url, topic=topic)

    if _is_called_from_cli:
        print("\n".join(content))
    return content


def _parse_line(line: str) -> tuple[timedelta | None, str | None]:
    """Parses a line to extract the duration since session start and raw data.

    The line is expected to be formatted as follows:
    (duration since session start, raw data)
    """
    pattern = r"(\d+:\d+:\d+\.\d+)(.*)"
    match = re.match(pattern, line)
    if match is None:
        return None, None
    session_time = to_timedelta(match.group(1))
    raw_data = match.group(2).strip("\r").strip('"')
    return session_time, raw_data


def _parse_and_decode_topic_content(
    topic: str,
    topic_raw_content: list[str],
    t0: datetime,
) -> list[Message]:
    messages = []
    for line in topic_raw_content:
        if len(line) == 0:
            continue
        session_time, content = _parse_line(line)

        if session_time is None:
            continue

        if isinstance(content, str):
            content = decode(content)

        messages.append(
            Message(
                topic=topic,
                content=content,
                timepoint=t0 + session_time,
            )
        )

    return messages


@lru_cache()
def _get_t0(session_url: str) -> datetime:
    """Calculates the most likely start time of a session (t0) based on
    Position and CarData messages.
    The calculation method comes from the FastF1 package (https://github.com/theOehrly/Fast-F1/blob/317bacf8c61038d7e8d0f48165330167702b349f/fastf1/core.py#L2208).
    """
    t_ref = datetime(1970, 1, 1)
    t0_candidates = []

    position_content = _get_topic_content(session_url=session_url, topic="Position.z")
    position_messages = _parse_and_decode_topic_content(
        topic="Position.z",
        topic_raw_content=position_content,
        t0=t_ref,
    )
    for message in position_messages:
        for record in message.content["Position"]:
            timepoint = to_datetime(record["Timestamp"])
            session_time = message.timepoint - t_ref
            t0_candidates.append(timepoint - session_time)

    cardata_content = _get_topic_content(session_url=session_url, topic="CarData.z")
    cardata_messages = _parse_and_decode_topic_content(
        topic="CarData.z",
        topic_raw_content=cardata_content,
        t0=t_ref,
    )
    for message in cardata_messages:
        for record in message.content["Entries"]:
            timepoint = to_datetime(record["Utc"])
            session_time = message.timepoint - t_ref
            t0_candidates.append(timepoint - session_time)

    t0_estimate = max(t0_candidates)
    t0_estimate = pytz.utc.localize(t0_estimate)

    return t0_estimate


@cli.command()
def get_t0(year: int, meeting_key: int, session_key: int) -> datetime:
    session_url = get_session_url(
        year=year, meeting_key=meeting_key, session_key=session_key
    )
    t0 = _get_t0(session_url)

    if _is_called_from_cli:
        print(t0)
    return t0


def _get_messages(session_url: str, topics: list[str], t0: datetime) -> list[Message]:
    messages = []
    for topic in topics:
        raw_content = _get_topic_content(
            session_url=session_url,
            topic=topic,
        )
        messages += _parse_and_decode_topic_content(
            topic=topic,
            topic_raw_content=raw_content,
            t0=t0,
        )
    messages = sorted(messages, key=lambda m: (m.timepoint, m.topic))
    return messages


@cli.command()
def get_messages(
    year: int,
    meeting_key: int,
    session_key: int,
    topics: list[str],
    verbose: bool = True,
) -> list[Message]:
    session_url = get_session_url(
        year=year, meeting_key=meeting_key, session_key=session_key
    )
    if verbose:
        logger.info(f"Session URL: {session_url}")

    t0 = _get_t0(session_url)
    if verbose:
        logger.info(f"t0: {t0}")

    messages = _get_messages(session_url=session_url, topics=topics, t0=t0)
    if verbose:
        logger.info(f"Fetched {len(messages)} messages")

    if _is_called_from_cli:
        messages_json = json.dumps(messages, indent=2, default=json_serializer)
        print(messages_json)

    return messages


def _get_processed_documents(
    year: int,
    meeting_key: int,
    session_key: int,
    collection_names: list[str],
    verbose: bool = True,
) -> dict[str, list[Document]]:
    session_url = get_session_url(
        year=year, meeting_key=meeting_key, session_key=session_key
    )
    if verbose:
        logger.info(f"Session URL: {session_url}")

    t0 = _get_t0(session_url)
    if verbose:
        logger.info(f"t0: {t0}")

    topics = set().union(*[get_source_topics(n) for n in collection_names])
    topics = sorted(list(topics))
    if verbose:
        logger.info(f"Topics used: {topics}")

    messages = _get_messages(session_url=session_url, topics=topics, t0=t0)
    if verbose:
        logger.info(f"Fetched {len(messages)} messages")

    if verbose:
        logger.info("Starting processing")

    docs_by_collection = process_messages(
        messages=messages, meeting_key=meeting_key, session_key=session_key
    )
    docs_by_collection = {
        col: docs_by_collection[col] if col in docs_by_collection else []
        for col in collection_names
    }

    if verbose:
        n_docs = sum(len(d) for d in docs_by_collection.values())
        logger.info(f"Processed {n_docs} documents")

    return docs_by_collection


@cli.command()
def get_processed_documents(
    year: int,
    meeting_key: int,
    session_key: int,
    collection_names: list[str],
    verbose: bool = True,
) -> dict[str, list[Document]]:
    docs_by_collection = _get_processed_documents(
        year=year,
        meeting_key=meeting_key,
        session_key=session_key,
        collection_names=collection_names,
        verbose=verbose,
    )

    if _is_called_from_cli:
        docs_by_collection = {
            k: [d.to_duckdb_doc_sync() for d in v] for k, v in docs_by_collection.items()  # Use DuckDB method
        }
        docs_by_collection_json = json.dumps(
            docs_by_collection, indent=2, default=json_serializer
        )
        print(docs_by_collection_json)

    return docs_by_collection


def _documents_to_csv(collection_name: str, docs: list[Document], verbose: bool = True) -> str:
    """
    Write documents to a CSV file for bulk loading into DuckDB
    
    Args:
        collection_name: The name of the collection
        docs: The documents to write
        verbose: Whether to show verbose output
        
    Returns:
        The path to the CSV file
    """
    # Create a temporary file with a unique name
    tmp_dir = tempfile.gettempdir()
    csv_path = os.path.join(tmp_dir, f"{collection_name}_{hash(str(docs[:5]))}.csv")
    
    if verbose:
        logger.info(f"Writing {len(docs)} documents to CSV file: {csv_path}")
    
    # Convert documents to DuckDB format
    prepared_docs = []
    for doc in docs:
        # Get document as dict with DuckDB representation
        doc_dict = doc.to_duckdb_doc_sync()
        prepared_docs.append(doc_dict)
    
    # If no documents, return empty path
    if not prepared_docs:
        return ""
    
    # Get all possible column names from all documents
    all_columns = set()
    for doc in prepared_docs:
        all_columns.update(doc.keys())
    
    # Sort columns for consistency
    all_columns = sorted(list(all_columns))
    
    # Write documents to CSV
    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=all_columns)
        writer.writeheader()
        for doc in prepared_docs:
            # Ensure all columns are present
            row = {col: doc.get(col, None) for col in all_columns}
            writer.writerow(row)
    
    return csv_path


def _load_csv_to_duckdb(collection_name: str, csv_path: str, verbose: bool = True) -> int:
    """
    Load CSV data into DuckDB via a temporary staging table and MERGE (upsert) into the final table.
    This prevents overwriting existing rows unintentionally and ensures only changes/new rows are applied.
    
    Args:
        collection_name: The name of the target table
        csv_path: Path to the source CSV
        verbose: Verbosity flag
    Returns:
        Number of rows staged (potentially inserted/updated)
    """
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        if verbose:
            logger.info(f"CSV file is empty or doesn't exist: {csv_path}")
        return 0

    conn = _get_duckdb_connection()

    # Peek first row to build/extend target schema
    with open(csv_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        try:
            first_row = next(reader)
        except StopIteration:
            if verbose:
                logger.info("CSV file is empty")
            return 0

    _ensure_table_exists(collection_name, first_row)

    # Create a unique temporary staging table name
    staging_table = f"_stg_{collection_name}_{uuid.uuid4().hex[:8]}"

    try:
        if verbose:
            logger.info(f"Staging CSV data into temp table {staging_table}")

        # Create temp table from CSV
        create_staging_sql = f"""
            CREATE TEMP TABLE {staging_table} AS
            SELECT * FROM read_csv('{csv_path}')
        """
        conn.execute(create_staging_sql)

        # Collect columns from staging & target
        staging_cols = [r[1] for r in conn.execute(f"PRAGMA table_info({staging_table})").fetchall()]
        target_cols = [r[1] for r in conn.execute(f"PRAGMA table_info({collection_name})").fetchall()]

        logger.info(f"Staging columns: {staging_cols}")

        # Add any missing columns (as VARCHAR) to target to accommodate new fields
        for col in staging_cols:
            if col not in target_cols:
                try:
                    conn.execute(f"ALTER TABLE {collection_name} ADD COLUMN {col} VARCHAR")
                    if verbose:
                        logger.info(f"Added missing column {col} to {collection_name}")
                except Exception as e:
                    if verbose:
                        logger.warning(f"Could not add column {col}: {e}")

        # Refresh target columns list
        target_cols = [r[1] for r in conn.execute(f"PRAGMA table_info({collection_name})").fetchall()]

        # Determine primary keys for merge logic
        primary_keys = get_primary_key_fields(collection_name)
        primary_keys = [pk for pk in primary_keys if pk in staging_cols]

        # If no primary keys available, fallback to simple INSERT (append-only semantics)
        if not primary_keys:
            if verbose:
                logger.warning(f"No primary keys found for {collection_name}; performing append INSERT from staging")
            insert_cols = [c for c in staging_cols]
            insert_sql = f"INSERT INTO {collection_name} ({', '.join(insert_cols)}) SELECT {', '.join(insert_cols)} FROM {staging_table}"
            conn.execute(insert_sql)
            row_count = conn.execute(f"SELECT COUNT(*) FROM {staging_table}").fetchone()[0]
            return row_count

        # INSERT OR REPLACE approach (simpler than MERGE); relies on PK/unique constraints
        common_cols = [c for c in staging_cols if c in target_cols]
        col_list = ", ".join(common_cols)
        insert_replace_sql = (
            f"INSERT OR REPLACE INTO {collection_name} ({col_list}) "
            f"SELECT {col_list} FROM {staging_table}"
        )
        if verbose:
            logger.info(
                f"Upserting into {collection_name} using INSERT OR REPLACE on {len(common_cols)} columns (pk={primary_keys})"
            )
            logger.debug(f"INSERT OR REPLACE SQL: {insert_replace_sql}")
        conn.execute(insert_replace_sql)

        staged_rows = conn.execute(f"SELECT COUNT(*) FROM {staging_table}").fetchone()[0]
        if verbose:
            logger.info(f"Upsert complete: {staged_rows} staged rows processed for {collection_name}")
        return staged_rows

    except Exception as e:
        logger.error(f"Error staging/merging CSV data into {collection_name}: {e}")
        return 0
    finally:
        # Remove CSV file
        if os.path.exists(csv_path):
            os.remove(csv_path)
            if verbose:
                logger.info(f"Deleted temporary CSV file: {csv_path}")


@cli.command()
def ingest_collections(
    year: int,
    meeting_key: int,
    session_key: int,
    collection_names: list[str],
    verbose: bool = True,
):
    existing_sessions = get_existing_sessions()
    if not _global_full_refresh and session_key in existing_sessions:
        return
    
    docs_by_collection = _get_processed_documents(
        year=year,
        meeting_key=meeting_key,
        session_key=session_key,
        collection_names=collection_names,
        verbose=verbose,
    )

    if verbose:
        logger.info("Inserting documents to DuckDB")  # Updated message

    for collection, docs in tqdm(list(docs_by_collection.items()), desc="Processing collections", disable=not verbose, leave=False):
        # Use CSV method if specified
        if _global_use_csv:
            if verbose:
                logger.info(f"Using CSV method for {collection}")
            # Convert documents to CSV and load into DuckDB
            csv_path = _documents_to_csv(collection, docs, verbose)
            _load_csv_to_duckdb(collection, csv_path, verbose)
        else:
            # Use traditional method
            if _global_full_refresh or (collection != "car_data" and collection != "location"):
                insert_data_sync(collection_name=collection, docs=docs, verbose=verbose, use_simple_insert=False)
            else:
                insert_data_sync(collection_name=collection, docs=docs, verbose=verbose, use_simple_insert=True)

@cli.command()
def ingest_session(year: int, meeting_key: int, session_key: int, verbose: bool = True):
    """Ingest a specific session's data"""
    collections = get_collections(meeting_key=meeting_key, session_key=session_key)
    collection_names = sorted([c.__class__.name for c in collections])

    if verbose:
        logger.info(
            f"Ingesting {len(collection_names)} collections: {collection_names}"
        )

    ingest_collections(
        year=year,
        meeting_key=meeting_key,
        session_key=session_key,
        collection_names=collection_names,
        verbose=verbose,
    )


@cli.command()
def ingest_meeting(year: int, meeting_key: int, verbose: bool = True):
    """Ingest all sessions from a specific meeting"""
    session_keys = get_session_keys(year=year, meeting_key=meeting_key)
    if verbose:
        logger.info(f"{len(session_keys)} sessions found: {session_keys}")
    
    for session_key in tqdm(session_keys, desc="Processing sessions", disable=not verbose, leave=False):
        if verbose:
            tqdm.write(f"Ingesting session {session_key}")
        ingest_session(
            year=year, meeting_key=meeting_key, session_key=session_key, verbose=verbose
        )


@cli.command()
def ingest_season(year: int, verbose: bool = True):
    """Ingest all meetings from a specific season"""
    meeting_keys = get_meeting_keys(year)
    if verbose:
        logger.info(f"{len(meeting_keys)} meetings found: {meeting_keys}")

    for meeting_key in tqdm(meeting_keys, desc="Processing meetings", disable=not verbose):
        if verbose:
            tqdm.write(f"Ingesting meeting {meeting_key}")
        ingest_meeting(year=year, meeting_key=meeting_key, verbose=True)


@cli.command()
def delete_meeting(year: int, meeting_key: int, verbose: bool = True):
    """Delete all data from all sessions in a specific meeting"""
    session_keys = get_session_keys(year=year, meeting_key=meeting_key)
    if verbose:
        logger.info(f"{len(session_keys)} sessions found for meeting {meeting_key}: {session_keys}")
        
    for session_key in tqdm(session_keys, desc="Processing sessions", disable=not verbose, leave=False):
        if verbose:
            tqdm.write(f"Deleting session {session_key}")
        delete_session(
            meeting_key=meeting_key, session_key=session_key, verbose=verbose
        )

@cli.command()
def delete_session(meeting_key: int, session_key: int, verbose: bool = True):
    """Delete all data from a specific session"""
    collections = get_collections(meeting_key=meeting_key, session_key=session_key)
    # Filter out "meetings" collection
    collection_names = sorted([c.__class__.name for c in collections if c.__class__.name != "meetings"])
    
    if verbose:
        logger.info(f"Deleting data from {len(collection_names)} collections: {collection_names}")
    
    # Total rows deleted counter
    total_rows_deleted = 0
    
    # Delete data from each collection
    for collection_name in tqdm(collection_names, desc="Deleting from collections", disable=not verbose, leave=False):
        rows_deleted = delete_data_by_session(
            collection_name=collection_name,
            meeting_key=meeting_key,
            session_key=session_key,
            verbose=verbose
        )
        total_rows_deleted += rows_deleted
    
    if verbose:
        logger.info(f"Session {session_key} data deletion completed: {total_rows_deleted} total rows deleted")


if __name__ == "__main__":
    _is_called_from_cli = True
    cli()
