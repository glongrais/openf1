import os
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List
import gc

import duckdb
from loguru import logger
from tqdm import tqdm

from openf1.util.misc import timed_cache
from openf1.util.duckdb_document import get_primary_key_fields

_DUCKDB_PATH = os.getenv("DUCKDB_PATH", "openf1.duckdb")

_SORT_KEYS = [
    "date",
    "date_start", 
    "meeting_key",
    "session_key",
    "lap_start",
    "lap_number",
    "lap_end",
    "date_end",
    "stint_number",
    "driver_number",
]


@lru_cache()
def _get_duckdb_connection():
    """Get a DuckDB connection"""
    return duckdb.connect(_DUCKDB_PATH)


def _generate_schema(collection_name, sample_doc):
    """Generate DuckDB schema from a sample document with special handling for mixed types"""
    columns = []
    primary_keys = get_primary_key_fields(collection_name)
    
    # Special fields that are known to have mixed types (numbers and strings)
    mixed_type_fields = {
        'intervals': ['gap_to_leader'],  # Can be numeric (seconds) or string ("+X LAP")
        # Add other collections and fields as needed
    }
    
    mixed_fields = mixed_type_fields.get(collection_name, [])
    
    for key, value in sample_doc.items():
        if key == '_key':
            columns.append(f"{key} VARCHAR NOT NULL")
            primary_keys.append(key)
        elif key == '_id':
            columns.append(f"{key} BIGINT")
        elif key == 'record_hash':
            # Special handling for record_hash field (used as primary key for race_control)
            columns.append(f"{key} VARCHAR NOT NULL")
        elif key in mixed_fields:
            # Force VARCHAR for fields known to have mixed types
            columns.append(f"{key} VARCHAR")
        elif isinstance(value, str):
            columns.append(f"{key} VARCHAR")
        elif isinstance(value, int):
            columns.append(f"{key} BIGINT")
        elif isinstance(value, float):
            columns.append(f"{key} DOUBLE")
        elif isinstance(value, bool):
            columns.append(f"{key} BOOLEAN")
        else:
            columns.append(f"{key} VARCHAR")  # Default to VARCHAR for unknown types
    
    return columns, primary_keys


def _get_actual_primary_keys(connection, table_name):
    """Get the actual primary key columns from the database table"""
    try:
        # Query DuckDB's information schema to get primary key constraints
        result = connection.execute(f"""
            SELECT column_name 
            FROM information_schema.key_column_usage 
            WHERE table_name = '{table_name}' 
            AND constraint_name LIKE '%_pkey'
            ORDER BY ordinal_position
        """).fetchall()
        
        if result:
            return [row[0] for row in result]
        
        # Alternative method using PRAGMA table_info for DuckDB
        table_info = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        pk_columns = []
        for row in table_info:
            # row format: (cid, name, type, notnull, dflt_value, pk)
            if len(row) > 5 and row[5] == 1:  # pk column is 1 for primary key
                pk_columns.append(row[1])  # column name
        
        return pk_columns
        
    except Exception as e:
        logger.debug(f"Could not determine primary keys for {table_name}: {e}")
        return []


def _ensure_table_exists(collection_name, sample_doc):
    """Ensure a table exists for the given collection with the right schema"""
    table_name = collection_name
    
    # Get main connection
    connection = _get_duckdb_connection()
    
    try:
        # Generate schema from sample document
        columns, primary_keys = _generate_schema(collection_name, sample_doc)
        
        # Try to describe the table first
        result = connection.execute(f"SELECT * FROM {table_name} LIMIT 0")
        existing_columns = [desc[0] for desc in result.description]
        
        # Check if all required columns exist
        missing_columns = [col for col in columns if col.split()[0] not in existing_columns]
        
        if missing_columns:
            # Add missing columns
            for col in missing_columns:
                col_name = col.split()[0]
                col_type = col.split()[1]
                try:
                    connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")
                    logger.info(f"Added column {col_name} to table {table_name}")
                except Exception as alter_error:
                    if "already exists" not in str(alter_error):
                        logger.warning(f"Failed to add column {col_name}: {alter_error}")
    
    except Exception as e:
        # Table doesn't exist, create it
        primary_key_clause = f", PRIMARY KEY ({', '.join(primary_keys)})" if primary_keys else ""
        create_statement = f"CREATE TABLE {table_name} ({', '.join(columns)}{primary_key_clause})"
        
        logger.info(f"About to execute CREATE TABLE for {table_name}: {create_statement}")
        
        try:
            connection.execute(create_statement)
            logger.info(f"Created table {table_name} with schema: {create_statement}")
        except Exception as create_error:
            if "already exists" in str(create_error):
                logger.info(f"Table {table_name} already exists, continuing...")
                return
            logger.warning(f"Failed to create table {table_name}: {create_error}")
            # Try a simpler version without primary keys for fallback
            try:
                simple_create = f"CREATE TABLE {table_name} ({', '.join(columns)})"
                connection.execute(simple_create)
                logger.info(f"Created fallback table {table_name} without constraints")
            except Exception as fallback_error:
                if "already exists" not in str(fallback_error):
                    logger.error(f"Failed to create even fallback table {table_name}: {fallback_error}")
                return
        
        # Verify table was created correctly
        try:
            verify_result = connection.execute(f"PRAGMA table_info({table_name})")
            table_info = verify_result.fetchall()
            if table_info:
                logger.info(f"Verified table {table_name} definition: {connection.execute(f'SELECT sql FROM sqlite_master WHERE type=\"table\" AND name=\"{table_name}\"').fetchone()[0]}")
            else:
                logger.warning(f"Could not verify table {table_name} definition")
        except Exception as verify_error:
            logger.warning(f"Could not verify table {table_name} definition")


def _flatten_document(doc: dict, prefix: str = "") -> dict:
    """Flatten nested dictionaries for DuckDB storage"""
    flattened = {}
    
    for key, value in doc.items():
        new_key = f"{prefix}{key}" if prefix else key
        
        if isinstance(value, dict):
            # Recursively flatten nested dictionaries
            flattened.update(_flatten_document(value, f"{new_key}_"))
        elif isinstance(value, list):
            # Convert lists to JSON strings
            import json
            flattened[new_key] = json.dumps(value)
        else:
            flattened[new_key] = value
    
    return flattened


def _prepare_document_for_duckdb(doc: dict) -> dict:
    """Prepare a document for DuckDB insertion"""
    # Flatten the document
    flat_doc = _flatten_document(doc)
    
    # Ensure datetime objects are properly formatted
    for key, value in flat_doc.items():
        if isinstance(value, datetime):
            if value.tzinfo is None:
                flat_doc[key] = value.replace(tzinfo=timezone.utc)
    
    return flat_doc


async def get_documents(collection_name: str, filters: dict) -> list[dict]:
    """Retrieves documents from a DuckDB table with filters and sorting"""
    conn = _get_duckdb_connection()
    
    # Build WHERE clause from filters
    where_conditions = []
    params = []
    
    for key, value in filters.items():
        if isinstance(value, dict):
            # Handle comparison operators like {"$gte": value}
            for op, val in value.items():
                if op == "$gte":
                    where_conditions.append(f"{key} >= ?")
                    params.append(val)
                elif op == "$lte":
                    where_conditions.append(f"{key} <= ?")
                    params.append(val)
                elif op == "$gt":
                    where_conditions.append(f"{key} > ?")
                    params.append(val)
                elif op == "$lt":
                    where_conditions.append(f"{key} < ?")
                    params.append(val)
                elif op == "$ne":
                    where_conditions.append(f"{key} != ?")
                    params.append(val)
        else:
            where_conditions.append(f"{key} = ?")
            params.append(value)
    
    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
    
    # Build ORDER BY clause
    sort_columns = []
    for key in _SORT_KEYS:
        # Check if column exists in the table
        try:
            conn.execute(f"SELECT {key} FROM {collection_name} LIMIT 1")
            sort_columns.append(key)
        except:
            continue
    
    order_clause = f"ORDER BY {', '.join(sort_columns)}" if sort_columns else ""
    
    query = f"""
    SELECT * FROM {collection_name}
    WHERE {where_clause}
    {order_clause}
    """
    
    try:
        results = conn.execute(query, params).fetchall()
        columns = [desc[0] for desc in conn.description]
        
        # Convert results to list of dictionaries
        documents = []
        for row in results:
            doc = dict(zip(columns, row))
            # Add UTC timezone to datetime fields if not set
            for key, val in doc.items():
                if isinstance(val, datetime) and val.tzinfo is None:
                    doc[key] = val.replace(tzinfo=timezone.utc)
            documents.append(doc)
        
        return documents
    except Exception as e:
        logger.error(f"Error querying {collection_name}: {e}")
        return []


@timed_cache(60)  # Cache the output for 1 minute
def get_latest_session_info() -> dict:
    """Get the latest session information"""
    conn = _get_duckdb_connection()
    
    try:
        result = conn.execute(
            "SELECT * FROM sessions ORDER BY date_start DESC LIMIT 1"
        ).fetchone()
        
        if result:
            columns = [desc[0] for desc in conn.description]
            return dict(zip(columns, result))
        else:
            raise SystemError("Could not find any session in DuckDB")
    except Exception as e:
        logger.error(f"Error getting latest session info: {e}")
        raise SystemError("Could not find any session in DuckDB")


@lru_cache()
def session_key_to_path(session_key: int) -> str | None:
    """Get the path for a session by session_key"""
    conn = _get_duckdb_connection()
    
    try:
        result = conn.execute(
            "SELECT _path FROM sessions WHERE session_key = ? AND _path IS NOT NULL LIMIT 1",
            [session_key]
        ).fetchone()
        
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Error getting session path: {e}")
        return None


def insert_data_sync(collection_name: str, docs: list[dict], batch_size: int = 2000, use_simple_insert: bool = False, verbose: bool = True) -> None:
    """Inserts documents into a DuckDB table in batches (single-threaded, memory-optimized)
    
    Args:
        collection_name: Name of the table to insert into
        docs: List of documents to insert
        batch_size: Size of each batch for better performance (reduced for memory efficiency)
        use_simple_insert: If True, use simple INSERT instead of upsert for append-only data
    """
    if not docs:
        return
    
    logger.info(f"Processing {len(docs)} documents for DuckDB insertion (memory-optimized)...")
    
    # Process first document to set up table schema
    if docs:
        first_doc = _prepare_document_for_duckdb(docs[0].to_duckdb_doc_sync())
    else:
        return
    
    # Ensure table exists using first document as schema
    conn = _get_duckdb_connection()
    _ensure_table_exists(collection_name, first_doc)
    
    # Get all columns from the first prepared document (for schema consistency)
    all_columns = sorted(list(first_doc.keys()))
    
    # Add missing columns to table if needed
    try:
        existing_columns_result = conn.execute(f"PRAGMA table_info({collection_name})").fetchall()
        existing_columns = {row[1] for row in existing_columns_result}  # row[1] is column name
        
        for col in all_columns:
            if col not in existing_columns:
                # Add missing column - default to VARCHAR
                try:
                    conn.execute(f"ALTER TABLE {collection_name} ADD COLUMN {col} VARCHAR")
                    logger.info(f"Added column {col} to table {collection_name}")
                except Exception as e:
                    logger.warning(f"Failed to add column {col}: {e}")
    except Exception as e:
        logger.warning(f"Error checking table structure: {e}")
    
    # Calculate total batches for progress tracking
    total_batches = (len(docs) + batch_size - 1) // batch_size
    logger.info(f"Inserting {len(docs)} documents into {collection_name} in {total_batches} batches...")
    
    # Get primary key information for upserts
    theoretical_primary_keys = get_primary_key_fields(collection_name)
    
    # Get actual primary keys from the database table
    actual_primary_keys = _get_actual_primary_keys(conn, collection_name)
    
    # Use actual primary keys if they exist, otherwise fall back to theoretical ones
    if actual_primary_keys:
        primary_keys = actual_primary_keys
        logger.info(f"Using actual database primary keys for {collection_name}: {primary_keys}")
    else:
        primary_keys = theoretical_primary_keys
        logger.info(f"No database constraints found, using theoretical primary keys for {collection_name}: {primary_keys}")
    
    # Prepare SQL statement once
    placeholders = ", ".join(["?" for _ in all_columns])
    
    if use_simple_insert:
        # Use simple INSERT for append-only data (better performance)
        insert_sql = f"""
        INSERT INTO {collection_name} ({', '.join(all_columns)})
        VALUES ({placeholders})
        """
        logger.info(f"Using simple INSERT for {collection_name} (append-only mode)")
    elif actual_primary_keys and all(pk in all_columns for pk in actual_primary_keys):
        # Only use ON CONFLICT if we have actual primary key constraints in the database
        pk_list = ', '.join(actual_primary_keys)
        set_clauses = ', '.join([f"{col} = EXCLUDED.{col}" for col in all_columns if col not in actual_primary_keys])
        
        if set_clauses:  # Only add UPDATE when there are non-PK columns to update
            insert_sql = f"""
            INSERT INTO {collection_name} ({', '.join(all_columns)})
            VALUES ({placeholders})
            ON CONFLICT ({pk_list}) DO UPDATE SET {set_clauses}
            """
        else:
            # If only primary key columns, just ignore duplicates
            insert_sql = f"""
            INSERT INTO {collection_name} ({', '.join(all_columns)})
            VALUES ({placeholders})
            ON CONFLICT ({pk_list}) DO NOTHING
            """
        logger.info(f"Using upsert with database primary keys [{pk_list}] for {collection_name}")
    else:
        # Fallback to simple INSERT if no actual primary key constraints exist
        insert_sql = f"""
        INSERT INTO {collection_name} ({', '.join(all_columns)})
        VALUES ({placeholders})
        """
        if actual_primary_keys:
            logger.warning(f"Primary key columns {actual_primary_keys} not found in document columns {all_columns}, using simple INSERT")
        else:
            logger.warning(f"No primary key constraints found in database for {collection_name}, using simple INSERT")
    
    # Split documents into batches and process them streaming-style to save memory
    successful_inserts = 0
    failed_inserts = 0

    with tqdm(total=total_batches, desc=f"Inserting into {collection_name}", unit="batch", disable=not verbose, leave=False) as pbar:
        for batch_idx in range(0, len(docs), batch_size):
            # Get current batch slice (this creates a view, not a copy)
            batch_end = min(batch_idx + batch_size, len(docs))
            current_batch_docs = [d.to_duckdb_doc_sync() for d in docs[batch_idx:batch_end]]

            try:
                # Process documents in this batch on-the-fly to save memory
                batch_values = []
                for doc in current_batch_docs:
                    prepared_doc = _prepare_document_for_duckdb(doc)
                    
                    # Ensure all expected columns are present
                    row_data = {}
                    for col in all_columns:
                        row_data[col] = prepared_doc.get(col)
                    
                    row = tuple(row_data[col] for col in all_columns)
                    batch_values.append(row)
                
                # Execute batch insert
                conn.executemany(insert_sql, batch_values)
                successful_inserts += len(current_batch_docs)
                
                # Clear batch data to free memory
                del batch_values
                del current_batch_docs
                
                # Force garbage collection every 10 batches to prevent memory buildup
                if (batch_idx // batch_size) % 10 == 0:
                    gc.collect()
                
            except Exception as e:
                logger.error(f"Batch {batch_idx // batch_size} error: {e}")
                
                # If batch fails, try individual inserts for partial success
                individual_success = 0
                for doc in current_batch_docs[:5]:  # Only try first 5 to avoid spam
                    try:
                        prepared_doc = _prepare_document_for_duckdb(doc)
                        row_data = {}
                        for col in all_columns:
                            row_data[col] = prepared_doc.get(col)
                        row = tuple(row_data[col] for col in all_columns)
                        conn.execute(insert_sql, row)
                        individual_success += 1
                    except Exception:
                        break
                
                successful_inserts += individual_success
                failed_inserts += len(current_batch_docs) - individual_success
            
            pbar.update(1)
    
    # Final summary
    total_docs = successful_inserts + failed_inserts
    logger.info(f"Insertion complete for {collection_name}: {successful_inserts}/{total_docs} documents inserted successfully")
    if failed_inserts > 0:
        logger.warning(f"{failed_inserts} documents failed to insert into {collection_name}")

async def insert_data_async(collection_name: str, docs: list[dict]):
    """Async wrapper for insert_data_sync (DuckDB is synchronous)"""
    insert_data_sync(collection_name, docs)


@lru_cache(maxsize=32)
def get_existing_sessions(year: int = None, meeting_key: int = None) -> set:
    """Gets all sessions that exist in the database, optionally filtered by year or meeting
    
    Args:
        year: Optional year to filter by
        meeting_key: Optional meeting key to filter by
        
    Returns:
        A set of tuples (meeting_key, session_key) for all existing sessions
        
    Note:
        This function is cached using LRU cache, so repeated calls with the same 
        parameters will return the cached result without hitting the database.
    """
    conn = _get_duckdb_connection()
    
    try:
        # Check if the sessions table exists
        table_exists = conn.execute("""
            SELECT count(*) FROM information_schema.tables 
            WHERE table_name = 'sessions'
        """).fetchone()[0]
        
        if not table_exists:
            return set()
        
        # Build query based on filters provided
        query = "SELECT meeting_key, session_key FROM sessions"
        params = []
        
        if year is not None or meeting_key is not None:
            query += " WHERE "
            conditions = []
            
            if year is not None:
                conditions.append("year = ?")
                params.append(year)
                
            if meeting_key is not None:
                conditions.append("meeting_key = ?")
                params.append(meeting_key)
                
            query += " AND ".join(conditions)
        
        # Execute query
        results = conn.execute(query, params).fetchall()
        
        # Return a set of (meeting_key, session_key) tuples
        return [row[1] for row in results]

    except Exception as e:
        logger.error(f"Error getting existing sessions: {e}")
        return []


def delete_data_by_session(collection_name: str, meeting_key: int, session_key: int, verbose: bool = True) -> int:
    """Deletes documents from a table for a specific session
    
    Args:
        collection_name: Name of the table to delete from
        meeting_key: The meeting key to filter by
        session_key: The session key to filter by
        verbose: Whether to show detailed logging
        
    Returns:
        Number of rows deleted
    """
    conn = _get_duckdb_connection()
    
    try:
        # Check if the table exists first
        table_exists = conn.execute(f"""
            SELECT count(*) FROM information_schema.tables 
            WHERE table_name = '{collection_name}'
        """).fetchone()[0]
        
        if not table_exists:
            if verbose:
                logger.info(f"Collection {collection_name} does not exist, skipping")
            return 0
            
        # Get the count before deletion
        count_before = conn.execute(f"""
            SELECT count(*) FROM {collection_name} 
            WHERE meeting_key = ? AND session_key = ?
        """, [meeting_key, session_key]).fetchone()[0]
        
        if count_before == 0:
            if verbose:
                logger.info(f"No data found for session {session_key} in collection {collection_name}")
            return 0
        
        # Delete data
        conn.execute(f"""
            DELETE FROM {collection_name} 
            WHERE meeting_key = ? AND session_key = ?
        """, [meeting_key, session_key])
        
        # Get count after deletion for verification
        count_after = conn.execute(f"""
            SELECT count(*) FROM {collection_name} 
            WHERE meeting_key = ? AND session_key = ?
        """, [meeting_key, session_key]).fetchone()[0]
        
        rows_deleted = count_before - count_after
        
        if verbose:
            logger.info(f"Deleted {rows_deleted} rows from {collection_name}")
            
        return rows_deleted
        
    except Exception as e:
        logger.error(f"Error deleting from {collection_name}: {e}")
        return 0
