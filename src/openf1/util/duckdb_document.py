"""
DuckDB-specific extensions for Document class
"""

import hashlib
import json
from datetime import datetime
from typing import Dict, Any


def _generate_record_hash(data: dict, collection_name: str) -> str:
    """Generate a hash ID for a record based on its content"""
    # Create a normalized version of the data for hashing
    hash_data = data.copy()
    
    # Sort keys to ensure consistent hashing
    sorted_data = json.dumps(hash_data, sort_keys=True, default=_json_serializer)
    
    # Create SHA-256 hash
    hash_object = hashlib.sha256(sorted_data.encode('utf-8'))
    hash_hex = hash_object.hexdigest()
    
    # Return first 16 characters for a shorter but still unique ID
    return f"{collection_name}_{hash_hex[:16]}"


def to_duckdb_doc_sync(document_instance) -> dict:
    """Converts a Document instance to a dictionary suitable for DuckDB storage.
    
    This function replaces the MongoDB-specific to_mongo_doc_sync method.
    Uses natural primary keys instead of generic _id/_key fields.
    """

    doc_dict = document_instance.__dict__.copy()

    # Flatten nested structures and handle special types
    flattened_doc = _flatten_for_duckdb(doc_dict)
    
    flattened_doc['_id'] = document_instance.__hash__()
    
    return flattened_doc


def _flatten_for_duckdb(data: Any, prefix: str = "") -> Dict[str, Any]:
    """Recursively flatten nested dictionaries and convert complex types for DuckDB"""
    result = {}
    
    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{prefix}{key}" if prefix else key
            if isinstance(value, dict):
                # Recursively flatten nested dictionaries
                result.update(_flatten_for_duckdb(value, f"{new_key}_"))
            elif isinstance(value, list):
                # Convert lists to JSON strings
                result[new_key] = json.dumps(value, default=_json_serializer)
            elif isinstance(value, datetime):
                # Ensure datetime has timezone info
                if value.tzinfo is None:
                    from datetime import timezone
                    value = value.replace(tzinfo=timezone.utc)
                result[new_key] = value
            else:
                result[new_key] = value
    else:
        # If data is not a dict, return as is
        if prefix:
            result[prefix.rstrip('_')] = data
        else:
            result['value'] = data
    
    return result


def _json_serializer(obj):
    """JSON serializer for special types"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif hasattr(obj, '__dict__'):
        return obj.__dict__
    else:
        return str(obj)


def get_primary_key_fields(collection_name: str) -> list[str]:
    """Get the primary key field(s) for a given collection/table"""
    primary_keys = {}
    #     'sessions': ['session_key'],
    #     'meetings': ['meeting_key'],
    #     'drivers': ['driver_number', 'session_key'],  # Composite key
    #     'laps': ['session_key', 'driver_number', 'lap_number'],  # Composite key
    #     'car_data': ['session_key', 'driver_number', 'date'],  # Composite key with timestamp
    #     'position': ['session_key', 'driver_number', 'date'],  # Composite key with timestamp
    #     'intervals': ['session_key', 'driver_number', 'date'],  # Composite key with timestamp
    #     'pit': ['session_key', 'driver_number', 'date'],  # Composite key with timestamp
    #     'stints': ['session_key', 'driver_number', 'stint_number'],  # Composite key
    #     'team_radio': ['session_key', 'driver_number', 'date'],  # Composite key with timestamp
    #     'weather': ['session_key', 'date'],  # Composite key with timestamp
    #     'race_control': ['record_hash'],  # Use hash of the entire record to avoid duplicates
    #     'location': ['session_key', 'driver_number', 'date'],  # Composite key with timestamp
    # }

    # Default to using _id if collection is not mapped
    return primary_keys.get(collection_name, ['_id'])


def get_table_schema(collection_name: str, sample_doc: dict) -> dict:
    """Generate table schema with appropriate primary key constraints"""
    primary_key_fields = get_primary_key_fields(collection_name)
    
    # Special fields that are known to have mixed types (numbers and strings)
    mixed_type_fields = {
        'intervals': ['gap_to_leader'],  # Can be numeric (seconds) or string ("+X LAP")
        # Add other collections and fields as needed
    }
    
    mixed_fields = mixed_type_fields.get(collection_name, [])
    
    columns = []
    constraints = []
    
    for key, value in sample_doc.items():
        # Determine column type
        if key == 'record_hash':
            # Special handling for record_hash field (used as primary key for race_control)
            col_type = "VARCHAR"
        elif key in mixed_fields:
            # Force VARCHAR for fields known to have mixed types
            col_type = "VARCHAR"
        elif isinstance(value, str):
            col_type = "VARCHAR"
        elif isinstance(value, int):
            col_type = "BIGINT"
        elif isinstance(value, float):
            col_type = "DOUBLE"
        elif isinstance(value, bool):
            col_type = "BOOLEAN"
        elif isinstance(value, datetime):
            col_type = "TIMESTAMP"
        else:
            # Default to VARCHAR for complex types, store as JSON
            col_type = "VARCHAR"
        
        # Add NOT NULL constraint for primary key fields
        if key in primary_key_fields:
            col_type += " NOT NULL"
        
        columns.append(f"{key} {col_type}")
    
    # Add primary key constraint
    if len(primary_key_fields) == 1:
        constraints.append(f"PRIMARY KEY ({primary_key_fields[0]})")
    elif len(primary_key_fields) > 1:
        constraints.append(f"PRIMARY KEY ({', '.join(primary_key_fields)})")
    
    return {
        'columns': columns,
        'constraints': constraints,
        'primary_keys': primary_key_fields
    }


# Monkey patch the Document class to add DuckDB support
def patch_document_class():
    """Add DuckDB support to the Document class"""
    from openf1.services.ingestor_livetiming.core.objects import Document
    
    # Add the new method to the Document class
    Document.to_duckdb_doc_sync = lambda self: to_duckdb_doc_sync(self)
