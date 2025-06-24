"""
Common enums
"""
import enum


class ConflictResolution(enum.StrEnum):
    """
    Specifies what to do if data already exists
    """
    ERROR = "error"
    """Fail and notify the user"""
    APPEND = "append"
    """Add to whatever exists. Will possibly add duplicate records"""
    APPEND_NEW = "append_new"
    """Only append new records - requires either a PK or a unique constraint."""
    OVERWRITE = "overwrite"
    """Overwrite what already exists."""
