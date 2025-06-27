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
    OVERWRITE = "overwrite"
    """Overwrite what already exists."""
