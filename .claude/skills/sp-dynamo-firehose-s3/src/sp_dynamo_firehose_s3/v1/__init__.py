"""Version 1 of the skill: its actions, config schema, generator, and templates.

Everything v1 needs lives in this package, so later versions can change freely
without affecting it.
"""

from sp_dynamo_firehose_s3.v1.cli import main
from sp_dynamo_firehose_s3.v1.generator import ProjectInitError, generate

__all__ = ["ProjectInitError", "generate", "main"]
