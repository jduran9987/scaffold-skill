"""Environment placeholder handling shared by the pipeline's jobs."""

ENV_PLACEHOLDER = "{env}"


def resolve_env(value: str, env: str) -> str:
    """Replaces the environment placeholder in a value, if it has one.

    Args:
        value: Path or table name that may contain the `{env}` placeholder.
        env: Environment name to substitute.

    Returns:
        The value with every placeholder replaced by the environment name.
    """
    return value.replace(ENV_PLACEHOLDER, env)
