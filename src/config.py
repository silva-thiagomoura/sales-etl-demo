"""
config.py

This module is responsible for loading application configuration.

Key goals:
- Keep database credentials OUT of the code and OUT of the YAML file.
- Load sensitive values (DB password, encryption key) from environment variables.
- Return a single config dictionary used by the rest of the ETL pipeline.

Why this design?
- It allows the same code to run in different environments (DEV/UAT/PROD)
  by changing only the environment variables and the config.yaml file.
- It improves security by avoiding hard-coded secrets.
"""

import os
import yaml
from dotenv import load_dotenv

# Loads variables from a local ".env" file into the process environment.
# This is very useful for local development.
# In production, environment variables are typically injected by the runtime
# (Docker, Kubernetes, CI/CD pipelines, etc.).
load_dotenv()


def load_config(path: str = "config.yaml") -> dict:
    """
    Loads the ETL configuration from a YAML file and enriches it with secrets
    stored in environment variables.

    Parameters
    ----------
    path : str
        Path to the YAML configuration file (default: "config.yaml").

    Returns
    -------
    dict
        A fully resolved configuration dictionary containing:
        - Database connection parameters
        - Security parameters (encryption key)

    Raises
    ------
    FileNotFoundError
        If the YAML file does not exist.
    ValueError
        If required environment variables are missing.
    """

    # Read YAML configuration (non-sensitive settings).
    # Example: host, port, db name, username, etc.
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)

    # The YAML contains the NAME of the env var that holds the password.
    # Example in YAML:
    #   password_env: DB_PASSWORD
    #
    # This prevents storing real passwords inside config.yaml.
    db_password_env_var = cfg["db"]["password_env"]
    db_password = os.getenv(db_password_env_var)

    if not db_password:
        raise ValueError(
            f"DB password not found in env var: {db_password_env_var}. "
            "Make sure it is defined in your environment or in the .env file."
        )

    # Encryption key used to protect sensitive fields.
    # NOTE: This is required for the security part of the project.
    # It should never be committed to Git.
    encryption_key = os.getenv("ENCRYPTION_KEY")

    if not encryption_key:
        raise ValueError(
            "ENCRYPTION_KEY not found in environment variables. "
            "Please define it in your .env file or export it in your shell."
        )

    # Inject resolved secrets into the config dictionary so the rest of the ETL
    # can use cfg["db"]["password"] directly.
    cfg["db"]["password"] = db_password

    # Keep security settings grouped in a dedicated section.
    cfg["security"] = {"encryption_key": encryption_key}

    return cfg
