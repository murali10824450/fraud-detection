from __future__ import annotations

import os
from typing import Optional, Dict

import yaml
from pyspark.sql import SparkSession


def _load_yaml(path: str = "spark.yaml") -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_spark(
    app_name: str,
    profile: str = "dev",
    extra_conf: Optional[Dict[str, str]] = None,
) -> SparkSession:
    """
    Create or get a SparkSession configured via spark.yaml.
    Profile precedence: default -> profile -> SPARK_PROFILE env -> extra_conf.
    """
    cfg = _load_yaml()
    merged: Dict[str, str] = {}
    merged.update(cfg.get("default", {}) or {})

    profiles = cfg.get("profiles") or {}
    if profile in profiles:
        merged.update(profiles[profile] or {})

    env_profile = os.getenv("SPARK_PROFILE")
    if env_profile and env_profile in profiles:
        merged.update(profiles[env_profile] or {})

    if extra_conf:
        merged.update(extra_conf)

    builder = SparkSession.builder.appName(app_name)
    for k, v in merged.items():
        builder = builder.config(k, str(v))

    spark = builder.getOrCreate()

    # Print a short summary
    active = dict(spark.sparkContext.getConf().getAll())
    keys = (
        "spark.master",
        "spark.executor.cores",
        "spark.executor.memory",
        "spark.driver.memory",
        "spark.sql.shuffle.partitions",
        "spark.sql.adaptive.enabled",
    )
    for key in keys:
        if key in active:
            print(f"[conf] {key} = {active[key]}")

    return spark