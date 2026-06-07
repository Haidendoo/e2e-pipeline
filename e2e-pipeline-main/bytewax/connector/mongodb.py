"""MongoDB DynamicSink implementation for Bytewax."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any

from bytewax.outputs import DynamicSink, StatelessSinkPartition
from pymongo import MongoClient


LOGGER = logging.getLogger("bytewax-mongo-sink")


class MongoSinkPartition(StatelessSinkPartition):
    """Worker-local sink partition that writes event batches to MongoDB."""

    def __init__(self, uri: str, db_name: str, collection_name: str) -> None:
        self._client = MongoClient(uri)
        self._client.admin.command("ping")
        self._collection = self._client[db_name][collection_name]
        LOGGER.info("Mongo sink connected to collection=%s.%s", db_name, collection_name)

    def write_batch(self, items: list[dict[str, Any]]) -> None:
        docs: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        for event in items:
            payload = dict(event)
            payload["_ingested_at"] = now
            docs.append(payload)

        if docs:
            self._collection.insert_many(docs)
            LOGGER.info("Mongo sink inserted batch size=%s", len(docs))


class MongoSink(DynamicSink):
    """Dynamic sink that builds one MongoDB sink partition per worker."""

    def __init__(self, uri: str, db_name: str, collection_name: str) -> None:
        self._uri = uri
        self._db_name = db_name
        self._collection_name = collection_name

    def build(self, _step_id: str, _worker_index: int, _worker_count: int) -> StatelessSinkPartition:
        return MongoSinkPartition(self._uri, self._db_name, self._collection_name)
