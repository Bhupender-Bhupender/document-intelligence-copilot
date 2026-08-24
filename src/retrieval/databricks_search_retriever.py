"""
Databricks AI Search retrieval adapter.

Runs hybrid retrieval against the managed Databricks AI Search child-chunk
index and converts the SDK response into project-native RetrievedChunk models.

No Databricks SDK type crosses this module boundary.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Sequence

from src.schema.models import DocumentChunk, RetrievedChunk
from src.utils.logging_utils import get_logger


logger = get_logger(__name__)
ParentRowsLoader = Callable[
    [Sequence[str]],
    List[Dict[str, Any]],
]

class DatabricksSearchRetrievalError(Exception):
    """Raised when Databricks AI Search retrieval cannot be completed safely."""


class DatabricksSearchRetriever:
    """Hybrid child-chunk retriever backed by Databricks AI Search."""

    RESULT_COLUMNS = [
        "chunk_id",
        "document_id",
        "page_id",
        "page_number",
        "parent_chunk_id",
        "file_type",
        "file_name",
        "word_count",
        "chunk_level",
        "chunking_version",
        "text",
    ]

    REQUIRED_RESPONSE_COLUMNS = {
        *RESULT_COLUMNS,
        "score",
    }

    def __init__(
        self,
        index_name: str,
        endpoint_name: Optional[str] = None,
        parent_table_name: Optional[str] = None,
        index: Optional[Any] = None,
        parent_rows_loader: Optional[ParentRowsLoader] = None,
    ) -> None:
        if not index_name.strip():
            raise ValueError("index_name must not be blank")

        self.index_name = index_name
        self.endpoint_name = endpoint_name
        self.parent_table_name = parent_table_name
        self._index = index
        self._parent_rows_loader = parent_rows_loader

    def _get_index(self) -> Any:
        """
        Lazily obtain the Databricks AI Search index.

        Import is intentionally deferred so local unit tests do not require
        the Databricks AI Search SDK.
        """
        if self._index is not None:
            return self._index

        try:
            from databricks.ai_search.client import AISearchClient

            client = AISearchClient()

            get_index_kwargs = {
                "index_name": self.index_name,
            }

            if self.endpoint_name:
                get_index_kwargs["endpoint_name"] = self.endpoint_name

            self._index = client.get_index(**get_index_kwargs)
            return self._index

        except Exception as exc:
            raise DatabricksSearchRetrievalError(
                "Unable to connect to the configured Databricks AI Search index."
            ) from exc

    def retrieve(
        self,
        query: str,
        top_k: int = 10,
        filters: Optional[dict] = None,
    ) -> List[RetrievedChunk]:
        """Run Databricks hybrid retrieval and return project-native chunks."""
        if not query or not query.strip():
            raise ValueError("query must not be blank")

        if top_k <= 0:
            raise ValueError("top_k must be greater than zero")

        index = self._get_index()

        try:
            search_kwargs = {
                "query_text": query,
                "columns": self.RESULT_COLUMNS,
                "num_results": top_k,
                "query_type": "hybrid",
            }

            if filters:
                search_kwargs["filters"] = filters

            response = self._index.similarity_search(
                **search_kwargs
            )
        except Exception as exc:
            # Do not include query text in logs or exception messages.
            raise DatabricksSearchRetrievalError(
                "Databricks AI Search hybrid retrieval failed."
            ) from exc

        results = self._parse_response(response)

        logger.debug(
            "databricks_ai_search_retrieval_complete",
            query_chars=len(query),
            requested=top_k,
            returned=len(results),
        )

        return results

    @classmethod
    def _parse_response(
        cls,
        response: Dict[str, Any],
    ) -> List[RetrievedChunk]:
        """Normalize the AI Search manifest/data_array response."""
        try:
            manifest = response["manifest"]
            result = response["result"]
            columns = manifest["columns"]
            rows = result["data_array"]
        except (KeyError, TypeError) as exc:
            raise DatabricksSearchRetrievalError(
                "Databricks AI Search returned an invalid response structure."
            ) from exc

        column_names = [
            column["name"]
            for column in columns
        ]

        missing = cls.REQUIRED_RESPONSE_COLUMNS - set(column_names)

        if missing:
            raise DatabricksSearchRetrievalError(
                "Databricks AI Search response is missing required columns: "
                + ", ".join(sorted(missing))
            )

        results: List[RetrievedChunk] = []

        for row in rows:
            if len(row) != len(column_names):
                raise DatabricksSearchRetrievalError(
                    "Databricks AI Search row width does not match manifest."
                )

            record = dict(zip(column_names, row))
            results.append(cls._to_retrieved_chunk(record))

        return results

    @staticmethod
    def _to_retrieved_chunk(
        record: Dict[str, Any],
    ) -> RetrievedChunk:
        """Convert one normalized AI Search result to RetrievedChunk."""
        chunk_level = record.get("chunk_level")

        if chunk_level != "child":
            raise DatabricksSearchRetrievalError(
                "Databricks child-chunk index returned a non-child result."
            )

        return RetrievedChunk(
            chunk_id=str(record["chunk_id"]),
            doc_id=str(record["document_id"]),
            page_id=str(record["page_id"]),
            file_name=str(record["file_name"]),
            page_number=_to_integral_int(
                record["page_number"],
                field_name="page_number",
            ),
            section_title=None,
            text=str(record["text"] or ""),
            word_count=_to_integral_int(
                record["word_count"],
                field_name="word_count",
            ),
            retrieval_method="hybrid",
            vector_score=None,
            bm25_score=None,
            fusion_score=_to_optional_float(record.get("score")),
            rerank_score=None,
            parent_chunk_id=record.get("parent_chunk_id") or None,
            file_type=record.get("file_type") or None,
        )

    @staticmethod
    def _to_parent_chunk(
        record: Dict[str, Any],
        ) -> DocumentChunk:
        """Convert one Gold parent row into the project-native model."""

        if record.get("chunk_level") != "parent":
            raise DatabricksSearchRetrievalError(
                "Gold parent table returned a non-parent chunk."
            )

        if record.get("parent_chunk_id") not in (None, ""):
            raise DatabricksSearchRetrievalError(
                "A parent chunk unexpectedly contains a parent_chunk_id."
            )

        return DocumentChunk(
            chunk_id=str(record["chunk_id"]),
            doc_id=str(record["document_id"]),
            page_id=str(record["page_id"]),
            page_number=_to_integral_int(
                record["page_number"],
                field_name="page_number",
            ),
            file_name=str(record["file_name"]),
            file_type=str(record["file_type"]),
            section_title=record.get("section_title") or None,
            text=str(record["text"] or ""),
            word_count=_to_integral_int(
                record["word_count"],
                field_name="word_count",
            ),
            chunk_index=_to_nonnegative_integral_int(
                record["chunk_index"],
                field_name="chunk_index",
            ),
            chunk_level="parent",
            parent_chunk_id=None,
        )


    def lookup_parents(
        self,
        retrieved: List[RetrievedChunk],
    ) -> List[Optional[DocumentChunk]]:
        """
        Resolve parent context deterministically from the Gold parent table.

        Output is aligned one-to-one with the input RetrievedChunk list.
        Missing parent IDs or unresolved parents return None.
        """
        if not retrieved:
            return []

        requested_ids = [
            item.parent_chunk_id
            for item in retrieved
            if item.parent_chunk_id
        ]

        if not requested_ids:
            return [None] * len(retrieved)

        # Preserve deterministic order while avoiding duplicate table lookups.
        unique_ids = list(dict.fromkeys(requested_ids))

        rows = self._load_parent_rows(unique_ids)

        parent_by_id: Dict[str, DocumentChunk] = {}

        for record in rows:
            parent = self._to_parent_chunk(record)

            if parent.chunk_id in parent_by_id:
                raise DatabricksSearchRetrievalError(
                    "Duplicate parent chunk returned from the Gold table."
                )

            parent_by_id[parent.chunk_id] = parent

        results: List[Optional[DocumentChunk]] = []

        for child in retrieved:
            if not child.parent_chunk_id:
                results.append(None)
                continue

            parent = parent_by_id.get(child.parent_chunk_id)

            if parent is not None and parent.doc_id != child.doc_id:
                raise DatabricksSearchRetrievalError(
                    "Child-to-parent document lineage mismatch."
                )

            results.append(parent)

        logger.debug(
            "databricks_parent_lookup_complete",
            requested=len(retrieved),
            unique_parent_ids=len(unique_ids),
            resolved=sum(parent is not None for parent in results),
        )

        return results

    def _load_parent_rows(
        self,
        parent_ids: Sequence[str],
        ) -> List[Dict[str, Any]]:
        """
        Load parent chunks from the Gold Delta table.

        A custom loader can be injected for tests or alternate serving
        runtimes. Otherwise an active Databricks Spark session is used.
        """
        if self._parent_rows_loader is not None:
            return self._parent_rows_loader(parent_ids)

        if not self.parent_table_name or not self.parent_table_name.strip():
            raise DatabricksSearchRetrievalError(
                "Databricks parent chunks table is not configured."
            )

        try:
            from pyspark.sql import SparkSession, functions as F

            spark = SparkSession.getActiveSession()

            if spark is None:
                raise DatabricksSearchRetrievalError(
                    "No active Spark session is available for parent lookup."
                )

            rows = (
                spark.table(self.parent_table_name)
                .filter(F.col("chunk_id").isin(list(parent_ids)))
                .select(
                    "chunk_id",
                    "document_id",
                    "page_id",
                    "page_number",
                    "file_name",
                    "file_type",
                    "section_title",
                    "text",
                    "word_count",
                    "chunk_index",
                    "chunk_level",
                    "parent_chunk_id",
                )
                .collect()
            )

            return [
                row.asDict(recursive=False)
                for row in rows
            ]

        except DatabricksSearchRetrievalError:
            raise

        except Exception as exc:
            raise DatabricksSearchRetrievalError(
                "Failed to load parent chunks from the configured Gold table."
            ) from exc

def _to_integral_int(value: Any, field_name: str) -> int:
    """
    Normalize integer metadata returned by AI Search.

    The SDK can deserialize integer index columns as Python floats
    (for example 12.0), so integral floats are accepted.
    """
    if isinstance(value, bool) or value is None:
        raise DatabricksSearchRetrievalError(
            f"{field_name} must contain an integer value."
        )

    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise DatabricksSearchRetrievalError(
            f"{field_name} must contain an integer value."
        ) from exc

    if not numeric.is_integer():
        raise DatabricksSearchRetrievalError(
            f"{field_name} must contain an integral value."
        )

    return int(numeric)


def _to_optional_float(value: Any) -> Optional[float]:
    """Normalize an optional numeric score."""
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise DatabricksSearchRetrievalError(
            "AI Search score must be numeric."
        ) from exc

def _to_nonnegative_integral_int(
        value: Any,
        field_name: str,
    ) -> int:
        if isinstance(value, bool) or value is None:
            raise DatabricksSearchRetrievalError(
                f"{field_name} must contain an integer value."
            )

        try:
            numeric = float(value)
        except (TypeError, ValueError) as exc:
            raise DatabricksSearchRetrievalError(
                f"{field_name} must contain an integer value."
            ) from exc

        if not numeric.is_integer() or numeric < 0:
            raise DatabricksSearchRetrievalError(
                f"{field_name} must contain a nonnegative integral value."
            )

        return int(numeric)