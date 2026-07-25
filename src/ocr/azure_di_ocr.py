"""
Azure AI Document Intelligence OCR adapter.

Boundary 1 (OCR) — adapter implementation.

Public API
----------
    AzureDiOcrAdapter.recover_pages(
        file_path: Path,
        empty_pages: List[ParsedPage],
    ) -> List[ParsedPage]

Design
------
This adapter is a strict boundary component:
  - Input:  project-native types (Path, List[ParsedPage])
  - Output: project-native types (List[ParsedPage])
  - No Azure SDK types cross this boundary

The adapter sends the full PDF file to Azure AI Document Intelligence
(prebuilt-read model) in a single API call and maps per-page results back
to ParsedPage records. Azure DI exposes per-word confidence scores, so
ocr_confidence is populated as the mean word confidence for each recovered
page — unlike the Docling/RapidOCR path where confidence is unavailable.

Import design
-------------
azure.ai.documentintelligence and azure.identity are imported only inside
__init__ and recover_pages. The module is importable without those packages
installed (local OCR backend does not require them).

Test bypass pattern
-------------------
Use object.__new__(AzureDiOcrAdapter) to bypass __init__, then assign
adapter._client = mock_client directly. This avoids Azure SDK imports in
the test environment entirely, identical to the BlobArtifactWriter pattern.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, List, Optional

from src.schema.models import ParsedPage
from src.utils.logging_utils import get_logger
from src.utils.text_utils import classify_extraction_status, clean_text, normalize_text

logger = get_logger(__name__)


class AzureDiOcrError(Exception):
    """Raised when the Azure AI Document Intelligence call fails."""


class AzureDiOcrAdapter:
    """
    OCR adapter backed by Azure AI Document Intelligence (prebuilt-read).

    Instantiate once per request or once per process (the underlying client
    is thread-safe). Use object.__new__ + direct attribute assignment to
    bypass __init__ in tests — the same pattern used by BlobArtifactWriter.
    """

    def __init__(self, endpoint: str, credential: Any = None) -> None:
        """
        Args:
            endpoint:   Azure AI Document Intelligence resource endpoint URL
                        (https://<resource>.cognitiveservices.azure.com/).
            credential: Optional Azure credential. Defaults to
                        DefaultAzureCredential() (Managed Identity compatible).

        Raises:
            AzureDiOcrError: If azure-ai-documentintelligence is not installed.
        """
        try:
            from azure.ai.documentintelligence import (  # type: ignore[import]
                DocumentIntelligenceClient,
            )
            from azure.ai.documentintelligence.models import (  # type: ignore[import]
                AnalyzeDocumentRequest,
            )
        except ImportError as exc:
            raise AzureDiOcrError(
                "azure-ai-documentintelligence is not installed. "
                "Add it to requirements-full.txt and reinstall."
            ) from exc

        if credential is None:
            try:
                from azure.identity import DefaultAzureCredential  # type: ignore[import]

                credential = DefaultAzureCredential()
            except ImportError as exc:
                raise AzureDiOcrError(
                    "azure-identity is not installed. "
                    "Add it to requirements-full.txt and reinstall."
                ) from exc

        self._client = DocumentIntelligenceClient(
            endpoint=endpoint,
            credential=credential,
        )
        # Stored so tests can inject a fake without needing the SDK installed.
        self._AnalyzeDocumentRequest = AnalyzeDocumentRequest

    def recover_pages(
        self,
        file_path: Path,
        empty_pages: List[ParsedPage],
    ) -> List[ParsedPage]:
        """
        Attempt OCR recovery for empty pages using Azure DI prebuilt-read.

        Sends the full file to Azure DI in a single API call, then maps
        per-page results back to the provided ParsedPage objects.

        Identity fields (page_id, doc_id, page_number) are never modified.
        If Azure DI returns no content for a page, that page is returned
        unchanged (original object, extraction_status remains "empty").

        Azure DI pages are 1-indexed and match the page_number convention
        used throughout this pipeline.

        Args:
            file_path:   Path to the PDF or image file.
            empty_pages: Pages with extraction_status="empty" to recover.

        Returns:
            List of ParsedPage in the same order as empty_pages.
            Each recovered page is a new object with updated fields.
            Pages not found in the Azure DI result are the original objects.

        Raises:
            AzureDiOcrError: If the Azure DI API call fails.
        """
        if not empty_pages:
            return []

        try:
            file_bytes = file_path.read_bytes()
            poller = self._client.begin_analyze_document(
                "prebuilt-read",
                self._AnalyzeDocumentRequest(bytes_source=file_bytes),
            )
            result = poller.result()
        except AzureDiOcrError:
            raise
        except Exception as exc:
            raise AzureDiOcrError(
                f"Azure DI API call failed for {file_path.name!r}: {exc}"
            ) from exc

        # Build a lookup: {1-based page_number -> (raw_text, mean_confidence)}
        page_data: dict[int, tuple[str, Optional[float]]] = {}
        for di_page in result.pages or []:
            page_no: int = di_page.page_number
            words = di_page.words or []
            lines = di_page.lines or []

            # Reconstruct raw text from lines — preserves natural line structure.
            raw_text = "\n".join(
                line.content for line in lines if line.content
            )

            # Mean word confidence — None when no words are present.
            confidences = [
                w.confidence
                for w in words
                if w.confidence is not None
            ]
            mean_conf: Optional[float] = (
                sum(confidences) / len(confidences) if confidences else None
            )

            page_data[page_no] = (raw_text, mean_conf)

        # Map Azure DI results back to ParsedPage records, preserving order.
        result_pages: List[ParsedPage] = []
        for original in empty_pages:
            page_no = original.page_number

            if page_no not in page_data:
                logger.debug(
                    "azure_di_ocr: Azure DI produced no page for this number — keeping empty",
                    page_number=page_no,
                    file=file_path.name,
                )
                result_pages.append(original)
                continue

            raw_text, mean_conf = page_data[page_no]
            norm = normalize_text(raw_text)
            recovered_status = classify_extraction_status(norm)

            updated = ParsedPage(
                # identity — never changed
                page_id=original.page_id,
                doc_id=original.doc_id,
                page_number=original.page_number,
                # recovered text content
                raw_text=clean_text(raw_text),
                normalized_text=norm,
                word_count=len(norm.split()) if norm.strip() else 0,
                char_count=len(norm),
                # OCR metadata
                parse_method="azure_di",
                extraction_status=recovered_status,
                ocr_engine="azure-document-intelligence",
                # Azure DI exposes per-word confidence; populate it.
                ocr_confidence=mean_conf,
                # prebuilt-read does not expose block-level layout structure.
                section_title=None,
                layout_blocks=[],
            )

            result_pages.append(updated)

            logger.debug(
                "azure_di_ocr: page recovered",
                page_number=page_no,
                status=recovered_status,
                word_count=updated.word_count,
                confidence=mean_conf,
            )

        return result_pages
