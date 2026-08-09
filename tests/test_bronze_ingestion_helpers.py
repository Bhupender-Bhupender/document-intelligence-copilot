from databricks.src.bronze_ingestion import (
    document_id_from_hash,
    sha256_file,
)


def test_sha256_is_deterministic(tmp_path):
    path = tmp_path / "sample.txt"
    path.write_text("same content", encoding="utf-8")

    first = sha256_file(path)
    second = sha256_file(path)

    assert first == second
    assert len(first) == 64


def test_different_content_has_different_hash(tmp_path):
    first = tmp_path / "first.txt"
    second = tmp_path / "second.txt"

    first.write_text("alpha", encoding="utf-8")
    second.write_text("beta", encoding="utf-8")

    assert sha256_file(first) != sha256_file(second)


def test_document_id_is_stable():
    file_hash = "a" * 64

    assert (
        document_id_from_hash(file_hash)
        == "doc_" + ("a" * 16)
    )
