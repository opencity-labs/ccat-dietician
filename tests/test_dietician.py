"""
Regression tests for the "hash changed" branch of before_rabbithole_stores_documents
and for remove_documents_by_metadata.

They run against a real in-memory Qdrant (qdrant-client) and a real SQLite database,
with a minimal stub of the Cheshire Cat around them.

Run from the plugin root:

    pip install -r tests/requirements.txt
    python -m unittest discover -s tests -v

NOTE: the Cheshire Cat imports every .py file of a plugin folder (recursively),
so this module must have NO side effects at import time: stubs, sys.modules
patching and third-party imports all happen inside setUpModule(), which only
the test runner calls.
"""
import hashlib
import importlib
import json
import os
import sys
import tempfile
import types
import unittest
import uuid

D = None  # the dietician module, imported in setUpModule
Document = None
EVENTS = []  # structured log events emitted by the plugin
VECTOR_SIZE = 4


def setUpModule():
    global D, Document

    from qdrant_client import QdrantClient  # noqa: F401  (fail early if missing)

    class _Log:
        def _record(self, msg, *args, **kwargs):
            try:
                EVENTS.append(json.loads(msg))
            except (TypeError, ValueError):
                pass

        info = warning = error = debug = critical = _record

    class _Document:
        def __init__(self, page_content, metadata=None):
            self.page_content = page_content
            self.metadata = metadata or {}

    stubs = {
        "cat": {},
        "cat.log": {"log": _Log()},
        "cat.mad_hatter": {},
        "cat.mad_hatter.decorators": {
            "hook": lambda *a, **kw: (lambda f: f),
            "plugin": lambda f: f,
        },
        "cat.looking_glass": {},
        "cat.looking_glass.stray_cat": {"StrayCat": object},
        "langchain": {},
        "langchain.docstore": {},
        "langchain.docstore.document": {"Document": _Document},
    }
    for name, attrs in stubs.items():
        module = types.ModuleType(name)
        for key, value in attrs.items():
            setattr(module, key, value)
        sys.modules[name] = module

    Document = _Document
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    D = importlib.import_module("dietician")


class FakeDeclarative:
    """Same interface as cat.memory.vector_memory_collection.VectorMemoryCollection (Cat 1.9)."""

    def __init__(self):
        from qdrant_client import QdrantClient
        from qdrant_client.models import Distance, VectorParams

        self.collection_name = "declarative"
        self.client = QdrantClient(":memory:")
        self.client.create_collection(
            self.collection_name,
            vectors_config=VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE),
        )
        self.fail_delete = False

    def _qdrant_filter_from_dict(self, filter):
        from qdrant_client.models import FieldCondition, Filter, MatchValue

        if not filter:
            return None
        return Filter(must=[
            FieldCondition(key=f"metadata.{k}", match=MatchValue(value=v))
            for k, v in filter.items()
        ])

    def delete_points(self, points_ids):
        if self.fail_delete:
            raise RuntimeError("qdrant unavailable")
        return self.client.delete(collection_name=self.collection_name, points_selector=points_ids)

    def add(self, text, source):
        """What the RabbitHole does with each chunk returned by the hook."""
        from qdrant_client.models import PointStruct

        self.client.upsert(self.collection_name, [PointStruct(
            id=str(uuid.uuid4()),
            vector=[1.0] * VECTOR_SIZE,
            payload={"page_content": text, "metadata": {"source": source}},
        )])

    def texts(self, source):
        return sorted(p.payload["page_content"] for p in self.points(source))

    def points(self, source):
        from qdrant_client.models import FieldCondition, Filter, MatchValue

        flt = Filter(must=[FieldCondition(key="metadata.source", match=MatchValue(value=source))])
        points, _ = self.client.scroll(self.collection_name, scroll_filter=flt, limit=100000)
        return points


class FakeCat:
    """CheshireCat-like instance (no working_memory)."""

    def __init__(self, db_path):
        settings = {"sqlite_db_path": f"sqlite:///{db_path}"}
        self.memory = types.SimpleNamespace(vectors=types.SimpleNamespace(declarative=FakeDeclarative()))
        self.mad_hatter = types.SimpleNamespace(
            get_plugin=lambda: types.SimpleNamespace(load_settings=lambda: settings)
        )


class DieticianTestCase(unittest.TestCase):

    def setUp(self):
        EVENTS.clear()
        self.tmp = tempfile.TemporaryDirectory()
        self.cat = FakeCat(os.path.join(self.tmp.name, "dietician.db"))
        self.memory = self.cat.memory.vectors.declarative

    def tearDown(self):
        if D.engine is not None:
            D.engine.dispose()
        self.tmp.cleanup()

    def ingest(self, source, chunks):
        """Simulate a RabbitHole ingestion: returns the chunks the plugin let through."""
        full_text = "\n".join(chunks)
        D.before_rabbithole_splits_text([Document(full_text, {"source": source})], self.cat)
        allowed = D.before_rabbithole_stores_documents(
            [Document(c, {"source": source}) for c in chunks], self.cat
        )
        for doc in allowed:
            self.memory.add(doc.page_content, source)
        return allowed

    def stored_hash(self, source):
        from sqlalchemy.orm import Session

        with Session(D.engine) as session:
            return session.query(D.DietDocument).filter_by(name=source).one().hash

    def events(self, name):
        return [e for e in EVENTS if e["event"] == name]

    @staticmethod
    def sha(chunks):
        return hashlib.sha256("\n".join(chunks).encode()).hexdigest()


class TestHashChange(DieticianTestCase):

    def test_page_with_more_than_10_chunks_only_reembeds_changed_chunk(self):
        # Before 0.2.6 scroll() returned only 10 points: chunks after the 10th were
        # re-embedded at every run and the old copies were never deleted.
        body = [f"chunk {i}" for i in range(25)]
        self.assertEqual(len(self.ingest("u", ["header 0"] + body[1:])), 25)

        for run in range(1, 6):
            allowed = self.ingest("u", [f"header {run}"] + body[1:])
            self.assertEqual(len(allowed), 1, f"run {run}")
            self.assertEqual(len(self.memory.points("u")), 25, f"run {run}")

        self.assertEqual(self.ingest("u", ["header 5"] + body[1:]), [])
        self.assertEqual(self.memory.texts("u"), sorted(["header 5"] + body[1:]))

    def test_hash_is_updated_after_change(self):
        # In 0.2.3 the hash was never updated: the page looked "changed" forever.
        self.ingest("u", ["a", "b"])
        self.ingest("u", ["a", "c"])
        self.assertEqual(self.stored_hash("u"), self.sha(["a", "c"]))
        self.assertEqual(self.ingest("u", ["a", "c"]), [])
        self.assertTrue(self.events("ingestion_unchanged"))

    def test_hash_conflict_keeps_page_content(self):
        # In 0.2.5 the UNIQUE hash raised IntegrityError after delete_points: the page was emptied.
        self.ingest("a", ["same 1", "same 2"])
        self.ingest("b", ["other 1", "other 2"])

        allowed = self.ingest("b", ["same 1", "same 2"])

        self.assertEqual([d.page_content for d in allowed], ["same 1", "same 2"])
        self.assertEqual(self.memory.texts("b"), ["same 1", "same 2"])
        self.assertEqual(self.stored_hash("b"), self.sha(["other 1", "other 2"]))
        self.assertEqual(self.events("hash_conflict")[0]["data"]["hash_owner"], "a")
        self.assertFalse(self.events("ingestion_error"))

        # Known trade-off: the page keeps being reprocessed, but nothing is re-embedded or duplicated.
        self.assertEqual(self.ingest("b", ["same 1", "same 2"]), [])
        self.assertEqual(self.memory.texts("b"), ["same 1", "same 2"])

    def test_preexisting_duplicates_are_removed(self):
        chunks = [f"chunk {i}" for i in range(12)]
        self.ingest("u", chunks)
        for i in range(11):  # 11 duplicates left by previous versions
            self.memory.add(f"chunk {i}", "u")
        self.assertEqual(len(self.memory.points("u")), 23)

        allowed = self.ingest("u", chunks[:-1] + ["changed"])

        self.assertEqual([d.page_content for d in allowed], ["changed"])
        self.assertEqual(self.memory.texts("u"), sorted(chunks[:-1] + ["changed"]))
        self.assertEqual(self.events("cleanup_duplicates_removed")[0]["data"]["count"], 11)

    def test_repeated_chunk_in_new_version_is_ingested_once(self):
        self.ingest("u", ["a", "b"])
        allowed = self.ingest("u", ["a", "footer", "footer"])
        self.assertEqual([d.page_content for d in allowed], ["footer"])

    def test_failed_vector_delete_restores_old_hash(self):
        self.ingest("u", ["a", "b"])
        self.memory.fail_delete = True

        self.assertEqual(self.ingest("u", ["a", "c"]), [])
        self.assertEqual(self.stored_hash("u"), self.sha(["a", "b"]))
        self.assertTrue(self.events("ingestion_error"))

        # Qdrant is back: the page must be processed again, not seen as "unchanged".
        self.memory.fail_delete = False
        self.assertEqual([d.page_content for d in self.ingest("u", ["a", "c"])], ["c"])
        self.assertEqual(self.memory.texts("u"), ["a", "c"])


class TestRemoveDocumentsByMetadata(DieticianTestCase):

    def test_removes_all_points_beyond_page_size(self):
        self.ingest("u", [f"chunk {i}" for i in range(25)])
        self.ingest("keep", ["k"])

        result = D.remove_documents_by_metadata(
            self.cat, {}, exclude_sources=["keep"], qdrant_limit=10
        )

        self.assertEqual(result["vector_removed_count"], 25)
        self.assertEqual(self.memory.points("u"), [])
        self.assertEqual(self.memory.texts("keep"), ["k"])


if __name__ == "__main__":
    unittest.main()
