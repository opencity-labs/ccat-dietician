import hashlib
import os
import json
from typing import List
from cat.log import log
from cat.mad_hatter.decorators import hook, plugin
from langchain.docstore.document import Document
from sqlalchemy import ForeignKey, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, Session
from cat.looking_glass.stray_cat import StrayCat


class Base(DeclarativeBase):
    pass


class DietDocument(Base):
    __tablename__= 'document'
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(256), unique=True)
    hash: Mapped[str] = mapped_column(String(64), unique=True)
    
    chunks: Mapped[List["Chunk"]] = relationship(back_populates="document", cascade="all, delete-orphan")


    def __repr__(self) -> str:
        return f'DietDocument(name={self.name!r}, hash={self.hash!r})'


class Chunk(Base):
    __tablename__ = 'chunk'
    id: Mapped[int] = mapped_column(primary_key=True)
    chunk_count: Mapped[int]
    document_id: Mapped[int] = mapped_column(ForeignKey("document.id"))

    document: Mapped["DietDocument"] = relationship(back_populates="chunks")

    def __repr__(self) -> str:
        return f'Chunk(chunk_count={self.chunk_count!r})'


# sqlalchemy sqlite engine
engine = None
# Base.metadata.create_all(engine, checkfirst=True)
# log.warning(f"Dietician is using a sqlite file located here: {DEFAULT_SQLITE_FILEPATH}. You can change the path in the plugin settings.")


@hook(priority=10)
def before_rabbithole_splits_text(doc, cat):
    # doc is a list with only one element, always
    dietician_data = {
        'name': doc[0].metadata['source'],
        'hash': hashlib.sha256(doc[0].page_content.encode()).hexdigest()
    }
    
    # Handle both CheshireCat (no working_memory) and StrayCat instances
    if hasattr(cat, 'working_memory'):
        # StrayCat instance - use working_memory
        cat.working_memory.ccat_dietician = dietician_data
    else:
        # CheshireCat instance - store on the cat instance directly
        cat._ccat_dietician_temp = dietician_data
        # log.debug("Dietician: Using temporary storage on CheshireCat instance (no working_memory available)")

    global engine
    db_filepath = cat.mad_hatter.get_plugin().load_settings()["sqlite_db_path"]
    engine = create_engine(db_filepath)
    Base.metadata.create_all(engine, checkfirst=True)
    # log.debug(json.dumps({
    #     "component": "ccat_dietician",
    #     "event": "db_init",
    #     "data": {
    #         "db_path": db_filepath
    #     }
    # }))

    return doc


@hook(priority=10)
def before_rabbithole_stores_documents(docs: List[Document], cat) -> List[Document]:
    # Handle both CheshireCat (no working_memory) and StrayCat instances
    if hasattr(cat, 'working_memory'):
        # StrayCat instance - use working_memory
        cat.working_memory.ccat_dietician['chunk_count'] = len(docs)
        dietician_data = cat.working_memory.ccat_dietician
    else:
        # CheshireCat instance - use temporary storage
        if not hasattr(cat, '_ccat_dietician_temp'):
            log.error(json.dumps({
                "component": "ccat_dietician",
                "event": "ingestion_error",
                "data": {
                    "error": "No temporary data found on CheshireCat instance"
                }
            }))
            return docs
        cat._ccat_dietician_temp['chunk_count'] = len(docs)
        dietician_data = cat._ccat_dietician_temp
        # log.debug("Dietician: Using temporary storage from CheshireCat instance")

    with Session(engine) as session:
        try:
            doc_by_name = session.query(DietDocument).filter_by(name=dietician_data['name']).first()
            # log.debug(f"Dietician check for '{dietician_data['name']}': new_hash={dietician_data['hash'][:8]}..., db_hash={doc_by_name.hash[:8] if doc_by_name else 'None'}...")
            if doc_by_name is None:
                doc_by_hash = session.query(DietDocument).filter_by(hash=dietician_data['hash']).first()
                if doc_by_hash is None:
                        db_doc = DietDocument(name=dietician_data['name'], hash=dietician_data['hash'], chunks=[Chunk(chunk_count=dietician_data['chunk_count'])])
                        session.add(db_doc)
                        session.commit()
                        log.info(json.dumps({
                            "component": "ccat_dietician",
                            "event": "ingestion_new",
                            "data": {
                                "source": dietician_data['name'],
                                "chunk_count": dietician_data['chunk_count']
                            }
                        }))
                        return docs
                else:
                    if dietician_data['chunk_count'] in [c.chunk_count for c in doc_by_hash.chunks]:
                        log.info(json.dumps({
                            "component": "ccat_dietician",
                            "event": "ingestion_duplicate",
                            "data": {
                                "source": dietician_data['name'],
                                "original_source": doc_by_hash.name
                            }
                        }))
                        return []
                    else:
                        doc_by_hash.chunks.append(Chunk(chunk_count=dietician_data['chunk_count']))
                        session.add(doc_by_hash)
                        session.commit()
                        log.info(json.dumps({
                            "component": "ccat_dietician",
                            "event": "ingestion_rechunk",
                            "data": {
                                "source": dietician_data['name'],
                                "original_source": doc_by_hash.name,
                                "chunk_count": dietician_data['chunk_count']
                            }
                        }))
                        return docs
            else:
                if dietician_data['hash'] == doc_by_name.hash:
                    if dietician_data['chunk_count'] in [c.chunk_count for c in doc_by_name.chunks]:
                        log.info(json.dumps({
                            "component": "ccat_dietician",
                            "event": "ingestion_unchanged",
                            "data": {
                                "source": doc_by_name.name
                            }
                        }))
                        return []
                    else:
                        doc_by_name.chunks.append(Chunk(chunk_count=dietician_data['chunk_count']))
                        session.add(doc_by_name)
                        session.commit()
                        log.info(json.dumps({
                            "component": "ccat_dietician",
                            "event": "ingestion_rechunk",
                            "data": {
                                "source": doc_by_name.name,
                                "chunk_count": dietician_data['chunk_count']
                            }
                        }))
                        return docs
                else:
                    # Read ALL the points of this source: without pagination qdrant returns
                    # only the first 10, so the chunks beyond the 10th were re-embedded at
                    # every run and the old ones were never deleted (growing duplicates).
                    old_chunks = _scroll_all_points(cat, {'source': doc_by_name.name})
                    old_chunks_text = set(c.payload['page_content'] for c in old_chunks)
                    new_chunks_text = set(d.page_content for d in docs)

                    # we have to delete all chunks in declarative memory that are not in the new document because those chunks are related an old version of the document.
                    # We also delete exact duplicates (same page_content stored more than once), keeping a single copy.
                    old_chunks_to_delete_ids = []
                    kept_texts = set()
                    duplicated_count = 0
                    for c in old_chunks:
                        text = c.payload['page_content']
                        if text not in new_chunks_text:
                            old_chunks_to_delete_ids.append(c.id)
                        elif text in kept_texts:
                            old_chunks_to_delete_ids.append(c.id)
                            duplicated_count += 1
                        else:
                            kept_texts.add(text)

                    # Update the stored hash, otherwise the document is seen as "changed" forever.
                    # `hash` is UNIQUE: if another document already owns the new hash we cannot store it,
                    # so we keep the old one instead of failing the commit (which would leave the vector memory
                    # without the deleted chunks and without the new ones).
                    # Known trade-off: while the conflict lasts the stored hash never matches, so at every run
                    # check_should_update() returns True and this branch scrolls the whole source again.
                    # Thanks to the dedup below nothing is re-embedded or duplicated, it's only repeated work.
                    old_hash = doc_by_name.hash
                    hash_owner = session.query(DietDocument).filter(
                        DietDocument.hash == dietician_data['hash'],
                        DietDocument.id != doc_by_name.id
                    ).first()
                    if hash_owner is None:
                        doc_by_name.hash = dietician_data['hash']
                    else:
                        log.warning(json.dumps({
                            "component": "ccat_dietician",
                            "event": "hash_conflict",
                            "data": {
                                "source": doc_by_name.name,
                                "hash_owner": hash_owner.name
                            }
                        }))
                    # Clear old chunk records and add new chunk count (proper SQLAlchemy way)
                    doc_by_name.chunks.clear()
                    doc_by_name.chunks.append(Chunk(chunk_count=dietician_data['chunk_count']))
                    session.commit()

                    if len(old_chunks_to_delete_ids) > 0:
                        try:
                            cat.memory.vectors.declarative.delete_points(old_chunks_to_delete_ids)
                        except Exception:
                            # The new hash is already committed: if the vector memory was not updated,
                            # restore the old hash so the document is reprocessed at the next run
                            # instead of being seen as "unchanged" with stale chunks forever.
                            doc_by_name.hash = old_hash
                            session.commit()
                            raise

                    # docs contain only chunks never inserted in declarative memory, we keep into the vectordb any chunk previously inserted (to avoid unnecessary calls to the embedding model).
                    # Chunks repeated inside the new document are ingested only once.
                    new_chunks_to_ingest = []
                    queued_texts = set()
                    for d in docs:
                        if d.page_content not in old_chunks_text and d.page_content not in queued_texts:
                            new_chunks_to_ingest.append(d)
                            queued_texts.add(d.page_content)

                    if duplicated_count > 0:
                        log.info(json.dumps({
                            "component": "ccat_dietician",
                            "event": "cleanup_duplicates_removed",
                            "data": {
                                "source": doc_by_name.name,
                                "count": duplicated_count
                            }
                        }))

                    if len(new_chunks_to_ingest) > 0:
                        log.info(json.dumps({
                            "component": "ccat_dietician",
                            "event": "ingestion_update",
                            "data": {
                                "source": doc_by_name.name,
                                "new_chunks_count": len(new_chunks_to_ingest),
                                "deleted_chunks_count": len(old_chunks_to_delete_ids)
                            }
                        }))
                    else:
                        log.info(json.dumps({
                            "component": "ccat_dietician",
                            "event": "ingestion_update_skipped",
                            "data": {
                                "source": doc_by_name.name,
                                "message": "Hash changed but all chunks already exist"
                            }
                        }))
                        if len(old_chunks_to_delete_ids) > 0:
                            log.info(json.dumps({
                                "component": "ccat_dietician",
                                "event": "cleanup_vector_removed",
                                "data": {
                                    "count": len(old_chunks_to_delete_ids)
                                }
                            }))

                    return new_chunks_to_ingest

        except Exception as e:
            session.rollback()
            log.error(json.dumps({
                "component": "ccat_dietician",
                "event": "ingestion_error",
                "data": {
                    "source": dietician_data['name'],
                    "error": str(e)
                }
            }))
            return []


def _scroll_all_points(cat, metadata_filter: dict, page_size: int = 256) -> list:
    """Return all the declarative memory points matching metadata_filter, following qdrant pagination."""
    declarative = cat.memory.vectors.declarative
    points = []
    offset = None
    while True:
        page, offset = declarative.client.scroll(
            collection_name=declarative.collection_name,
            scroll_filter=declarative._qdrant_filter_from_dict(metadata_filter),
            with_payload=True,
            with_vectors=False,
            limit=page_size,
            offset=offset
        )
        points.extend(page)
        if offset is None:
            break
    return points


def remove_documents_by_metadata(cat, metadata_filter: dict, exclude_metadata: dict = None, exclude_sources: list = None, qdrant_limit: int = 10000) -> dict:
    """
    Generic function to remove documents from both dietician database and vector memory
    based on metadata filtering.
    
    Args:
        cat: The StrayCat instance
        metadata_filter: Dictionary of metadata key-value pairs to match for removal
        exclude_metadata: Optional dictionary of metadata to exclude from removal
        exclude_sources: Optional list of source URLs to exclude from removal (for pages that were scraped but not re-ingested)
        
    Returns:
        dict: Summary of removal operations with counts and details
    """
    global engine
    
    if engine is None:
        db_filepath = cat.mad_hatter.get_plugin().load_settings()["sqlite_db_path"]
        engine = create_engine(db_filepath)
        Base.metadata.create_all(engine, checkfirst=True)
    
    removed_count = 0
    vector_removed_count = 0
    errors = []
    removed_urls = []
    
    try:
        # Find chunks in vector memory that match the filter criteria
        # qdrant_limit is now the page size: all the matching points are read, following pagination
        all_chunks = _scroll_all_points(cat, metadata_filter, page_size=qdrant_limit)
        
        chunks_to_remove = []
        urls_to_remove_from_db = set()
        
        for chunk in all_chunks:
            should_remove = True
            
            # Get metadata from chunk payload (it's nested under 'metadata' key)
            chunk_metadata = chunk.payload.get('metadata', {})
            chunk_source = chunk_metadata.get('source')
            
            # Check if source is in the exclude_sources list (pages that were scraped, even if not re-ingested)
            if exclude_sources and chunk_source in exclude_sources:
                should_remove = False
                # log.debug(f"Chunk {chunk.id} excluded from removal - source {chunk_source} is in scraped pages list")
            
            # Check if chunk should be excluded based on exclude_metadata
            if should_remove and exclude_metadata:
                for key, value in exclude_metadata.items():
                    chunk_value = chunk_metadata.get(key)
                    if chunk_value == value:
                        should_remove = False
                        # log.debug(f"Chunk {chunk.id} excluded from removal - {key}={chunk_value} matches exclude filter")
                        break
            
            if should_remove:
                chunks_to_remove.append(chunk.id)
                if chunk_source:
                    urls_to_remove_from_db.add(chunk_source)
                    if chunk_source not in removed_urls:
                        removed_urls.append(chunk_source)
                # log.debug(f"Chunk {chunk.id} marked for removal - source: {chunk_source}, metadata: {chunk_metadata}")
        
        # Remove chunks from vector memory
        if chunks_to_remove:
            cat.memory.vectors.declarative.delete_points(chunks_to_remove)
            vector_removed_count = len(chunks_to_remove)
            log.info(json.dumps({
                "component": "ccat_dietician",
                "event": "cleanup_vector_removed",
                "data": {
                    "count": vector_removed_count
                }
            }))
        
        # Remove corresponding documents from dietician database
        with Session(engine) as session:
            for url in urls_to_remove_from_db:
                try:
                    doc_to_remove = session.query(DietDocument).filter_by(name=url).first()
                    if doc_to_remove:
                        # Delete all associated chunks first to avoid foreign key constraint issues
                        for chunk in doc_to_remove.chunks:
                            session.delete(chunk)
                        # Now delete the document
                        session.delete(doc_to_remove)
                        removed_count += 1
                        log.info(json.dumps({
                            "component": "ccat_dietician",
                            "event": "cleanup_db_removed",
                            "data": {
                                "url": url
                            }
                        }))
                except Exception as e:
                    error_msg = f"Error removing document {url} from database: {str(e)}"
                    log.error(json.dumps({
                        "component": "ccat_dietician",
                        "event": "cleanup_db_error",
                        "data": {
                            "url": url,
                            "error": str(e)
                        }
                    }))
                    errors.append(error_msg)
            
            # Commit all database changes
            session.commit()
            
    except Exception as e:
        error_msg = f"Metadata-based cleanup operation failed: {str(e)}"
        log.error(json.dumps({
            "component": "ccat_dietician",
            "event": "cleanup_error",
            "data": {
                "error": str(e)
            }
        }))
        errors.append(error_msg)
    
    result = {
        "removed_count": removed_count,
        "vector_removed_count": vector_removed_count,
        "removed_urls": removed_urls,
        "errors": errors
    }
    
    log.info(json.dumps({
        "component": "ccat_dietician",
        "event": "cleanup_complete",
        "data": result
    }))
    return result


def check_should_update(url: str, cat, provided_hash: str = None) -> bool:
    """
    Check if a URL needs to be updated based on its content hash.
    
    Args:
        url: The URL to check
        cat: The Cat instance
        provided_hash: Optional pre-computed hash. If not provided, it will be computed from content.
        
    Returns:
        bool: True if the URL should be updated (new or changed), False otherwise.
    """
    global engine
    
    if engine is None:
        db_filepath = cat.mad_hatter.get_plugin().load_settings()["sqlite_db_path"]
        engine = create_engine(db_filepath)
        Base.metadata.create_all(engine, checkfirst=True)

    # Check for PDF optimization
    settings = cat.mad_hatter.get_plugin().load_settings()
    if settings.get("optimize_pdf_check", False) and url.lower().endswith('.pdf'):
        with Session(engine) as session:
            try:
                doc_by_name = session.query(DietDocument).filter_by(name=url).first()
                if doc_by_name:
                    # log.debug(f"Dietician check: {url} is PDF and exists (optimization enabled)")
                    return False
            except Exception as e:
                log.error(json.dumps({
                    "component": "ccat_dietician",
                    "event": "check_update_error",
                    "data": {
                        "url": url,
                        "error": str(e)
                    }
                }))
        
    # If hash is not provided, we can't check without fetching content
    # But this function is designed to be called with a hash computed during the check phase
    if not provided_hash:
        log.warning(json.dumps({
            "component": "ccat_dietician",
            "event": "check_update_warning",
            "data": {
                "url": url,
                "message": "check_should_update called without hash. Assuming update needed."
            }
        }))
        return True
        
    with Session(engine) as session:
        try:
            # Check if document exists in DB
            doc_by_name = session.query(DietDocument).filter_by(name=url).first()
            
            if doc_by_name is None:
                # New URL. Check if content exists under another name (duplicate)
                doc_by_hash = session.query(DietDocument).filter_by(hash=provided_hash).first()
                if doc_by_hash:
                     # log.debug(f"Dietician check: {url} is DUPLICATE of {doc_by_hash.name} (hash match)")
                     return False # Skip ingestion
                
                # New document, needs update
                # log.debug(f"Dietician check: {url} is NEW")
                return True
            
            # Check if hash matches
            if doc_by_name.hash == provided_hash:
                # Hash matches, no update needed
                # log.debug(f"Dietician check: {url} is UNCHANGED (hash match)")
                return False
            else:
                # Hash different, update needed
                # log.debug(f"Dietician check: {url} is CHANGED (hash mismatch)")
                return True
                
        except Exception as e:
            log.error(json.dumps({
                "component": "ccat_dietician",
                "event": "check_update_error",
                "data": {
                    "url": url,
                    "error": str(e)
                }
            }))
            return True


def save_plugin_settings_to_file(settings: dict, plugin_path: str) -> dict:
    """
    Save plugin settings to settings.json file in the plugin directory.
    This replicates the default save behavior from the Cat framework.
    
    Args:
        settings: The settings dictionary to save
        plugin_path: The path to the plugin directory
        
    Returns:
        The updated settings dictionary, or empty dict if save failed
    """
    settings_file_path = os.path.join(plugin_path, "settings.json")
    
    # Load already saved settings (replicate load_settings behavior)
    old_settings = {}
    if os.path.exists(settings_file_path):
        try:
            with open(settings_file_path, "r") as json_file:
                old_settings = json.load(json_file)
        except Exception as e:
            log.error(json.dumps({
                "component": "ccat_dietician",
                "event": "settings_load_error",
                "data": {
                    "error": str(e)
                }
            }))
    
    # Merge new settings with old ones
    updated_settings = {**old_settings, **settings}
    
    # Save settings to file
    try:
        with open(settings_file_path, "w") as json_file:
            json.dump(updated_settings, json_file, indent=4)
        return updated_settings
    except Exception as e:
        log.error(json.dumps({
            "component": "ccat_dietician",
            "event": "settings_save_error",
            "data": {
                "error": str(e)
            }
        }))
        return {}


@plugin
def save_settings(settings):
    """Handle plugin settings save with optional database deletion."""
    delete_db = settings.get("delete_db", False)
    
    if delete_db:
        db_path = settings.get("sqlite_db_path", "sqlite:///cat/data/dietician.db")
        
        # Extract file path from SQLAlchemy connection string
        if db_path.startswith("sqlite:///"):
            file_path = db_path[10:]  # Remove "sqlite:///" prefix
        else:
            file_path = db_path
            
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                log.info(json.dumps({
                    "component": "ccat_dietician",
                    "event": "db_delete_success",
                    "data": {
                        "path": file_path
                    }
                }))
            else:
                log.warning(json.dumps({
                    "component": "ccat_dietician",
                    "event": "db_delete_warning",
                    "data": {
                        "path": file_path,
                        "message": "Database file does not exist"
                    }
                }))
        except Exception as e:
            log.error(json.dumps({
                "component": "ccat_dietician",
                "event": "db_delete_error",
                "data": {
                    "path": file_path,
                    "error": str(e)
                }
            }))
        
        # Reset the delete_db flag to False after attempting deletion
        settings["delete_db"] = False
    
    # Save settings using the extracted function (replicates default Cat behavior)
    plugin_path = os.path.dirname(os.path.abspath(__file__))
    return save_plugin_settings_to_file(settings, plugin_path)
