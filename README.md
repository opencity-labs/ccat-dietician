# Dietician

[![Awesome plugin](https://custom-icon-badges.demolab.com/static/v1?label=&message=Awesome+plugin&color=000000&style=for-the-badge&logo=cheshire_cat_ai)](https://github.com/cheshire-cat-ai/awesome-plugins)

![fat cat](./img/fat_cat_2.jpg)

This plugin hooks into the `RabbitHole` to prevend multiple ingestions of the same document.

Using this plugin you can relax yourself and put into the RabbitHole all the files you want, the Dietician will only allow new documents (o newer versions of the same file, by updating only the modified chunks) for you.

If you like this plugin, please show appreciacion by giving a star to the repository, otherwise a kitten will die!

## Recent Changes

- **Modular Settings**: Settings have been moved to a separate `settings.py` file for better code organization
- **Database Management**: Added a new "Delete Database" option in plugin settings that allows you to completely reset the Dietician's tracking database
- **Enhanced Cleanup**: Added `remove_documents_by_metadata()` function for programmatic removal of documents based on metadata filters
- **Improved Memory Handling**: Updated working memory access pattern for better compatibility
- **Cascade Deletion**: Improved database relationships with cascade deletion for cleaner data management
- **Debug Logging**: Reduced log verbosity by changing database location messages to debug level


## Usage

1. Install the plugin BEFORE ingesting any document: Dietician tracks document ingestion only if activated 
2. Ingest documents
3. Optionally use the plugin settings to:
   - Change the SQLite database path (advanced users only)
   - Reset the tracking database by enabling "Delete Database" option

## Advanced Features

### Database Management
The plugin now includes a "Delete Database" setting that allows you to completely reset the Dietician's tracking database. This is useful when you want to start fresh or if you encounter any database-related issues.

### Programmatic Document Removal
The plugin provides a `remove_documents_by_metadata()` function that allows for programmatic removal of documents based on metadata filters. This is useful for batch cleanup operations or integration with other plugins.

## Notice
If you wipe the declarative memory, you can now use the plugin settings to delete the dietician database by enabling the "Delete Database" option. Alternatively, you can manually delete the `dietician.db` file inside the plugin directory.

## File Structure
- `dietician.py` - Main plugin logic with document tracking and cleanup functions
- `settings.py` - Plugin configuration settings
- `settings.json` - Saved plugin settings (auto-generated)

## Under the hood

![Diagram flow](./img/ccat-dietician.png)

## Log Schema

This plugin uses structured JSON logging to facilitate monitoring and debugging. All logs follow this base structure:

```json
{
  "component": "ccat_dietician",
  "event": "<event_name>",
  "data": {
    ... <event_specific_data>
  }
}
```

### Event Types

| Event Name | Description | Data Fields |
|------------|-------------|-------------|
| `db_init` | Logged when the database is initialized | `db_path` |
| `ingestion_new` | Logged when a new document is allowed for ingestion | `source`, `chunk_count` |
| `ingestion_duplicate` | Logged when a duplicate document is skipped | `source`, `original_source` |
| `ingestion_rechunk` | Logged when a document is re-chunked | `source`, `chunk_count`, `original_source` |
| `ingestion_unchanged` | Logged when a document is unchanged | `source` |
| `ingestion_update` | Logged when a document is updated | `source`, `new_chunks_count`, `deleted_chunks_count` |
| `ingestion_update_skipped` | Logged when a document has changed hash but chunks exist | `source` |
| `ingestion_error` | Logged when an error occurs during ingestion check | `source`, `error` |
| `cleanup_vector_removed` | Logged when chunks are removed from vector memory | `count` |
| `cleanup_db_removed` | Logged when a document is removed from the database | `url` |
| `cleanup_db_error` | Logged when an error occurs removing from DB | `url`, `error` |
| `cleanup_error` | Logged when a general cleanup error occurs | `error` |
| `cleanup_complete` | Logged when cleanup is complete | `removed_count`, `vector_removed_count`, `removed_urls`, `errors` |
| `check_update_warning` | Logged when checking update status without hash | `url` |
| `check_update_error` | Logged when checking update status fails | `url`, `error` |
| `settings_load_error` | Logged when loading settings fails | `error` |
| `settings_save_error` | Logged when saving settings fails | `error` |
| `db_delete_success` | Logged when database is successfully deleted | `path` |
| `db_delete_warning` | Logged when database file to delete is not found | `path` |
| `db_delete_error` | Logged when deleting database fails | `path`, `error` |
