"""folder_watch — a minimal folder-watch service backing the File Initiator.

Watch configured folders; on a new/changed file matching the patterns, emit a
bus event to the farm stream so the deployed Project (GraphRecord) runs.
"""
