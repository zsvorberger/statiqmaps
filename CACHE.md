# Cache Inventory

This document lists existing cache layers, their storage type, data source,
and what causes them to refresh or invalidate. It is descriptive only.

1) Raw activity store
   - Cache name: Strava activities JSON
   - Storage: disk JSON
   - Path: `users_data/ID<strava_id>/strava_activities.json`
   - Source of truth: Strava API via `user_data_pullers/fetch_data.py`
   - Refresh/invalidate: overwritten on Strava sync; mtime changes drive
     dependent caches.

2) In-memory activity cache
   - Cache name: `_ACTIVITY_CACHE`
   - Storage: in-memory (LRU OrderedDict)
   - Location: `user_data_pullers/activity_cache.py`
   - Source of truth: `users_data/ID<strava_id>/strava_activities.json`
   - Refresh/invalidate: mtime mismatch on read or `invalidate_activities_cache`.

3) Stats bundle cache
   - Cache name: stats bundle
   - Storage: disk JSON
   - Path: `users_data/stats_cache/<app_user_id>__<strava_id>.json`
   - Source of truth: derived from activity data and helpers in
     `services/stats_builder.py`
   - Refresh/invalidate: built by `build_stats_bundle` during stats build
     jobs (e.g., `/api/refresh_stats`) or post-sync `prime_precomputed_stats`;
     deleted by `_clear_user_caches`.

4) Disk JSON caches in `data/cache`
   - Cache names: `all_time_stats`, `foot_stats`, `lifetime`,
     `yearly_overview`, `year_detail_<year>`, `personal_best_v2_*`,
     `foot_map`, `bike_list`, and related per-user caches.
   - Storage: disk JSON
   - Path pattern: `data/cache/<cache_name>_<app_user_id>_<strava_id>.json`
   - Source of truth: derived from activity data and/or stats bundle contents.
   - Refresh/invalidate: built by `prime_precomputed_stats` and/or
     `_cached_json` on first access; TTLs enforced by `load_cache`;
     deleted by `_clear_user_caches`.

5) Activity frame cache
   - Cache name: activity frame cache
   - Storage: disk pickle + sidecar mtime
   - Path: `users_data/activity_frames/<app_user_id>__<strava_id>.pkl`
     with `*.mtime`
   - Source of truth: `users_data/ID<strava_id>/strava_activities.json`
   - Refresh/invalidate: rebuilt when `strava_activities.json` mtime changes;
     written by `services/activity_frame_cache.py` and `load_activities_df`.

6) Heatmap cache
   - Cache name: heatmap segments
   - Storage: disk JSON
   - Path: `users_data/heatmap_cache/<app_user_id>__<strava_id>.json`
   - Source of truth: activities + gear lookup via
     `services/heatmap_builder.py`
   - Refresh/invalidate: built by `build_heatmap_segments` during
     stats refresh or when explicitly called; removed by `_clear_user_caches`
     only if manually deleted.

7) In-memory graph response cache
   - Cache name: `_GRAPH_RESPONSE_CACHE`
   - Storage: in-memory (TTL + max size)
   - Location: `appp.py`
   - Source of truth: stats bundle graph series
   - Refresh/invalidate: TTL expiry or max-size eviction.
