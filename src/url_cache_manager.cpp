#include "url_cache_manager.hpp"

namespace duckdb {

UrlCacheManager::UrlCacheManager(
    std::shared_ptr<DeltaSharingClient> client,
    std::string share_name,
    std::string schema_name,
    std::string table_name,
    std::string refresh_token,
    std::chrono::milliseconds refresh_threshold,
    std::chrono::milliseconds check_interval)
    : client_(std::move(client)),
      share_name_(std::move(share_name)),
      schema_name_(std::move(schema_name)),
      table_name_(std::move(table_name)),
      refresh_token_(std::move(refresh_token)),
      refresh_threshold_(refresh_threshold),
      check_interval_(check_interval) {
}

UrlCacheManager::~UrlCacheManager() {
    StopRefreshThread();
}

void UrlCacheManager::RegisterFile(const std::string &file_id, const std::string &url,
                                   int64_t expiration_unix_millis, int64_t size) {
    std::lock_guard<std::mutex> lock(cache_mutex_);

    CachedFileUrl cached_url;
    cached_url.url = url;
    cached_url.file_id = file_id;
    cached_url.size = size;
    cached_url.expiration = ExpirationFromUnixMillis(expiration_unix_millis);

    url_cache_[file_id] = cached_url;
}

std::string UrlCacheManager::GetUrlNoRefresh(const std::string &file_id) {
    // Caller must hold cache_mutex_
    auto it = url_cache_.find(file_id);
    if (it == url_cache_.end()) {
        throw InvalidInputException("File ID not found in URL cache: " + file_id);
    }
    return it->second.url;
}

std::string UrlCacheManager::GetUrl(const std::string &file_id) {
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        
        auto it = url_cache_.find(file_id);
        if (it == url_cache_.end()) {
            throw InvalidInputException("File ID not found in URL cache: " + file_id);
        }
        
        // Check if URL is still valid (not expired or about to expire)
        if (!it->second.IsExpired(refresh_threshold_)) {
            return it->second.url;
        }
    }
    
    // URL is expired or about to expire - trigger refresh
    // Don't hold lock during refresh to avoid blocking other threads
    RefreshUrls();
    
    // After refresh, get the new URL
    std::lock_guard<std::mutex> lock(cache_mutex_);
    return GetUrlNoRefresh(file_id);
}

void UrlCacheManager::RefreshUrls() {
    // Coordinate concurrent refreshes via a condition variable so other
    // threads sleep cheaply while one thread does the work, and are
    // notified the moment it completes (or fails).
    {
        std::unique_lock<std::mutex> guard(refresh_mutex_);
        if (refresh_in_progress_) {
            // Another thread is already refreshing; wait for it to finish.
            refresh_cv_.wait(guard, [this] { return !refresh_in_progress_; });
            return;
        }
        refresh_in_progress_ = true;
    }

    // RAII guard so we always clear the flag and wake waiters, even on throw.
    struct ScopeGuard {
        UrlCacheManager *self;
        ~ScopeGuard() {
            {
                std::lock_guard<std::mutex> g(self->refresh_mutex_);
                self->refresh_in_progress_ = false;
            }
            self->refresh_cv_.notify_all();
        }
    } scope_guard{this};

    if (refresh_token_.empty()) {
        throw InvalidInputException("No refresh token available for URL refresh");
    }

    // TODO(url-refresh): Pass refresh_token_ in the QueryTable request body
    // so the server can return URLs for the same set of files even if the
    // table has been updated since the initial query. Today this is a plain
    // re-query, which is functionally adequate but loses the version-pinning
    // guarantee the protocol's refreshToken is meant to provide. See
    // https://github.com/delta-io/delta-sharing/issues/383 for the analogous
    // Spark behaviour.
    DeltaSharingClient::QueryTableResult result;
    try {
        result = client_->QueryTable(share_name_, schema_name_, table_name_);
    } catch (const std::exception &e) {
        throw IOException("Failed to refresh URLs: " + std::string(e.what()));
    }

    std::lock_guard<std::mutex> lock(cache_mutex_);

    // Match refreshed entries to existing cache entries by file id. Files in
    // the refresh response that aren't in the cache are ignored (we never
    // expand the working set mid-query); files in the cache that aren't in
    // the refresh response keep their existing URL (best-effort).
    for (const auto &file : result.files) {
        auto it = url_cache_.find(file.id);
        if (it != url_cache_.end()) {
            it->second.url = file.url;
            it->second.expiration = ExpirationFromUnixMillis(file.expiration_timestamp);
        }
    }
}

bool UrlCacheManager::ShouldRefresh() const {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    // Check if any URLs are about to expire
    for (const auto &entry : url_cache_) {
        if (entry.second.IsExpired(refresh_threshold_)) {
            return true;
        }
    }
    return false;
}

void UrlCacheManager::StartRefreshThread() {
    if (refresh_thread_running_.load()) {
        return;  // Already running
    }
    
    if (refresh_token_.empty()) {
        // No refresh token, can't start refresh thread
        return;
    }
    
    refresh_thread_running_.store(true);
    refresh_thread_ = duckdb::make_uniq<std::thread>([this]() {
        RefreshThreadLoop();
    });
}

void UrlCacheManager::StopRefreshThread() {
    if (refresh_thread_running_.load()) {
        refresh_thread_running_.store(false);
        if (refresh_thread_ && refresh_thread_->joinable()) {
            refresh_thread_->join();
        }
    }
}

void UrlCacheManager::RefreshThreadLoop() {
    while (refresh_thread_running_.load()) {
        std::this_thread::sleep_for(check_interval_);
        
        if (!refresh_thread_running_.load()) {
            break;
        }
        
        if (ShouldRefresh()) {
            try {
                RefreshUrls();
            } catch (const std::exception &e) {
                // Log error but continue - we'll try again on next interval
                // In production, use proper logging instead of stderr
                // For now, just silently continue to avoid cluttering output
            }
        }
    }
}

std::chrono::system_clock::time_point
UrlCacheManager::ExpirationFromUnixMillis(int64_t expiration_unix_millis) {
    // Per the Delta Sharing protocol, expirationTimestamp is a unix timestamp
    // in milliseconds. The field is optional; the parser uses 0 as the
    // sentinel for "not provided", in which case we fall back to a
    // conservative default (1 hour from now), since the documented typical
    // pre-signed URL TTL is around 1 hour.
    if (expiration_unix_millis <= 0) {
        return std::chrono::system_clock::now() + std::chrono::hours(1);
    }
    return std::chrono::system_clock::time_point(
        std::chrono::milliseconds(expiration_unix_millis));
}

} // namespace duckdb
