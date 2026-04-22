#pragma once

#include "duckdb.hpp"
#include "delta_sharing_client.hpp"
#include <string>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <thread>
#include <atomic>
#include <memory>

namespace duckdb {

// Represents a cached file URL with expiration tracking
struct CachedFileUrl {
    std::string url;
    std::string file_id;
    std::chrono::system_clock::time_point expiration;
    int64_t size;
    
    bool IsExpired(std::chrono::milliseconds threshold) const {
        auto now = std::chrono::system_clock::now();
        return (expiration - now) < threshold;
    }
    
    bool IsExpiredNow() const {
        auto now = std::chrono::system_clock::now();
        return now >= expiration;
    }
};

// Manages URL caching and refresh for a Delta Sharing table
class UrlCacheManager {
public:
    UrlCacheManager(
        std::shared_ptr<DeltaSharingClient> client,
        std::string share_name,
        std::string schema_name,
        std::string table_name,
        std::string refresh_token,
        std::chrono::milliseconds refresh_threshold = std::chrono::minutes(10),
        std::chrono::milliseconds check_interval = std::chrono::minutes(1)
    );
    
    ~UrlCacheManager();
    
    // Get URL for a file ID, refreshing if necessary
    std::string GetUrl(const std::string &file_id);
    
    // Register a file with its initial URL and expiration
    void RegisterFile(const std::string &file_id, const std::string &url, 
                     const std::string &expiration_timestamp, int64_t size);
    
    // Start background refresh thread
    void StartRefreshThread();
    
    // Stop background refresh thread
    void StopRefreshThread();
    
    // Manually trigger refresh
    void RefreshUrls();
    
    // Check if cache has a refresh token available
    bool HasRefreshToken() const {
        return !refresh_token_.empty();
    }
    
    // Get cache size (for debugging/monitoring)
    size_t GetCacheSize() const {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        return url_cache_.size();
    }

private:
    std::shared_ptr<DeltaSharingClient> client_;
    std::string share_name_;
    std::string schema_name_;
    std::string table_name_;
    std::string refresh_token_;
    
    std::unordered_map<std::string, CachedFileUrl> url_cache_;
    mutable std::mutex cache_mutex_;
    
    std::chrono::milliseconds refresh_threshold_;
    std::chrono::milliseconds check_interval_;
    
    std::atomic<bool> refresh_thread_running_{false};
    std::unique_ptr<std::thread> refresh_thread_;
    std::atomic<bool> refresh_in_progress_{false};
    
    void RefreshThreadLoop();
    bool ShouldRefresh() const;
    std::chrono::system_clock::time_point ParseExpirationTimestamp(const std::string &timestamp);
    
    // Get URL without automatic refresh (internal use)
    std::string GetUrlNoRefresh(const std::string &file_id);
};

} // namespace duckdb
