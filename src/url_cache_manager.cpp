#include "url_cache_manager.hpp"
#include <iomanip>
#include <sstream>

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
                                   const std::string &expiration_timestamp, int64_t size) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    CachedFileUrl cached_url;
    cached_url.url = url;
    cached_url.file_id = file_id;
    cached_url.size = size;
    
    if (!expiration_timestamp.empty()) {
        try {
            cached_url.expiration = ParseExpirationTimestamp(expiration_timestamp);
        } catch (...) {
            // If parsing fails, default to 1 hour from now
            cached_url.expiration = std::chrono::system_clock::now() + std::chrono::hours(1);
        }
    } else {
        // Default to 1 hour if not provided
        cached_url.expiration = std::chrono::system_clock::now() + std::chrono::hours(1);
    }
    
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
    // Prevent concurrent refreshes
    bool expected = false;
    if (!refresh_in_progress_.compare_exchange_strong(expected, true)) {
        // Another thread is already refreshing, wait for it to complete
        while (refresh_in_progress_.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return;
    }
    
    try {
        if (refresh_token_.empty()) {
            refresh_in_progress_.store(false);
            throw InvalidInputException("No refresh token available for URL refresh");
        }
        
        // Make refresh request to server
        // Note: The server should support using refreshToken in the request
        // For now, we'll re-query the table which should return refreshed URLs
        auto result = client_->QueryTable(share_name_, schema_name_, table_name_);
        
        std::lock_guard<std::mutex> lock(cache_mutex_);
        
        // Update URLs in cache
        size_t updated_count = 0;
        for (const auto &file : result.files) {
            auto it = url_cache_.find(file.id);
            if (it != url_cache_.end()) {
                it->second.url = file.url;
                if (!file.expiration_timestamp.empty()) {
                    try {
                        it->second.expiration = ParseExpirationTimestamp(file.expiration_timestamp);
                    } catch (...) {
                        it->second.expiration = std::chrono::system_clock::now() + std::chrono::hours(1);
                    }
                } else {
                    it->second.expiration = std::chrono::system_clock::now() + std::chrono::hours(1);
                }
                updated_count++;
            }
        }
        
        // Note: In a production implementation, the server would return a new refresh token
        // For now, we keep the same token
        
        refresh_in_progress_.store(false);
        
    } catch (const std::exception &e) {
        refresh_in_progress_.store(false);
        throw IOException("Failed to refresh URLs: " + std::string(e.what()));
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
UrlCacheManager::ParseExpirationTimestamp(const std::string &timestamp) {
    // Parse ISO 8601 timestamp: "2024-04-22T12:00:00Z" or "2024-04-22T12:00:00.000Z"
    // or Unix timestamp in milliseconds as string
    
    // Try parsing as Unix timestamp (milliseconds) first
    try {
        int64_t millis = std::stoll(timestamp);
        auto duration = std::chrono::milliseconds(millis);
        return std::chrono::system_clock::time_point(duration);
    } catch (...) {
        // Not a numeric timestamp, try ISO 8601
    }
    
    // Parse ISO 8601 format
    std::tm tm = {};
    std::istringstream ss(timestamp);
    
    // Try with milliseconds first: "2024-04-22T12:00:00.123Z"
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    
    if (ss.fail()) {
        throw InvalidInputException("Failed to parse expiration timestamp: " + timestamp);
    }
    
    // Convert to time_point (assuming UTC)
    auto time = std::mktime(&tm);
    return std::chrono::system_clock::from_time_t(time);
}

} // namespace duckdb
