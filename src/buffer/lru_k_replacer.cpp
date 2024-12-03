//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

#include <limits>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> lock_guard(latch_);

  if (curr_size_ == 0) return std::nullopt;

  frame_id_t evictable_frame;
  double max_kth = 0, kth;
  for (const auto &iter : node_store_) {
    const LRUKNode &node = iter.second;
    if (!node.is_evictable_) continue;

    if (node.history_.size() < node.k_)
      kth = std::numeric_limits<double>::infinity();
    else
      kth = node.history_.back() - node.history_.front();

    if (kth > max_kth) {
      max_kth = kth;
      evictable_frame = node.fid_;
    }
    if (kth == std::numeric_limits<double>::infinity()) {
      evictable_frame =
          node.history_.back() < node_store_[evictable_frame].history_.back() ? node.fid_ : evictable_frame;
    }
  }
  node_store_.erase(evictable_frame);
  curr_size_--;

  return evictable_frame;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "Invalid frame_id");
  std::lock_guard<std::mutex> lock_guard(latch_);

  if (node_store_.count(frame_id) == 0) {
    LRUKNode node{};
    node.fid_ = frame_id;
    node.k_ = k_;
    node.history_.push_back(current_timestamp_++);

    node_store_[frame_id] = node;
  } else {
    node_store_[frame_id].history_.push_back(current_timestamp_++);
  }

  if (node_store_[frame_id].history_.size() > node_store_[frame_id].k_) {
    node_store_[frame_id].history_.pop_front();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "Invalid frame_id");
  std::lock_guard<std::mutex> lock_guard(latch_);

  bool current_evictable = node_store_[frame_id].is_evictable_;
  if (current_evictable && !set_evictable) {
    // evictable -> not evictable
    node_store_[frame_id].is_evictable_ = false;
    curr_size_--;
  } else if (!current_evictable && set_evictable) {
    // not evictable -> evictable
    node_store_[frame_id].is_evictable_ = true;
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "Invalid frame_id");
  std::lock_guard<std::mutex> lock_guard(latch_);

  if (node_store_.count(frame_id) && !node_store_[frame_id].is_evictable_)
    throw std::runtime_error("Tried to remove an unevictable frame");
  if (node_store_.erase(frame_id)) curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
