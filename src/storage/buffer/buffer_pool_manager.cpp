/*------------------------------------------------------------------------------
 - Copyright (c) 2024. Websoft research group, Nanjing University.
 -
 - This program is free software: you can redistribute it and/or modify
 - it under the terms of the GNU General Public License as published by
 - the Free Software Foundation, either version 3 of the License, or
 - (at your option) any later version.
 -
 - This program is distributed in the hope that it will be useful,
 - but WITHOUT ANY WARRANTY; without even the implied warranty of
 - MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 - GNU General Public License for more details.
 -
 - You should have received a copy of the GNU General Public License
 - along with this program.  If not, see <https://www.gnu.org/licenses/>.
 -----------------------------------------------------------------------------*/

//
// Created by ziqi on 2024/7/17.
//
#include "buffer_pool_manager.h"
#include "replacer/lru_replacer.h"
#include "replacer/lru_k_replacer.h"

#include "../../../common/error.h"

namespace wsdb {

BufferPoolManager::BufferPoolManager(DiskManager *disk_manager, wsdb::LogManager *log_manager, size_t replacer_lru_k)
    : disk_manager_(disk_manager), log_manager_(log_manager)
{
  if (REPLACER == "LRUReplacer") {
    replacer_ = std::make_unique<LRUReplacer>();
  } else if (REPLACER == "LRUKReplacer") {
    replacer_ = std::make_unique<LRUKReplacer>(replacer_lru_k);
  } else {
    WSDB_FETAL("Unknown replacer: " + REPLACER);
  }
  // init free_list_
  for (frame_id_t i = 0; i < static_cast<int>(BUFFER_POOL_SIZE); i++) {
    free_list_.push_back(i);
  }
}

auto BufferPoolManager::FetchPage(file_id_t fid, page_id_t pid) -> Page * {
//    WSDB_STUDENT_TODO(l1, t2);
    std::lock_guard<std::mutex> lock(latch_);  // 获取锁
//    检查该页是否在buffer pool中
    auto frame = GetFrame(fid, pid);
    if (frame != nullptr) {
//        如果有了,pin这一页
    frame->Pin();
    fid_pid_t key = {fid, pid};
    frame_id_t frameId = page_frame_lookup_[key];
    replacer_->Pin(frameId);
    return frame->GetPage();
    } else{
        frame_id_t frame_id = GetAvailableFrame();
        UpdateFrame(frame_id, fid, pid);


        return frame->GetPage();

    }
}

auto BufferPoolManager::UnpinPage(file_id_t fid, page_id_t pid, bool is_dirty) -> bool {
//    WSDB_STUDENT_TODO(l1, t2);
    std::lock_guard<std::mutex> lock(latch_);
    auto frame = GetFrame(fid, pid);
    if (frame == nullptr) return false;
    frame->Unpin();
    if(is_dirty){
        fid_pid_t key = {fid, pid};
        frame_id_t frameId = page_frame_lookup_[key];
        frame->SetDirty(is_dirty);
        replacer_->Unpin(frameId);
    }
    return true;
}


auto BufferPoolManager::DeletePage(file_id_t fid, page_id_t pid) -> bool {
//    WSDB_STUDENT_TODO(l1, t2);
    std::lock_guard<std::mutex> lock(latch_);
    auto frame = GetFrame(fid, pid);
//    如果这一页不在buffer pool中,直接返回true
    if (frame == nullptr) return true;
//    如果这一页正在被使用，返回false
    if (frame->GetPinCount() > 0) return false;
    auto frame_id = page_frame_lookup_[{fid, pid}];
    FlushPage(fid, pid);
    frame->Reset();
    free_list_.push_back(frame_id);
    replacer_->Unpin(frame_id);
    page_frame_lookup_.erase({fid, pid});
    return true;
}

auto BufferPoolManager::DeleteAllPages(file_id_t fid) -> bool {
//    WSDB_STUDENT_TODO(l1, t2);
    bool returnValue = true;
    for(auto iter = page_frame_lookup_.begin(); iter != page_frame_lookup_.end();){
        if(iter->first.fid == fid){
//            只要有一个页面没有被删除，就返回false
            returnValue &= DeletePage(iter->first.fid, iter->first.pid);
            iter = page_frame_lookup_.begin();
        }
        else iter++;
    }
    return returnValue;
}

auto BufferPoolManager::FlushPage(file_id_t fid, page_id_t pid) -> bool {
//    WSDB_STUDENT_TODO(l1, t2);
    std::lock_guard<std::mutex> lock(latch_);
    auto frame = GetFrame(fid, pid);
    if (frame == nullptr) return false;
    if(frame->IsDirty()){
        disk_manager_->WritePage(fid, pid, frames_[page_frame_lookup_[{fid, pid}]].GetPage()->GetData());
    }
    return true;
}

auto BufferPoolManager::FlushAllPages(file_id_t fid) -> bool {
    bool returnValue = true;
    for(auto iter = page_frame_lookup_.begin(); iter != page_frame_lookup_.end();){
        if(iter->first.fid == fid){
            returnValue&=FlushPage(iter->first.fid, iter->first.pid);
            iter = page_frame_lookup_.begin();
        }
        else iter++;
    }
    return returnValue;
}

auto BufferPoolManager::GetAvailableFrame() -> frame_id_t {
//    WSDB_STUDENT_TODO(l1, t2);
    frame_id_t frame_id;
    if(!free_list_.empty()){
         frame_id = free_list_.front();
        free_list_.pop_front();
        return frame_id;
    }else{
       return replacer_->Victim(&frame_id);
    }
}

void BufferPoolManager::UpdateFrame(frame_id_t frame_id, file_id_t fid, page_id_t pid) {
//    WSDB_STUDENT_TODO(l1, t2);
    auto frame = &frames_[frame_id];
    if(frame->IsDirty()){
        FlushPage(fid, pid);
        frame->Reset();
        disk_manager_->ReadPage(fid, pid, frame->GetPage()->GetData());
        frame->SetDirty(false);
        frame->Pin();
        replacer_->Pin(frame_id);
        page_frame_lookup_.insert({{fid, pid}, frame_id});
    }
}

auto BufferPoolManager::GetFrame(file_id_t fid, page_id_t pid) -> Frame *
{
  const auto it = page_frame_lookup_.find({fid, pid});
  return it == page_frame_lookup_.end() ? nullptr : &frames_[it->second];
}

}  // namespace wsdb
