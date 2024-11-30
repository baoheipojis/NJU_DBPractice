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
assert(frame->GetPage()->GetPageId()==pid);
        assert(frame->GetPage()->GetFileId()==fid);
    frame->Pin();
    fid_pid_t key = {fid, pid};
    frame_id_t frameId = page_frame_lookup_[key];
    replacer_->Pin(frameId);
    return frame->GetPage();
    } else{
//        缺页，需要读数据
        frame_id_t frame_id = GetAvailableFrame();
        UpdateFrame(frame_id, fid, pid);

        frame = &frames_[frame_id];
        assert(frame->GetPage()->GetPageId()==pid);
        assert(frame->GetPage()->GetFileId()==fid);
        return frame->GetPage();

    }
}

auto BufferPoolManager::UnpinPage(file_id_t fid, page_id_t pid, bool is_dirty) -> bool {
//    WSDB_STUDENT_TODO(l1, t2);
    std::lock_guard<std::mutex> lock(latch_);
    auto frame = GetFrame(fid, pid);
    if (frame == nullptr||!frame->InUse()) return false;
    frame->Unpin();
    if(is_dirty){
        disk_manager_->WritePage(fid, pid, frames_[page_frame_lookup_[{fid, pid}]].GetPage()->GetData());
    }
    fid_pid_t key = {fid, pid};
    frame_id_t frameId = page_frame_lookup_[key];
    frame->SetDirty(is_dirty);
    replacer_->Unpin(frameId);
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
    if(frame->IsDirty()) {
        disk_manager_->WritePage(fid, pid, frames_[page_frame_lookup_[{fid, pid}]].GetPage()->GetData());
    }

    frame->Reset();
    free_list_.push_back(frame_id);
    replacer_->Unpin(frame_id);
    page_frame_lookup_.erase({fid, pid});
    return true;
}

auto BufferPoolManager::DeleteAllPages(file_id_t fid) -> bool {
    std::lock_guard<std::mutex> lock(latch_);

//    WSDB_STUDENT_TODO(l1, t2);
    bool returnValue = true;
    // 使用 while 循环确保迭代器有效
    for (auto it = page_frame_lookup_.begin(); it != page_frame_lookup_.end(); ) {
        if (it->first.fid == fid) {
            pid_t pid = it->first.pid;
            auto frame = GetFrame(fid, pid);
//    如果这一页不在buffer pool中,直接返回true
            if (frame == nullptr) return true;
//    如果这一页正在被使用，返回false
            if (frame->GetPinCount() > 0) return false;
            auto frame_id = page_frame_lookup_[{fid, pid}];

            if(frame->IsDirty()){
                disk_manager_->WritePage(fid, pid, frames_[page_frame_lookup_[{fid, pid}]].GetPage()->GetData());
            }

            frame->Reset();
            free_list_.push_back(frame_id);
            replacer_->Unpin(frame_id);
            // 删除元素，并自动更新迭代器
            it = page_frame_lookup_.erase(it);
        } else {
            ++it;
        }
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
    for(auto iter = page_frame_lookup_.begin(); iter != page_frame_lookup_.end();iter++){
        if(iter->first.fid == fid){
            returnValue&=FlushPage(iter->first.fid, iter->first.pid);
        }
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
       bool has_victim =replacer_->Victim(&frame_id);
       if(!has_victim){
           WSDB_THROW(WSDBExceptionType::WSDB_NO_FREE_FRAME, "No free frame");
       }else{
           return frame_id;
       }
    }
}

void BufferPoolManager::UpdateFrame(frame_id_t frame_id, file_id_t fid, page_id_t pid) {
//    WSDB_STUDENT_TODO(l1, t2);
    auto frame = &frames_[frame_id];
    file_id_t old_fid=frame->GetPage()->GetFileId();
    page_id_t old_pid=frame->GetPage()->GetPageId();
    if(frame->IsDirty()){
//        如果旧页是脏的，那么写回
            disk_manager_->WritePage(old_fid, old_pid, frames_[page_frame_lookup_[{old_fid, old_pid}]].GetPage()->GetData());
    }
//    注意，这里还需要删除所有page_frame_lookup中的值。
    page_frame_lookup_.erase({old_fid, old_pid});

    frame->Reset();

    disk_manager_->ReadPage(fid, pid, frame->GetPage()->GetData());
    frame->GetPage()->SetFilePageId(fid, pid);
    frame->SetDirty(false);
    frame->Pin();
    replacer_->Pin(frame_id);
    page_frame_lookup_.insert({{fid, pid}, frame_id});

}

auto BufferPoolManager::GetFrame(file_id_t fid, page_id_t pid) -> Frame *
{
  const auto it = page_frame_lookup_.find({fid, pid});
  return it == page_frame_lookup_.end() ? nullptr : &frames_[it->second];
}

}  // namespace wsdb
