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

#include "lru_replacer.h"
#include "common/config.h"
#include "../common/error.h"
namespace wsdb {

LRUReplacer::LRUReplacer() : cur_size_(0), max_size_(BUFFER_POOL_SIZE) {}


auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
//    LRU是最近最少用，需要记录一个"最后访问时间"，然后淘汰掉这个时间最靠前的。
//    if (cur_size_< max_size_) {
//        如果缓存池还没有满，那就直接返回false
//        return false;
//    }
//    如果满了，那么就清除第一个即可。
    for(auto it = lru_list_.begin(); it!=lru_list_.end(); it++){
        auto a=*it;
        if(a.second == false){
//            如果它不能被淘汰，那么就看下一个
            continue;
        }else{
            *frame_id = a.first;
//            然后把它淘汰
            lru_list_.erase(it);
            cur_size_--;
            lru_hash_.erase(*frame_id);
            return true;
        }
    }
//    说明所有的frame都是pinned，无法被淘汰
    return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(latch_);  // 获取锁

//    这里的it是一个键值对，it->first是frame_id，it->second是bool值
    auto it = lru_hash_.find(frame_id);
    if (it != lru_hash_.end()) {
        // 从链表中移除该帧
        lru_list_.erase(it->second);
        cur_size_--;
//        哈希表里不用移除，因为会直接被覆盖。
    }
//   这里pin可能要调用victim，具体未知，需要测试后再知道。
//把这个新的帧插入到末尾
    lru_list_.push_back(std::make_pair(frame_id, false));
    lru_hash_[frame_id] = lru_list_.insert(lru_list_.end(), std::make_pair(frame_id, false));
    cur_size_++;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(latch_);  // 获取锁

//    其实就是把这个frame_id的bool值改为true，表示可以被移除了。
    auto it = lru_hash_.find(frame_id);
    if (it != lru_hash_.end()) {
        it->second->second = true;
    }else{
        return;
    }
}

auto LRUReplacer::Size() -> size_t {
    int size = 0;
    for(auto it = lru_list_.begin(); it!=lru_list_.end(); it++){
        if(it->second == true){
            size++;
        }
    }
    return size;
}

}  // namespace wsdb
