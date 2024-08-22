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
// Created by ziqi on 2024/7/18.
//

#include "executor_filter.h"

namespace wsdb {

FilterExecutor::FilterExecutor(AbstractExecutorUptr child, std::function<bool(const Record &)> filter)
    : AbstractExecutor(Basic), child_(std::move(child)), filter_(std::move(filter))
{}
void FilterExecutor::Init() { WSDB_STUDENT_TODO(L2, t1, FilterExecutor, Init()); }

void FilterExecutor::Next() { WSDB_STUDENT_TODO(L2, t1, FilterExecutor, Next()); }

auto FilterExecutor::IsEnd() const -> bool { WSDB_STUDENT_TODO(L2, t1, FilterExecutor, IsEnd()); }

auto FilterExecutor::GetOutSchema() const -> const RecordSchema * { return child_->GetOutSchema(); }
}  // namespace wsdb