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

#include "executor_projection.h"

namespace wsdb {

ProjectionExecutor::ProjectionExecutor(AbstractExecutorUptr child, RecordSchemaUptr proj_schema)
    : AbstractExecutor(Basic), child_(std::move(child))
{
  out_schema_ = std::move(proj_schema);
}

void ProjectionExecutor::Init() { WSDB_STUDENT_TODO(L2, t1, ProjectionExecutor, Init()); }

void ProjectionExecutor::Next() { WSDB_STUDENT_TODO(L2, t1, ProjectionExecutor, Next()); }

auto ProjectionExecutor::IsEnd() const -> bool { WSDB_STUDENT_TODO(L2, t1, ProjectionExecutor, IsEnd()); }

}  // namespace wsdb