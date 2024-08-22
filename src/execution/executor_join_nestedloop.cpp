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
// Created by ziqi on 2024/8/4.
//

#include "executor_join_nestedloop.h"
#include "expr/condition_expr.h"

namespace wsdb {
NestedLoopJoinExecutor::NestedLoopJoinExecutor(
    JoinType join_type, AbstractExecutorUptr left, AbstractExecutorUptr right, ConditionVec conditions)
    : JoinExecutor(join_type, std::move(left), std::move(right), std::move(conditions))
{}

/// inner join
void NestedLoopJoinExecutor::InitInnerJoin()
{
  WSDB_STUDENT_TODO(L2, f1, NestedLoopJoinExecutor, InitInnerJoin());
  WSDB_STUDENT_TODO(L3, t1, NestedLoopJoinExecutor, InitInnerJoin());
}

void NestedLoopJoinExecutor::NextInnerJoin()
{
  WSDB_STUDENT_TODO(L2, f1, NestedLoopJoinExecutor, NextInnerJoin());
  WSDB_STUDENT_TODO(L3, t1, NestedLoopJoinExecutor, NextInnerJoin());
}

auto NestedLoopJoinExecutor::IsEndInnerJoin() const -> bool
{
  WSDB_STUDENT_TODO(L2, f1, NestedLoopJoinExecutor, IsEndInnerJoin());
  WSDB_STUDENT_TODO(L3, t1, NestedLoopJoinExecutor, IsEndInnerJoin());
}

/// outer join
void NestedLoopJoinExecutor::InitOuterJoin() { WSDB_STUDENT_TODO(L3, t2, NestedLoopJoinExecutor, InitOuterJoin()); }

void NestedLoopJoinExecutor::NextOuterJoin() { WSDB_STUDENT_TODO(L3, t2, NestedLoopJoinExecutor, NextOuterJoin()); }

auto NestedLoopJoinExecutor::IsEndOuterJoin() const -> bool
{
  WSDB_STUDENT_TODO(L3, t2, NestedLoopJoinExecutor, IsEndOuterJoin());
}

}  // namespace wsdb