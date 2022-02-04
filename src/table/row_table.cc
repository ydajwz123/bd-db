#include "row_table.h"
#include <cstring>
#include <vector>
#include <thread>
#include <iostream>

namespace bytedance_db_project {

RowTable::RowTable() {
}

RowTable::~RowTable() {
  if (rows_ != nullptr) {
    delete rows_;
    rows_ = nullptr;
  }
}

void RowTable::Load(BaseDataLoader *loader) {
  num_cols_ = loader->GetNumCols();
  auto rows = loader->GetRows();
  num_rows_ = rows.size();
  rows_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row,
                FIXED_FIELD_LEN * num_cols_);
  }
}

int32_t RowTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  return *(int32_t *) (rows_ + (row_id * num_cols_ + col_id) * FIXED_FIELD_LEN);
}

void RowTable::PutIntField(int32_t row_id, int32_t col_id, int32_t field) {
  // TODO: Implement this!
  *(int32_t *) (rows_ + (row_id * num_cols_ + col_id) * FIXED_FIELD_LEN) = field;
  return;
}

int64_t RowTable::ColumnSum() {
  // TODO: Implement this!
  int64_t res = 0;
  // SumArgs_T sum_args[MAX_THREADS];
  // std::vector<std::thread> threads;
  // uint32_t nrows_per_thread = (num_rows_ + nthreads_ - 1) / nthreads_;

  // for (size_t i = 0; i < nthreads_; ++i) {
  //   sum_args[i].begin = i * nrows_per_thread;
  //   sum_args[i].end = (i + 1) * nrows_per_thread;
  //   sum_args[i].end = sum_args[i].end > num_rows_ ? num_rows_ : sum_args[i].end;
  //   threads.push_back(std::thread(&RowTable::ColumnSumHelper, this, &sum_args[i]));
  // }

  // for (size_t i = 0; i < nthreads_; ++i)
  //   threads[i].join();

  // for (size_t i = 0; i < nthreads_; ++i)
  //   res += sum_args[i].res;
  for (int i = 0; i < num_rows_; ++i)
    res += GetIntField(i, 0);
  return res;
}

int64_t RowTable::PredicatedColumnSum(int32_t threshold1, int32_t threshold2) {
  // TODO: Implement this!
  int64_t res = 0;

  for (int i = 0; i < num_rows_; ++i)
    if (GetIntField(i, 1) > threshold1 && GetIntField(i, 2) < threshold2)
      res += GetIntField(i, 0);
  return res;
}

int64_t RowTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  int64_t res = 0;
  for (int i = 0; i < num_rows_; ++i)
    if (GetIntField(i, 0) > threshold)
      for (int j = 0; j < num_cols_; ++j)
        res += GetIntField(i, j);

  return res;
}

int64_t RowTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!
  int64_t count = 0;
  for (int i = 0; i < num_rows_; ++i)
    if (GetIntField(i, 0) < threshold) {
      PutIntField(i, 3, GetIntField(i, 3) + GetIntField(i, 2));
      count++;
    }
  return count;
}
} // namespace bytedance_db_project
