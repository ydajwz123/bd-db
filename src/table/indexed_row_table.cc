#include "indexed_row_table.h"
#include <cstring>

namespace bytedance_db_project {
IndexedRowTable::IndexedRowTable(int32_t index_column) {
  index_column_ = index_column;
}

IndexedRowTable::~IndexedRowTable() {
  if (rows_ != nullptr) {
    delete rows_;
    rows_ = nullptr;
  }
}

void IndexedRowTable::Load(BaseDataLoader *loader) {
  // TODO: Implement this!
  num_cols_ = loader->GetNumCols();
  auto rows = loader->GetRows();
  num_rows_ = rows.size();
  rows_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row,
                FIXED_FIELD_LEN * num_cols_);
    // maintain the index when loading each row
    // the vector in index_ is monotonic
    int32_t col_val = *(int32_t*) (cur_row + FIXED_FIELD_LEN * index_column_);
    std::vector<int> &tmp_vec = index_[col_val];
    tmp_vec.push_back(row_id);
  }
}

int32_t IndexedRowTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  return *(int32_t *) (rows_ + (row_id * num_cols_ + col_id) * FIXED_FIELD_LEN);
}

void IndexedRowTable::PutIntField(int32_t row_id, int32_t col_id,
                                  int32_t field) {
  // TODO: Implement this!
  int32_t ori_v;
  ori_v = *(int32_t *) (rows_ + (row_id * num_cols_ + col_id) * FIXED_FIELD_LEN); 
  *(int32_t *) (rows_ + (row_id * num_cols_ + col_id) * FIXED_FIELD_LEN) = field;
  if (col_id == index_column_ && ori_v != field) {
    std::vector<int> &tmp_vec = index_[ori_v];
    // delete current row_id from original place
    for (size_t i = 0; i < tmp_vec.size(); ++i)
      if (tmp_vec[i] == row_id) {
        for (size_t j = i; j < tmp_vec.size() - 1; ++j) {
          tmp_vec[j] = tmp_vec[j + 1];
        }
        break;
      }
    // add to new place
    index_[field].push_back(row_id);
  }
}

int64_t IndexedRowTable::ColumnSum() {
  // TODO: Implement this!
  int64_t res = 0;
  for (int i = 0; i < num_rows_; ++i)
    res += GetIntField(i, 0);
  return res;
}

int64_t IndexedRowTable::PredicatedColumnSum(int32_t threshold1,
                                             int32_t threshold2) {
  // TODO: Implement this!
  int64_t res = 0;
  if (index_column_ == 1) {
    // col1 > threshold1 
    for (int32_t col1_v = threshold1 + 1; col1_v < 1024; ++col1_v) {
      for (int row_id : index_[col1_v]) {
        if (GetIntField(row_id, 2) < threshold2)
          res += GetIntField(row_id, 0);
      }
    }
  }
  else if (index_column_ == 2) {
    // col2 < threshold2
    for (int32_t col2_v = 0; col2_v < threshold2; ++col2_v)
      for (int row_id : index_[col2_v])
        if (GetIntField(row_id, 1) > threshold1) {
          res += GetIntField(row_id, 0);
        }
  }
  else {
    // no index
    for (int i = 0; i < num_rows_; ++i)
      if (GetIntField(i, 1) > threshold1 && GetIntField(i, 2) < threshold2)
        res += GetIntField(i, 0);
  }
  return res;
}

int64_t IndexedRowTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  int64_t res = 0;
  if (index_column_ == 0) {
    // col0 > threshold
    for (int32_t col0_v = threshold + 1; col0_v < 1024; ++col0_v) {
      for (int row_id : index_[col0_v]) {
        for (int j = 0; j < num_cols_; ++j)
          res += GetIntField(row_id, j);
      }
    }
  }
  else {
    for (int i = 0; i < num_rows_; ++i)
      if (GetIntField(i, 0) > threshold)
        for (int j = 0; j < num_cols_; ++j)
          res += GetIntField(i, j);
  }
  return res;
}

int64_t IndexedRowTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!
  int64_t cnt = 0;
  if (index_column_ == 0) {
    // col0 < threshold
    for (int32_t col0_v = 0; col0_v < threshold; ++col0_v)
      for (int row_id : index_[col0_v]) {
          PutIntField(row_id, 3, GetIntField(row_id, 3) + GetIntField(row_id, 2));
          cnt++;
        }
  }
  else {
    for (int i = 0; i < num_rows_; ++i)
      if (GetIntField(i, 0) < threshold) {
        PutIntField(i, 3, GetIntField(i, 3) + GetIntField(i, 2));
        cnt++;
      }
  }
  return cnt;
}
} // namespace bytedance_db_project
