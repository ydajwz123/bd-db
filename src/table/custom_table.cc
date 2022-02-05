#include "custom_table.h"
#include <algorithm>
#include <random>
#include <cstring>
#include <assert.h>
#include <iostream>
#include <cstdio>

namespace bytedance_db_project {
CustomTable::CustomTable() {
  nbytespr_part_[0] = 4; // 4 Bytes, i.e. 32 bits for three numbers
}

CustomTable::~CustomTable() {
  for (size_t i = 0; i < TABLE_NPARTS; ++i)
    if (storage_part_[i] != nullptr) {
      delete storage_part_[i];
      storage_part_[i] = nullptr;
    }
  if (storage_sum_row_ != nullptr) {
    delete storage_sum_row_;
    storage_sum_row_ = nullptr;
  }
}


void CustomTable::PushIndex1D(std::map<int16_t, std::vector<int32_t> > &index_, 
    int16_t col_v, int32_t row_id) {
  index_[col_v].push_back(row_id);
}

void CustomTable::PopIndex1D(std::map<int16_t, std::vector<int32_t> > &index_, 
    int16_t col_v, int32_t row_id) {
  auto it_idx = index_.find(col_v);
  assert(it_idx != index_.end());
  std::vector<int32_t> &v = it_idx->second;
  for (auto it = v.begin(); it != v.end(); ++it) {
    if (*it == row_id) {
      it = v.erase(it);
      break;
    }
  }
  if (v.size() == 0)
    index_.erase(it_idx);
}



void CustomTable::Load(BaseDataLoader *loader) {
  // TODO: Implement this!
  num_cols_ = loader->GetNumCols();
  auto rows = loader->GetRows();
  num_rows_ = rows.size();
  // since some queries requires col3
  assert(num_cols_ > 3);
  nbytespr_part_[1] = ((num_cols_ - PART_ONE_NCOLS) * FIXED_BITS_FIELD_1 + 7) >> 3;

  for (size_t i = 0; i < TABLE_NPARTS; ++i)
    storage_part_[i] = new char[(nbytespr_part_[i] * num_rows_ + 7) & ~0x7];
  storage_sum_row_ = new char[((FIXED_BITS_SUM_FIELD >> 3) * num_rows_ + 7) & ~0x7];

  for (size_t row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    int64_t sum = 0;
    int32_t val = 0;
    int32_t val_part1 = 0;
    int32_t *ptr_0 = (int32_t*) (storage_part_[0] + nbytespr_part_[0] * row_id);
    int16_t *ptr_1 = (int16_t*) (storage_part_[1] + nbytespr_part_[1] * row_id);
    for (size_t col_id = 0; col_id < num_cols_; ++col_id) {
      val = *(int32_t*) (cur_row + FIXED_FIELD_LEN * col_id);
      assert(val < 1024 && val >= 0);
      sum += val;
      switch (col_id) {
        case 0:
          sum_col0_ += val;
          PushIndex1D(index_0_, val, row_id);
        case 2:
        case 3:
          val_part1 = (val_part1 << 10) | val;
          break;
        // start of table II
        case 1:
          PushIndex1D(index_1_, val, row_id);
        default:
          *ptr_1++ = (int16_t) val;
      }
      *ptr_0 = val_part1;
    }
    int64_t tmp_val = *(int64_t*) (storage_sum_row_ + (FIXED_BITS_SUM_FIELD >> 3) * row_id);
    tmp_val &= ~((1 << FIXED_BITS_SUM_FIELD) - 1);
    tmp_val |= sum;
    *(int64_t*) (storage_sum_row_ + (FIXED_BITS_SUM_FIELD >> 3) * row_id) = tmp_val;
  }
  is_col0_sumed_ = 1;
}

inline int64_t CustomTable::GetRowSum(int32_t row_id) {

  int64_t tmp_val;
  tmp_val = *(int64_t*) (storage_sum_row_ + (FIXED_BITS_SUM_FIELD >> 3) * row_id);
  tmp_val &= (1 << FIXED_BITS_SUM_FIELD) - 1;
  return tmp_val;
}

inline void CustomTable::UpdateRowSum(int32_t row_id, int64_t val_diff) {
  
  *(uint64_t*) (storage_sum_row_ + (FIXED_BITS_SUM_FIELD >> 3) * row_id) += val_diff;
}

int32_t CustomTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  // Note that: ONE val need TWO Bytes data
  int byte_id, bit_offset = 2 * FIXED_BITS_FIELD_0;
  int32_t res = 0;

  // tb0: col0, 2, 3
  // 32bits: 2bits + 10bits for col0 + 10bits for col2 + 10bits for col3
  // tb1: col1, 4, 5, ...
  // 16 bits for each element
  switch (col_id) {
    case 3:
      bit_offset -= FIXED_BITS_FIELD_0;
    case 2:
      bit_offset -= FIXED_BITS_FIELD_0;
    case 0:
      res = *(int32_t*) (storage_part_[0] + 4 * row_id);
      res = (res >> bit_offset) & ((1 << FIXED_BITS_FIELD_0) - 1);
      break;
    case 1:
      byte_id = 3;
    default:
      byte_id -= 3;
      byte_id *= FIXED_BITS_FIELD_1 >> 3;
      res = *(int16_t*) (storage_part_[1] + nbytespr_part_[1] * row_id + byte_id);
  }
  return res;
}

inline int32_t CustomTable::GetCol0AtRowIfCol2(int32_t row_id, int32_t threshold) {
  int32_t v = *(int32_t*) (storage_part_[0] + 4 * row_id);
  int32_t v0, v2;
  v0 = v >> 2 * FIXED_BITS_FIELD_0;
  v2 = (v >> FIXED_BITS_FIELD_0) & ((1 << FIXED_BITS_FIELD_0) - 1);
  return v2 < threshold ? v0 : 0;
}

inline void CustomTable::UpdateCol2ToCol3(int32_t row_id) {
  int32_t v = *(int32_t*) (storage_part_[0] + 4 * row_id);
  int32_t v2;
  v2 = (v >> FIXED_BITS_FIELD_0) & ((1 << FIXED_BITS_FIELD_0) - 1);
  // v3 = v & ((1 << FIXED_BITS_FIELD_0) - 1);
  v += v2;
  *(int32_t*) (storage_part_[0] + 4 * row_id) = v;
  // update sum_row
  UpdateRowSum(row_id, v2);
}


void CustomTable::PutIntField(int32_t row_id, int32_t col_id, int32_t field) {
  // TODO: Implement this!
  // assert(field < 1024 && field >= 0);
  int byte_id, bit_offset = 2 * FIXED_BITS_FIELD_0;
  int32_t res = 0;
  int32_t ori_val = 0;
  int32_t tmp;
  int64_t sum_diff;

  switch (col_id) {
    case 3:
      bit_offset -= FIXED_BITS_FIELD_0;
    case 2:
      bit_offset -= FIXED_BITS_FIELD_0;
    case 0:
      ori_val = *(int32_t*) (storage_part_[0] + 4 * row_id);
      tmp = ori_val & ((1 << bit_offset) - 1);
      ori_val = (ori_val >> bit_offset) & ((1 << FIXED_BITS_FIELD_0) - 1);
      sum_diff = field - ori_val;
      res = (ori_val >> (bit_offset + FIXED_BITS_FIELD_0) << 
          FIXED_BITS_FIELD_0) | field;
      res = (res << bit_offset) | tmp;
      break;
    case 1:
      byte_id = 3;
    default:
      byte_id -= 3;
      byte_id *= FIXED_BITS_FIELD_1 >> 3;
      ori_val = *(int16_t*) (storage_part_[1] + nbytespr_part_[1] * row_id + byte_id);
      sum_diff = field - ori_val;
      *(int16_t*) (storage_part_[1] + nbytespr_part_[1] * row_id + byte_id) = field;
  }

  // update cached sum
  UpdateRowSum(row_id, sum_diff);
  // if (__builtin_expect((col_id == 0 && sum_diff != 0), 0)) {
  if (sum_diff != 0) {
    if (col_id == 0) {
      sum_col0_ += sum_diff;
      PopIndex1D(index_0_, ori_val, row_id);
      PushIndex1D(index_0_, field, row_id);
    }
    if (col_id == 1) {
      PopIndex1D(index_1_, ori_val, row_id);
      PushIndex1D(index_1_, field, row_id);
    }
  }
}

int64_t CustomTable::ColumnSum() {
  // TODO: Implement this!
  int64_t res = 0;

  if (is_col0_sumed_)
    return sum_col0_;

  for (size_t i = 0; i < num_rows_; ++i)
    res += GetIntField(i, 0);
  is_col0_sumed_ = 1;
  return res;
}

int64_t CustomTable::PredicatedColumnSum(int32_t threshold1,
                                         int32_t threshold2) {
  // TODO: Implement this!
  int64_t res = 0;

  std::map<int16_t, std::vector<int32_t> >::iterator it;
  it = index_1_.lower_bound((int16_t) threshold1 + 1);
  for (; it != index_1_.end(); ++it) {
    for (int32_t row_id : it->second) {
      res += GetCol0AtRowIfCol2(row_id, threshold2);
    }
  }
  // for (size_t row_id = 0; row_id < num_rows_; ++row_id)
  //   if (GetIntField(row_id, 1) > threshold1 && GetIntField(row_id, 2) < threshold2)
  //     res += GetIntField(row_id, 0);
  return res;
}

int64_t CustomTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  // col0 > threshold
  int64_t res = 0;
  std::map<int16_t, std::vector<int32_t> >::iterator it;
  it = index_0_.lower_bound((int16_t) threshold + 1);
  for (; it != index_0_.end(); ++it) {
    std::vector<int32_t> &v = it->second;
    for (int32_t row_id : v) {
      res += GetRowSum(row_id);
    }
  }

  // for (size_t row_id = 0; row_id < num_rows_; ++row_id)
  //   if (GetIntField(row_id, 0) > threshold)
  //       res += GetRowSum(row_id);
  return res;
}

int64_t CustomTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!
  int64_t cnt = 0;
  std::map<int16_t, std::vector<int32_t> >::iterator it, it_end;
  it_end = index_0_.upper_bound((int16_t) threshold - 1);
  for (it = index_0_.begin(); it != it_end; ++it) {
    std::vector<int32_t> &v = it->second;
    for (int32_t row_id : v) {
      // PutIntField(row_id, 3, GetIntField(row_id, 3) + GetIntField(row_id, 2));
      UpdateCol2ToCol3(row_id);
      cnt++;
    }
  }
  // for (size_t row_id  = 0; row_id < num_rows_; ++row_id)
  //   if (GetIntField(row_id, 0) < threshold) {
  //     PutIntField(row_id, 3, GetIntField(row_id, 3) + GetIntField(row_id, 2));
  //     cnt++;
  //   }
  return cnt;
}
} // namespace bytedance_db_project
