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

inline void CustomTable::PushIndex0(int16_t col_v, int32_t row_id) {
  index_0_[col_v].push_back(row_id);
}

inline void CustomTable::PopIndex0(int16_t col_v, int32_t row_id) {
  std::vector<int32_t> &v = index_0_[col_v];
  std::vector<int>::iterator it = std::find(v.begin(), v.end(), row_id);
  v.erase(it);
  // size_t N = v.size();
  // for (size_t i = 0; i < N; ++i)
  //   if (v[i] == row_id) {
  //     for (size_t j = i; j < N - 1; ++j) {
  //       v[j] = v[j + 1];
  //     }
  //     break;
  //   }
  // delete
}

inline void CustomTable::PushIndex1(int16_t col_v, int32_t row_id) {
  index_1_[col_v].push_back(row_id);
}

inline void CustomTable::PopIndex1(int16_t col_v, int32_t row_id) {
  std::vector<int32_t> &v = index_1_[col_v];
  std::vector<int>::iterator it = std::find(v.begin(), v.end(), row_id);
  v.erase(it);
  // size_t N = v.size();
  // for (size_t i = 0; i < N; ++i)
  //   if (v[i] == row_id) {
  //     for (size_t j = i; j < N - 1; ++j) {
  //       v[j] = v[j + 1];
  //     }
  //     break;
  //   }
  // delete
}

void CustomTable::PushIndex12(int16_t col1_v, int16_t col2_v, int32_t row_id) {
  index_1_2_[col1_v][col2_v].push_back(row_id);
}

void CustomTable::PopIndex12(int16_t col1_v, int16_t col2_v, int32_t row_id) {
  std::vector<int32_t> &v = index_1_2_[col1_v][col2_v];
  for (auto it = v.begin(); it != v.end(); ++it) {
    if (*it == row_id) {
      v.erase(it);
      break;
    }
  }
  // delte node
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
<<<<<<< HEAD
      if (col_id == 0) {
        sum_col0_ += val;
        PushIndex0(val, (int32_t) row_id);
        // test
        // auto rng = std::default_random_engine {};
        // std::shuffle(index_0_[val].begin(), index_0_[val].end(), rng);
        tb_id = 0;
      }
      else if (col_id == 2 || col_id == 3) {
        tb_id = 1;
      }
      else {
        tb_id = 2;
      }
      // if (col_id == 2) {
      //   PushIndex12(prev_val, val, row_id);
      // }
      if (col_id == 1) {
        PushIndex1(val, row_id);
        // auto rng = std::default_random_engine {};
        // std::shuffle(index_1_[val].begin(), index_1_[val].end(), rng);
      }
      bit_packer[tb_id].write((uint16_t) val, FIXED_BITS_FIELD);
=======
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
>>>>>>> DevTwoTableNG
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

<<<<<<< HEAD
  switch (col_id) {
    case 0 : 
      tb_id = 0;
      break;
    case 2:
    case 3:
      tb_id = 1;
      col_id -= 2;
      break;
    default:
      tb_id = 2;
      col_id -= 1 + ((col_id != 1) ? 2 : 0);
  }
  
  byte_id = ((row_id * num_cols_tb_[tb_id] + col_id) * FIXED_BITS_FIELD >> 3); 
  bit_offset = (row_id * num_cols_tb_[tb_id] + col_id) * FIXED_BITS_FIELD - (byte_id << 3);

  v1 = *(uint8_t*) (storage_part_[tb_id] + byte_id);
  v2 = *(uint8_t*) (storage_part_[tb_id] + byte_id + 1);
=======
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
>>>>>>> DevTwoTableNG

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

void CustomTable::Update2to3(int32_t row_id) {
  // table II ,tb_id = 1
  int byte_id[2], bit_offset[2], len0[2];
  uint8_t v1[2], v2[2];
  int32_t ori_val[2] = {0};
  
  for (int i = 0; i < 2; ++i) {
    byte_id[i] = ((row_id * PART_TWO_NCOLS) * FIXED_BITS_FIELD >> 3); 
    bit_offset[i] = ((row_id * PART_TWO_NCOLS) * FIXED_BITS_FIELD - (byte_id[i] << 3)); 

    v1[i] = *(uint8_t*) (storage_part_[1] + byte_id[i]);
    v2[i] = *(uint8_t*) (storage_part_[1] + byte_id[i] + 1);

    len0[i] = 8 - bit_offset[i]; // bits in first Byte
    ori_val[i] |= ((int32_t) v1[i] & ((1 << len0[i]) - 1)) << (FIXED_BITS_FIELD - len0[i]);
    ori_val[i] |= (v2[i] >> (len0[i] - 2));
  }
  ori_val[1] += ori_val[0];
  v1[1] = ((v1[1] >> len0[1]) << len0[1]) | (uint8_t) ((ori_val[1]) >> (FIXED_BITS_FIELD - len0[1]));
  // caution, v2 will auto extend to more bits if v2 << len1 >> len1
  v2[1] = (v2[1] & ((1 << (len0[1] - 2)) - 1)) | ((uint8_t) ori_val[1] << (len0[1] - 2));
  *(uint8_t*) (storage_part_[1] + byte_id[1]) = v1[1];
  *(uint8_t*) (storage_part_[1] + byte_id[1] + 1) = v2[1];
  if (ori_val[0] != 0)
    PutRowSum(row_id, GetRowSum(row_id) + ori_val[0]);
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
<<<<<<< HEAD
    case 0 : 
      tb_id = 0;
      break;
    case 2:
    case 3:
      tb_id = 1;
      col_id -= 2;
      break;
    default:
      tb_id = 2;
      col_id -= 1 + ((col_id != 1) ? 2 : 0);
  }

  byte_id = ((row_id * num_cols_tb_[tb_id] + col_id) * FIXED_BITS_FIELD >> 3); 
  bit_offset = (row_id * num_cols_tb_[tb_id] + col_id) * FIXED_BITS_FIELD - (byte_id << 3);

  v1 = *(uint8_t*) (storage_part_[tb_id] + byte_id);
  v2 = *(uint8_t*) (storage_part_[tb_id] + byte_id + 1);

  len0 = 8 - bit_offset; // bits in first Byte
  len1 = FIXED_BITS_FIELD - len0;
  ori_val |= ((int32_t) v1 & ((1 << len0) - 1)) << len1;
  ori_val |= (v2 >> (8 - len1));
  sum_diff = field - ori_val;

  // if (col_id == 1 || col_id == 2) {
  //   PopIndex12(GetIntField(row_id, 1), GetIntField(row_id, 2), row_id);
  // }
  // update value and put value in storage
  v1 = ((v1 >> len0) << len0) | (uint8_t) (field >> len1);
  // caution, v2 will auto extend to more bits if v2 << len1 >> len1
  v2 = (v2 & ((1 << (8 - len1)) - 1)) | ((uint8_t) field << (8 - len1));
  *(uint8_t*) (storage_part_[tb_id] + byte_id) = v1;
  *(uint8_t*) (storage_part_[tb_id] + byte_id + 1) = v2;

  // update cached sum && index
  if (sum_diff != 0) {
    PutRowSum(row_id, GetRowSum(row_id) + sum_diff);
    // if (col_id == 0) {
    //   PushIndex0(field, row_id);
    //   sum_col0_ += sum_diff;
    //   PopIndex0(ori_val, row_id);
    // }
    // std::vector<int>::iterator it;
    // std::vector<int32_t> *v;
    
    switch (col_id) {
      case 0:
        sum_col0_ += sum_diff;
        // v = &index_0_[ori_val];
        // it = std::find(v->begin(), v->end(), row_id);
        // v->erase(it);
        // index_0_[field].push_back(row_id);
        PushIndex0(field, row_id);
        PopIndex0(ori_val, row_id);
        break;
      case 1:
        PushIndex1(field, row_id);
        PopIndex1(ori_val, row_id);
        // v = &index_1_[ori_val];
        // it = std::find(v->begin(), v->end(), row_id);
        // v->erase(it);
        // index_1_[field].push_back(row_id);
        break;
    }
  }
  // if (col_id == 1 || col_id == 2) {
  //   PushIndex12(GetIntField(row_id, 1), GetIntField(row_id, 2), row_id);
  // }
=======
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
>>>>>>> DevTwoTableNG
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

<<<<<<< HEAD
  // std::map<int16_t, std::map<int16_t, std::vector<int32_t> > >::iterator it1;
  // std::map<int16_t, std::vector<int32_t> >::iterator it2, it2_end;
  // it1 = index_1_2_.lower_bound((int16_t) threshold1 + 1);

  // for (; it1 != index_1_2_.end(); ++it1) {
  //   std::map<int16_t, std::vector<int32_t> > &m = it1->second;
  //   it2_end = m.upper_bound((int16_t) threshold2 - 1);
  //   for (it2 = m.begin(); it2 != it2_end; ++it2) {
  //     for (int32_t row_id : it2->second) {
  //       res += GetIntField(row_id, 0);
  //     }
  //   }
  // }
=======
>>>>>>> DevTwoTableNG
  std::map<int16_t, std::vector<int32_t> >::iterator it;
  it = index_1_.lower_bound((int16_t) threshold1 + 1);
  for (; it != index_1_.end(); ++it) {
    for (int32_t row_id : it->second) {
<<<<<<< HEAD
      if (GetIntField(row_id, 2) < threshold2)
        res += GetIntField(row_id, 0);
=======
      res += GetCol0AtRowIfCol2(row_id, threshold2);
>>>>>>> DevTwoTableNG
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
<<<<<<< HEAD
=======

>>>>>>> DevTwoTableNG
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
<<<<<<< HEAD
      Update2to3(row_id);
=======
      UpdateCol2ToCol3(row_id);
>>>>>>> DevTwoTableNG
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
