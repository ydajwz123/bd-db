#include "custom_table.h"
#include <cstring>
#include <assert.h>
#include <iostream>
#include <cstdio>

namespace bytedance_db_project {
CustomTable::CustomTable() {
  // Since ncols must larger than 3
  num_cols_tb_[0] = PART_ONE_NCOLS;
  num_cols_tb_[1] = PART_TWO_NCOLS;
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

CustomTable::BitPacker::BitPacker() : bit_offset_(0), 
  ptr_cur_(nullptr) {}

CustomTable::BitPacker::~BitPacker() {}

void CustomTable::BitPacker::set_ptr_cur(uint16_t* ptr_cur) {
  reset();
  ptr_cur_ = ptr_cur;
  *ptr_cur_ = 0;
}


int16_t CustomTable::BitPacker::swap_bytes(uint16_t val) {
  union {
    uint16_t integer;
    uint8_t  bytes[2];
  } a, b;
  a.integer = val;
  for (int i = 0; i < 2; ++i)
    b.bytes[i] = a.bytes[1-i];
  return b.integer;
}

int CustomTable::BitPacker::write(uint16_t val, size_t nbits) {
  size_t available = MAXB - bit_offset_;
  int len0 = (available >= nbits) ? nbits : available;
  int len1 = nbits - len0;
  assert(nbits <= MAXB);

  // Only in case val is not clean
  if (nbits != MAXB)
		val = val & ((1ULL << nbits) - 1ULL);
  if (len0) {
    *ptr_cur_ |= ((val >> len1) << (available - len0));
    bit_offset_ += len0;
  }
  // assuming space is enough, Otherwise should return -1;
  if (len1) {
    // swap Bytes (le to be)
    *ptr_cur_ = swap_bytes(*ptr_cur_);
    ptr_cur_++;
    *ptr_cur_ = val << (MAXB - len1);
    bit_offset_ = len1;
  }

  return nbits;
}

void CustomTable::BitPacker::flush() {
  if (!is_flushed_ && bit_offset_ != 0) {
    *ptr_cur_ = swap_bytes(*ptr_cur_);
  }
  is_flushed_ = 1;
  bit_offset_ = 0;
}

void CustomTable::BitPacker::reset() {
  bit_offset_ = 0;
  ptr_cur_ = nullptr;
  is_flushed_ = 0;
}

inline void CustomTable::PushIndex0(int16_t col_v, int32_t row_id) {
  index_0_[col_v].push_back(row_id);
}

inline void CustomTable::PopIndex0(int16_t col_v, int32_t row_id) {
  std::vector<int32_t> &v = index_0_[col_v];
  size_t N = v.size();
  for (size_t i = 0; i < N; ++i)
    if (v[i] == row_id) {
      for (size_t j = i; j < N - 1; ++j) {
        v[j] = v[j + 1];
      }
      break;
    }
  // delete
}

inline void CustomTable::PushIndex1(int16_t col_v, int32_t row_id) {
  index_1_[col_v].push_back(row_id);
}

inline void CustomTable::PopIndex1(int16_t col_v, int32_t row_id) {
  std::vector<int32_t> &v = index_1_[col_v];
  size_t N = v.size();
  for (size_t i = 0; i < N; ++i)
    if (v[i] == row_id) {
      for (size_t j = i; j < N - 1; ++j) {
        v[j] = v[j + 1];
      }
      break;
    }
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
  BitPacker bit_packer[TABLE_NPARTS];
  num_cols_ = loader->GetNumCols();
  auto rows = loader->GetRows();
  num_rows_ = rows.size();
  // since some queries requires col3
  assert(num_cols_ > 3);

  // allocate memory
  num_cols_tb_[2] = num_cols_ - num_cols_tb_[0] - num_cols_tb_[1];
  for (int i = 0; i < TABLE_NPARTS; ++i) {
    nbytes_part_[i] = (FIXED_BITS_FIELD * num_rows_ *
                       num_cols_tb_[i] + 7) >> 3;
    nbytes_part_[i] = (nbytes_part_[i] + 1) & (~0x1);
  }

  for (size_t i = 0; i < TABLE_NPARTS; ++i) {
    storage_part_[i] = new char[nbytes_part_[i]];
    bit_packer[i].set_ptr_cur((uint16_t*) storage_part_[i]);
  }
  int nbytes_sum = num_rows_ * (FIXED_BITS_SUM_FIELD) >> 3;
  nbytes_sum = (nbytes_sum + 7) & ~0x7;
  storage_sum_row_ = new char[nbytes_sum];

  for (size_t row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    int64_t sum = 0; // less than 1024 * 1024 (20bits)
    int32_t val;
    for (size_t col_id = 0; col_id < num_cols_; ++col_id) {
      int tb_id;
      val = *(int32_t*) (cur_row + FIXED_FIELD_LEN * col_id);
      // assert(val < 1024 && val >= 0);
      sum += val;
      if (col_id == 0) {
        sum_col0_ += val;
        PushIndex0(val, (int32_t) row_id);
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
      }
      bit_packer[tb_id].write((uint16_t) val, FIXED_BITS_FIELD);
    }
    PutRowSum(row_id, sum);
  }
  for (size_t i = 0; i < TABLE_NPARTS; ++i)
    bit_packer[i].flush();

  is_col0_sumed_ = 1;
}

inline int64_t CustomTable::GetRowSum(int32_t row_id) {
  int64_t val;
  val = *(int64_t*) (storage_sum_row_ + row_id * (FIXED_BITS_SUM_FIELD >> 3));
  val &= (1 << FIXED_BITS_SUM_FIELD) - 1;
  return val;
}

inline void CustomTable::PutRowSum(int32_t row_id, int64_t val) {
  int64_t ori_val;
  assert(val < 1024 * 1024 * 2);
  ori_val = *(int64_t*) (storage_sum_row_ + row_id * (FIXED_BITS_SUM_FIELD >> 3));
  ori_val &= ~((1 << FIXED_BITS_SUM_FIELD) - 1);
  ori_val |= val;
  *(int64_t*) (storage_sum_row_ + row_id * (FIXED_BITS_SUM_FIELD >> 3)) = ori_val;
}

int32_t CustomTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  // Note that: ONE val need TWO Bytes data
  int byte_id, bit_offset, len0, len1;
  uint8_t v1, v2;
  int32_t res = 0;
  int tb_id; // table idx

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

  len0 = 8 - bit_offset; // bits in first Byte
  len1 = FIXED_BITS_FIELD - len0;
  res |= ((int32_t) v1 & ((1 << len0) - 1)) << len1;
  res |= (v2 >> (8 - len1));
  // printf("byte_id: %d, bit_offset: %d, v1: %2X, v2: %2X", byte_id, bit_offset, v1, v2);

  return res;
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
  int byte_id, bit_offset, len0, len1;
  uint8_t v1, v2;
  int32_t ori_val = 0;
  int tb_id; // table idx
  int64_t sum_diff;

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
    switch (col_id) {
      case 0:
        PushIndex0(field, row_id);
        sum_col0_ += sum_diff;
        PopIndex0(ori_val, row_id);
        break;
      case 1:
        PushIndex1(field, row_id);
        PopIndex1(ori_val, row_id);
        break;
    }
  }
  // if (col_id == 1 || col_id == 2) {
  //   PushIndex12(GetIntField(row_id, 1), GetIntField(row_id, 2), row_id);
  // }
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
  std::map<int16_t, std::vector<int32_t> >::iterator it;
  it = index_1_.lower_bound((int16_t) threshold1 + 1);
  for (; it != index_1_.end(); ++it) {
    for (int32_t row_id : it->second) {
      if (GetIntField(row_id, 2) < threshold2)
        res += GetIntField(row_id, 0);
    }
  }
  // for (size_t row_id = 0; row_id < num_rows_; ++row_id)
  //   if (GetIntField(row_id, 1) > threshold1 && GetIntField(row_id, 2) < threshold2)
  //     res += GetIntField(row_id, 0);
  return res;
}

int64_t CustomTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
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
      Update2to3(row_id);
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
