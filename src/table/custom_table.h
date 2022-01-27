#pragma once
#include "table.h"
#include <vector>
#include <list>
#include <map>

// Since DATA ranges [0, 1023), 10 bits are enough. 
// Bottleneck should be of data fetch, 
// rather than computation complexity of offset.
#define FIXED_BITS_FIELD     10
#define FIXED_BITS_SUM_FIELD 24 // 24 bits should be enough
#define TABLE_NPARTS   2
#define PART_ONE_NCOLS 4

#define MAXB 16
namespace bytedance_db_project {
//
// Custom table implementation to adapt to provided query mix.
//
class CustomTable : Table {
public:
  CustomTable();
  ~CustomTable();

  // Loads data into the table through passed-in data loader. Is not timed.
  void Load(BaseDataLoader *loader) override;

  // Returns the int32_t field at row `row_id` and column `col_id`.
  int32_t GetIntField(int32_t row_id, int32_t col_id) override;
  
  int64_t GetRowSum(int32_t row_id);
  void UpdateRowSum(int32_t row_id, int64_t val_diff);

  // Inserts the passed-in int32_t field at row `row_id` and column `col_id`.
  void PutIntField(int32_t row_id, int32_t col_id, int32_t field) override;

  // Implements the query
  // SELECT SUM(col0) FROM table;
  // Returns the sum of all elements in the first column of the table.
  int64_t ColumnSum() override;

  // Implements the query
  // SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
  // Returns the sum of all elements in the first column of the table,
  // subject to the passed-in predicates.
  int64_t PredicatedColumnSum(int32_t threshold1, int32_t threshold2) override;

  // Implements the query
  // SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 >
  // threshold; Returns the sum of all elements in the rows which pass the
  // predicate.
  int64_t PredicatedAllColumnsSum(int32_t threshold) override;

  // Implements the query
  // UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
  // Returns the number of rows updated.
  int64_t PredicatedUpdate(int32_t threshold) override;

private:
  class BitPacker {
    public:
      BitPacker();
      ~BitPacker();
      void set_ptr_cur(uint16_t* ptr_cur);
      int write(uint16_t val, size_t nbits);
      void flush();
      int16_t swap_bytes(uint16_t ptr);
      void reset();
    private:
      size_t bit_offset_{0};
	    uint16_t* ptr_cur_{nullptr};
      bool is_flushed_{0};
  };

  uint32_t num_cols_{0};
  uint32_t num_rows_{0};
  // take storage into two part
  // part I: sum, col0, col1, col2, col3
  // part II: col4, col5, ...
  char *storage_part_[TABLE_NPARTS];
  // N Bytes per row for part1, will align to Bytes for convenience
  size_t nbytespr_part_[TABLE_NPARTS];
  bool is_col0_sumed_{0};
  int64_t sum_col0_{0};
  // indexed for col0
  std::map<int16_t, std::vector<int32_t> > index_0_;
  std::map<int16_t, std::vector<int32_t> > index_1_;
  std::map<int16_t, std::map<int16_t, std::vector<int32_t> > > index_1_2_;

  void PushIndex0(int16_t col_v, int32_t row_id);
  void PopIndex0(int16_t col_v, int32_t row_id);
  void PushIndex1(int16_t col_v, int32_t row_id);
  void PopIndex1(int16_t col_v, int32_t row_id);
  void PushNumToVec(std::vector<int32_t>& v, int32_t n);
};
} // namespace bytedance_db_project
