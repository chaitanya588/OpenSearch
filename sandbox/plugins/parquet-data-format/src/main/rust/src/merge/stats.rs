/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Comparison and merge statistics for benchmarking.
//! Gated behind `test-utils` feature to avoid any overhead in production.

use std::cell::Cell;

// Per-batch sort comparison counter (used by sort_batch_row_conversion).
thread_local! {
    static SORT_COMPARISONS: Cell<u64> = const { Cell::new(0) };
}

/// K-way merge statistics.
#[derive(Debug, Clone, Default)]
pub struct MergeStats {
    pub heap_pops: u64,
    pub heap_pushes: u64,
    pub tier1_rows: u64,
    pub tier2_skips: u64,
    pub tier2_rows: u64,
    pub tier3_searches: u64,
    pub tier3_comparisons: u64,
    pub tier3_rows: u64,
}

/// Combined stats from a full sort_large_file run.
#[derive(Debug, Clone, Default)]
pub struct SortStats {
    /// Total comparisons across all per-chunk sorts.
    pub chunk_sort_comparisons: u64,
    /// Number of chunks sorted.
    pub chunk_count: u64,
    /// Total size of all chunk files in bytes (before merge and cleanup).
    pub chunk_total_bytes: u64,
    /// K-way merge stats.
    pub merge: MergeStats,
}

impl SortStats {
    pub fn total_comparisons(&self) -> u64 {
        self.chunk_sort_comparisons
            + self.merge.tier2_skips       // each skip is one cmp_sort_values call
            + self.merge.tier3_comparisons // binary search comparisons
            + self.merge.heap_pops         // each pop triggers a comparison in the heap
    }
}

pub fn reset_sort_comparisons() {
    SORT_COMPARISONS.with(|c| c.set(0));
}

pub fn increment_sort_comparisons() {
    SORT_COMPARISONS.with(|c| c.set(c.get() + 1));
}

pub fn get_sort_comparisons() -> u64 {
    SORT_COMPARISONS.with(|c| c.get())
}
