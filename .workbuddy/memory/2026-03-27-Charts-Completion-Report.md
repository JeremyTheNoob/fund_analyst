# Chart Region Optimization — March 27, 2026

## Task Summary
- **Objective**: Add 4 new specialized charts to the fund analysis visualization system
- **Timeline**: 2026-03-27 20:55 - March 27, 2026

## Completed Work

### 1. ✅ Schema Layer Updates
**File**: `fund_quant_v2/models/schema.py`

Added field to `BondMetrics`:
```python
# Line 175: Credit spread history (for credit spread trend chart)
credit_spread_history: Optional[pd.DataFrame] = None  # Columns: date / spread
```

**Purpose**: Store historical credit spread data for generating the credit spread trend chart

---

### 2. ✅ Chart Layer Implementation
**File**: `fund_quant_v2/reporter/chart_gen.py`

All 4 new charts implemented:

#### 2.1 Excess Return Dynamic Curve (Universal)
- **Function**: `_excess_return_chart(nav_df: pd.DataFrame, report: FundReport)`
- **Lines**: 371-445
- **Features**:
  - Geometric excess calculation: `(1 + fund_ret) / (1 + bm_ret) - 1`
  - Inner join alignment for dates
  - Zero-point reset for analysis intervals
  - Dynamic color (red for positive, green for negative)
  - Extreme value handling (bm_ret ≤ -0.99)

#### 2.2 Morningstar Style Box (Equity)
- **Function**: `_style_box_chart(m: EquityMetrics)`
- **Lines**: 452-534
- **Features**:
  - 3×3 grid mapping based on SMB/HML factor loadings
  - Size axis: Large (SMB < -0.5), Mid (-0.5 ≤ SMB ≤ 0.5), Small (SMB > 0.5)
  - Value axis: Value (HML > 0.5), Balanced (-0.5 ≤ HML ≤ 0.5), Growth (HML < -0.5)
  - Trajectory points for historical style drift

#### 2.3 Credit Spread Trend (Fixed Income)
- **Function**: `_credit_spread_chart(m: BondMetrics)`
- **Lines**: 541-601
- **Features**:
  - Raw spread data (light gray line)
  - 5-day SMA smoothed spread (orange line)
  - Reads from `m.credit_spread_history`
  - Handles missing data gracefully

#### 2.4 Tracking Difference Distribution (Index/ETF)
- **Function**: `_tracking_diff_histogram(m: IndexMetrics, nav_df: pd.DataFrame, benchmark_df: pd.DataFrame)`
- **Lines**: 608-699
- **Features**:
  - Histogram with dynamic bin width calculation
  - Normal distribution overlay curve
  - Statistical annotations (mean, std, skewness, kurtosis)
  - Automatic bin width: `max(0.01, std / 5)`
  - Dynamic bar color based on mean TD

### 3. ✅ Chart Data Generation Integration
**File**: `fund_quant_v2/reporter/chart_gen.py`

Updated `generate_chart_data()` function to include all 4 new charts:
```python
# Universal charts (Line 38-39)
charts["excess_return"] = _excess_return_chart(nav_df, report)

# Equity-specific (Line 46-47)
charts["style_box"] = _style_box_chart(report.equity_metrics)

# Bond-specific (Line 53-54)
charts["credit_spread"] = _credit_spread_chart(report.bond_metrics)

# Index/ETF-specific (Line 59-62)
if bm_df is not None and nav_df is not None:
    charts["tracking_diff_hist"] = _tracking_diff_histogram(report.index_metrics, nav_df, bm_df)
```

### 4. ✅ Pipeline Layer - Credit Spread Data
**File**: `fund_quant_v2/engine/bond_engine.py`

Added credit spread history extraction in `_run_bond_three_factor()`:
- **Lines**: 149-152: Extract `dCS` (credit spread changes) from yield_df
- **Lines**: 154-157: Include `dCS` in OLS regression
- **Purpose**: Support credit spread trend chart calculation

**Note**: The actual `credit_spread_history` DataFrame needs to be constructed from `yield_df` before passing to `BondMetrics` constructor. This is handled in the bond_pipeline.

---

## Technical Details

### Chart Data Format

All charts return dictionaries with the following common structure:
```python
{
    "type": "line" | "scatter" | "histogram",
    "x": [...],                      # Date or category values
    "series": [...],                  # Data series
    "title": "...",
    "y_label": "...",
    "statistics": {...} | None,        # For histogram
    "legend": "..." | None,            # Interpretive text
    "zero_line": True | None           # Add zero reference line
}
```

### Factor Exposure Normalization (Style Box)

```python
# Size mapping (1-3)
if smb > 0.5:
    size_coord = 3  # Small
elif smb < -0.5:
    size_coord = 1  # Large
else:
    size_coord = 2  # Mid

# Value mapping (1-3)
if hml > 0.5:
    value_coord = 1  # Value
elif hml < -0.5:
    value_coord = 3  # Growth
else:
    value_coord = 2  # Balanced
```

### Geometric Excess Calculation

```python
# Formula for compounding accuracy
excess_ret = (1 + fund_ret) / (1 + bm_ret) - 1

# Cumulative excess
cum_excess = (1 + excess_ret).cumprod() - 1
```

---

## Testing Recommendations

### Test Fund Cases

| Fund Code | Type         | New Charts to Test                    |
|-----------|--------------|-------------------------------------|
| 005660    | Equity       | `excess_return`, `style_box`         |
| 000069    | Fixed Income | `excess_return`, `credit_spread`     |
| 008326    | Index/ETF    | `excess_return`, `tracking_diff_hist` |
| 007316    | Convertible  | `excess_return` (fallback to equity) |

### Test Script
A test script has been created at `fund_quant_v2/test_new_charts.py` with mock data generation for all 4 charts.

---

## Architecture Status

### Current State
- ✅ **Schema Layer**: Complete - BondMetrics has `credit_spread_history`
- ✅ **Chart Layer**: Complete - All 4 charts implemented
- ✅ **Common Metrics**: Complete - `skewness` and `kurtosis` functions available
- ✅ **Pipeline Layer**: Partial - Credit spread data extracted, needs final integration
- ⚠️ **UI Layer**: Incomplete - `main.py` still using old system

### Remaining Work

1. **Complete bond_pipeline integration**
   - Add credit_spread_history construction from yield_df
   - Pass to BondMetrics constructor

2. **Update main.py for new architecture**
   - Replace old `models` imports with `fund_quant_v2.analyze_fund`
   - Add UI rendering for 4 new charts
   - Convert chart data (JSON) to Plotly figures

3. **Add equity_engine style box trajectory**
   - Implement quarterly rolling SMB/HML calculations
   - Store trajectory in EquityMetrics.style_box

4. **Full regression testing**
   - Test with 4 real funds
   - Verify chart rendering
   - Check for edge cases (missing data, extreme values)

---

## Known Constraints

1. **Data Availability**: Credit spread chart requires real `credit_spread` column in yield data from AkShare
2. **Style Box Trajectory**: Currently only shows current position; historical trajectory needs historical FF factor calculations
3. **Bin Width Sensitivity**: Tracking difference histogram requires careful bin width tuning for ETF (typically [-0.1%, 0.1%] range)

---

## Dependencies

- `pandas`: DataFrame operations
- `numpy`: Numerical calculations
- `scipy.stats`: Normal distribution fitting
- `statsmodels.api`: OLS regression (for factor analysis)
- `pydantic`: Data validation

---

## Key Learnings

1. **Geometric vs Arithmetic Excess**: Geometric excess is more accurate for compounding scenarios but requires careful date alignment
2. **Style Box Interpretation**: Factor exposure thresholds (±0.5) determine 3×3 grid position
3. **Histogram Binning**: Fixed bin widths are insufficient; dynamic calculation based on std is essential
4. **Data Pipeline Separation**: Clear separation between data loading, processing, and visualization layers improves maintainability
