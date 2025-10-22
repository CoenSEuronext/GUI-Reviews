Euronext Index Review System - Technical Documentation
Table of Contents

System Overview
Architecture and Data Flow
Document Explanations
How the System Works
Data Sources and Formats
Review Process Explained 
Quality Control and Validation


System Overview
This is a Flask-based web application that automates the quarterly review process for 60+ Euronext stock indices. The system replaces manual Excel calculations with automated Python scripts that:

Load market data from network sources
Apply index-specific methodology rules
Calculate constituent weights and share counts
Generate standardized Excel outputs
Track changes through inclusion/exclusion analysis
Validate outputs against reference implementations

Why This System Exists
Before: Index reviews were manual, time-consuming processes involving:

Copying data between multiple Excel files
Manual calculations prone to human error
Hours of work per index
Difficult to validate and audit

After: Automated reviews that:

Process in minutes instead of hours
Apply consistent methodology
Generate audit trails
Enable batch processing of multiple indices
Provide quality control through automated comparison

Key Capabilities

Single Review Mode: Calculate one index at a time with immediate results
Batch Review Mode: Process multiple indices sequentially (e.g., all French indices)
Background Processing: Long-running calculations don't block the user interface
Progress Tracking: Real-time updates on calculation status
Persistent Tasks: Reviews survive server restarts (tasks stored as JSON)
Auto-opening Results: Automatically opens Excel files on completion (for local users)
Quality Control: Automated comparison tools validate outputs against reference data


Architecture and Data Flow
High-Level System Architecture
┌─────────────────────────────────────────────────────────────────┐
│                     USER INTERACTION LAYER                      │
│  Web Browser → HTML Interface (Document 3)                      │
│  - Form inputs (dates, index selection, currency)               │
│  - Real-time progress display                                   │
│  - Result links and status messages                             │
└────────────────────┬────────────────────────────────────────────┘
                     │ HTTP Requests (POST/GET)
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                    APPLICATION LAYER                            │
│  Flask Web Server (Document 1)                                  │
│  - Route handling (/calculate, /calculate-batch, /task-status) │
│  - Request validation and parsing                               │
│  - Response formatting (JSON)                                   │
└────────────────────┬────────────────────────────────────────────┘
                     │ Function Calls
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                  ORCHESTRATION LAYER                            │
│  Task Manager (Document 9/10)                                   │
│  - Background thread management                                 │
│  - Task queuing and status tracking                             │
│  - Progress updates (0-100%)                                    │
│  - Concurrency control (max 3 simultaneous)                     │
└────────────────────┬────────────────────────────────────────────┘
                     │ Review Execution
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ROUTING LAYER                                │
│  Review Logic Router (Document 2)                               │
│  - Maps review type code to specific function                   │
│  - Error handling and timing                                    │
│  - Result standardization                                       │
└────────────────────┬────────────────────────────────────────────┘
                     │ Specific Review Call
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                   CALCULATION LAYER                             │
│  Individual Review Scripts (Document 11 + 60+ others)           │
│  - Index-specific methodology implementation                    │
│  - Data loading and transformation                              │
│  - Weight calculations and capping                              │
│  - Number of shares determination                               │
└────────────────────┬────────────────────────────────────────────┘
                     │ Data Requests
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                     DATA LAYER                                  │
│  Data Loaders (Document 4)                                      │
│  - Network file access (EOD data, reference files)              │
│  - CSV/Excel parsing                                            │
│  - Data validation and cleaning                                 │
│  - Geographic fallback logic                                    │
└────────────────────┬────────────────────────────────────────────┘
                     │ Raw Data
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                   UTILITY LAYER                                 │
│  Helper Functions (Capping, Inclusion/Exclusion Analysis)       │
│  - Weight capping algorithms                                    │
│  - Constituent change detection                                 │
│  - Formatting and standardization                               │
└─────────────────────────────────────────────────────────────────┘
                     │ Results
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                    OUTPUT LAYER                                 │
│  Excel File Generation                                          │
│  - Multi-sheet workbooks                                        │
│  - Formatted composition tables                                 │
│  - Inclusion/exclusion reports                                  │
│  - Full universe for audit                                      │
└─────────────────────────────────────────────────────────────────┘
```

### Parallel Quality Control System
```
┌─────────────────────────────────────────────────────────────────┐
│              STANDALONE VALIDATION TOOLS                        │
│  File Comparison Scripts (Documents 6-7)                        │
│  - Load outputs from two sources (Coen vs Dataiku)             │
│  - Compare ISINs, shares, free floats, capping factors          │
│  - Generate detailed mismatch reports                           │
│  - Highlight discrepancies for investigation                    │
└─────────────────────────────────────────────────────────────────┘

Document Explanations
Document 1: Flask Application (app.py)
Purpose: This is the main web server that handles all HTTP requests and coordinates the entire system.
What It Does:

Route Definition: Sets up URL endpoints that the frontend can call

/ → Serves the HTML interface
/calculate → Starts a single index review
/calculate-batch → Starts multiple reviews in sequence
/task-status/<task_id> → Returns current progress of a running task
/cancel-task/<task_id> → Cancels a pending task
/cleanup-tasks → Removes old completed tasks
/system-status → Returns system capacity information


Request Processing:

Validates form data (dates must be in correct format, review types must exist)
Checks if the request is local (for auto-opening files)
Extracts parameters like currency, dates, review types


Task Creation:

Generates unique task IDs (UUIDs)
Hands off execution to the task manager
Returns immediate response with task ID (doesn't wait for completion)


Error Handling:

Catches validation errors (e.g., invalid date format)
Returns user-friendly error messages
Logs detailed errors for debugging



Key Design Decision: Non-blocking architecture - when you submit a review, the server immediately returns a task ID and the calculation runs in the background. This allows the user interface to remain responsive.

Document 2: Review Logic Router (review_logic.py)
Purpose: Acts as a central dispatcher that maps index codes to their calculation functions.
What It Does:

Maintains the Function Registry: The REVIEW_FUNCTIONS dictionary is the system's "phone book" - it knows which function to call for each index:

python  "FRI4P" → run_fri4p_review()
  "AEXEW" → run_aexew_review()
  "GICP" → run_gicp_review()
  # ... 60+ mappings

Unified Execution Interface: Provides a single run_review() function that:

Accepts a review type and parameters
Looks up the correct function
Calls it with the provided parameters
Wraps results in a standardized format


Timing and Metadata: Automatically adds:

Start/end timestamps
Duration in seconds
Completion time (ISO format)


Error Standardization: All errors are caught and formatted consistently:

python  {
      "status": "error",
      "message": "Human-readable error",
      "traceback": "Full stack trace for debugging",
      "duration_seconds": 5.23
  }

Thread-Local Storage: Uses Python's threading.local() to track which review is running in each thread (useful for debugging concurrent executions)

Why This Exists: Without this router, each piece of code would need to know about all 60+ review functions. With it, you just call run_review("FRI4P", ...) and the router handles the rest.

Document 3: HTML Frontend (index.html)
Purpose: The user interface that people interact with in their web browser.
What It Provides:

Mode Selector:

Single Review: Calculate one index
Multiple Reviews: Batch process several indices


Single Review Mode:

Dropdown to select index type
Auto-populated index code and ISIN (read-only)
Date inputs with format validation
Currency selector
Auto-open checkbox


Multiple Review Mode:

Scrollable list of all available indices
Checkboxes with index details (name, ISIN, mnemonic)
"Select All" / "Select None" buttons
Selection counter (e.g., "5 selected")


Form Validation:

Date format checking (YYYYMMDD, DD-MMM-YY)
Required field enforcement
At least one review selected in batch mode


Real-Time Progress:

Progress bar (0-100%)
Status messages ("Processing FRI4P (2/5)")
Task status indicator (Pending, Running, Completed, Failed)
Links to completed files


Results Display:

Success/error visual indicators (green/red backgrounds)
Duration information
Download links to generated Excel files
Detailed error messages when failures occur



Technical Implementation:

Pure JavaScript (no frameworks) for simplicity
2-second polling to check task status
Prevents duplicate submissions while tasks are running
Disables form during execution
Automatically stops polling when task completes

Design Philosophy: The interface prioritizes clarity and feedback. Users always know what's happening, how long it's taking, and what the results are.

Document 4: Data Loader (data_loader.py)
Purpose: Centralized data loading logic that handles all the complexity of reading files from multiple sources.
What It Does:

EOD (End of Day) Data Loading:

python   load_eod_data(date, co_date, area, area2, dlf_folder)
The Challenge: Market data files may exist in US folders, EU folders, or both. Files may be missing. The function must be resilient.
The Solution:

Tries to load from both geographic areas
Combines data from multiple sources when available
Returns None for missing files (with warnings) rather than crashing
Automatically merges index currency information into stock data

Returns Three DataFrames:

index_eod_df: Index-level data (market caps, prices)
stock_eod_df: Stock data for the calculation date
stock_co_df: Stock data for the cut-off date

Why Two Stock DataFrames? Index reviews use different dates:

Calculation Date: When we run the numbers (e.g., March 20)
Cut-off Date: Historical date used for eligibility (e.g., February 21)


Reference Data Loading:

python   load_reference_data(folder, required_files, sheet_names)
Handles 40+ Different Reference Files:

Free float data (FF.xlsx)
Industry classifications (ICB.xlsx, NACE.xlsx)
ESG ratings (Oekom, SESAMm, CDP)
Universe definitions (Developed Market.xlsx, Emerging Market.xlsx)
Index family data (CAC Family.xlsx, AEX Family.xlsx)
Tax jurisdiction lists (GAFI lists, EU fiscal cooperation)

Smart Loading:

Only loads requested files (saves memory/time)
Can specify which Excel sheet to read
Validates sheet existence before loading
Returns None for missing files (caller decides how to handle)

Sheet Selection Example:
python   ref_data = load_reference_data(
       folder,
       required_files=['cac_family'],
       sheet_names={'cac_family': 'PX1'}  # Load "PX1" sheet, not default
   )
```

3. **Graceful Degradation:**
   - If US files are missing but EU files exist, it continues
   - If a reference file is missing, it returns `None` and lets the review decide
   - Logs warnings but doesn't crash unnecessarily

**Why This Matters:** Data loading is error-prone (network issues, missing files, format changes). Centralizing this logic means we handle these issues once, correctly, rather than 60+ times in different review scripts.

---

### Document 5: Batch Processor (`batch_processor.py`)

**Purpose:** Handles execution of multiple reviews, ensuring they don't conflict or overwhelm system resources.

**What It Does:**

1. **Sequential Processing:**
```
   Instead of running all reviews in parallel:
   Review 1 → Complete
   Review 2 → Complete
   Review 3 → Complete
Why Sequential? Excel file generation and data loading can conflict if run simultaneously. Sequential processing is slower but more reliable.

Progress Tracking:

Updates after each review completes
Calculates percentage (e.g., "3 of 10 completed = 30%")
Provides review-specific status messages


Duplicate Prevention:

python   processed_reviews = set()  # Track what we've processed
If a user accidentally selects the same index twice, it only runs once.

Individual Result Tracking:
Each review in the batch gets its own result object:

python   {
       "review_type": "FRI4P",
       "status": "success",
       "message": "Review completed",
       "duration_seconds": 45.2,
       "data": {"output_path": "..."}
   }

Batch Summary:

python   {
       "summary": {
           "total": 10,
           "successful": 8,
           "failed": 2
       },
       "results": [...]  # Individual results
   }
```

6. **Auto-Open Management:**
   - Opens files sequentially as each review completes
   - Only works for local users (security)
   - Doesn't crash if auto-open fails

**Design Trade-off:** Sequential processing is slower than parallel, but eliminates resource conflicts, file locking issues, and makes debugging easier (you know exactly which review is running).

---

### Documents 6 & 7: File Comparison Tools

**Purpose:** Quality control - validate that automated outputs match reference implementations or previous versions.

**Document 6: EIA-Specific Comparison**
- **Specialized for EIA Reviews:** Handles the specific quirk that Dataiku files have headers in row 2 while Coen files have headers in row 1
- **Ultra-High Precision:** Uses 14 decimal places for float comparisons (some financial calculations are extremely precise)
- **Company Name Mapping:** Includes company names in difference reports for easier human review
- **Three-Sheet Output:**
  - Summary: Overall match status for each index
  - Field Mismatches: Detailed differences (which ISIN, which field, what values)
  - ISIN Differences: Complete list of ISINs only in one file or the other

**Document 7: General Comparison**
- **Broader Application:** Works for any review type
- **Standard Precision:** Uses 10 decimal places (sufficient for most cases)
- **Effective Date Checking:** Includes validation of the review effective date
- **Currency Equivalence:** Understands that GBX = GBP, ILA = ILS (different currency code conventions)

**What Gets Compared:**
1. **ISIN Lists:**
   - Are the same companies included?
   - Any additions or removals?
   - Same count in both files?

2. **Field Values (per ISIN):**
   - MIC (Market Identifier Code)
   - Number of Shares
   - Free Float
   - Capping Factor
   - Currency
   - Effective Date (Document 7 only)

3. **Tolerances:**
   - **Exact Match:** MIC, Currency, Effective Date
   - **Float Tolerance:** Shares, Free Float, Capping (allows tiny rounding differences)

**Output Report Structure:**
```
Summary Sheet:
- Mnemo | ISIN Count Match | ISIN Set Match | MIC Match | Shares Match | ... | Status

Field Mismatches Sheet:
- Mnemo | Field | ISIN | Coen Value | Dataiku Value

ISIN Differences Sheet:
- Mnemo | ISIN | Company | Present In (Coen Only / Dataiku Only)
Typical Workflow:

Run both Coen's process and your automated process
Save outputs to designated folders
Run comparison script
Review generated Excel report
Investigate any discrepancies

Use Cases:

Validation: Confirm new script produces correct results
Migration: When moving from manual to automated
Debugging: Find exactly which ISINs or fields differ
Auditing: Document that outputs match reference data


Document 8: Configuration (config.py)
Purpose: Central repository for all system configuration - no hardcoded values scattered across files.
What It Contains:

Network Paths:

python   DLF_FOLDER = r"\\pbgfshqa08601v\gis_ttm\Archive"  # Source data location
   DATA_FOLDER = r"V:\PM-Indices-IndexOperations\Review Files"  # Reference data
   DATA_FOLDER2 = r"C:\Users\...\GUI + Reviews"  # Working directory
These are Windows network paths to shared drives where market data lives.

Index Configurations (60+ Indices):

python   INDEX_CONFIGS = {
       "FRI4P": {
           "index": "FRI4P",      # Index mnemonic code
           "isin": "FRIX00003643", # Index ISIN identifier
           "output_key": "fri4p_path"  # Key for output file path in results
       },
       # ... 60+ more
   }
Why This Structure?

Frontend uses it to populate dropdown menus
Backend uses it to validate requests
Review scripts use it to get ISIN codes
All in one place, single source of truth


Batch Processing Limits:

python   BATCH_CONFIG = {
       "max_concurrent_reviews": 3,  # Don't overload system
       "timeout_minutes": 30,        # Kill stuck reviews
       "retry_attempts": 1,          # How many times to retry failures
       "progress_update_interval": 1 # Seconds between status updates
   }

Predefined Review Groups:

python   REVIEW_GROUPS = {
       "french_indices": ["FRI4P", "FRD4P", "FRECP", "FRN4P", "FR20P"],
       "dutch_indices": ["EDWP", "EDWPT", "GICP", "AERDP", "BNEW"],
       "eurozone_indices": ["EZ40P", "EZ60P", "EZ15P", "EZN1P"],
       "all_indices": list(INDEX_CONFIGS.keys())
   }
Use Case: Want to run all French indices? Just:
python   review_types = get_review_group("french_indices")
```

5. **Helper Functions:**
   - `get_index_config(review_type)` - Look up config by code
   - `get_batch_config()` - Get batch settings
   - `get_review_group(name)` - Get predefined group

**Benefits of Centralization:**
- Adding a new index? Edit one file (config.py), automatically available everywhere
- Change a network path? One edit, system-wide effect
- Adjust concurrency? One setting controls behavior
- No magic strings scattered through codebase

---

### Document 9: Enhanced Task Manager (Production)

**Purpose:** Industrial-strength task management with persistence - tasks survive server restarts.

**Core Concept: Task Lifecycle**
```
PENDING → RUNNING → COMPLETED
                 ↘ FAILED
                 ↘ CANCELLED
What It Does:

Task Creation:

python   task_id = task_manager.create_task(
       task_type="single",  # or "batch"
       review_type="FRI4P",
       date="20250320",
       # ... more parameters
   )
```
   - Generates unique UUID for the task
   - Creates `TaskResult` object with metadata
   - Saves to JSON file immediately (persistence)
   - Returns task ID to caller

2. **Persistent Storage:**
```
   task_storage/
   ├── 123e4567-e89b-12d3-a456-426614174000.json
   ├── 234e5678-f90b-23d4-b567-537725285111.json
   └── ...
Each JSON file contains:
json   {
       "task_id": "123e4567...",
       "task_type": "single",
       "status": "running",
       "created_at": "2025-10-17T10:30:00",
       "started_at": "2025-10-17T10:30:05",
       "progress": 45,
       "message": "Processing data...",
       "review_type": "FRI4P",
       "parameters": {...}
   }

Background Execution:

Spawns daemon threads for each task
Threads don't block the main Flask server
Updates task status in real-time
Saves state after every status change


Server Restart Handling:

On startup, loads all existing tasks from JSON files
Marks any RUNNING or PENDING tasks as FAILED (server crashed while they were executing)
Historical completed tasks remain available


Progress Tracking:

python   Progress Scale:
   0% → Task created
   10% → Started loading data
   50% → Data loaded, calculating
   90% → Calculations complete, saving
   100% → Done

Concurrency Control:

python   max_concurrent_tasks = 3
   
   if get_running_tasks_count() >= 3:
       # Don't start new tasks, queue them as PENDING
Prevents system overload.

Auto-Open Logic:

python   if auto_open and is_local_request and status == success:
       os.startfile(output_path)  # Open Excel file
Only works if:

User checked "auto-open" box
Request came from local machine (security)
Review succeeded


Cleanup:

python   cleanup_old_tasks(max_age_hours=24)
Removes completed/failed tasks older than 24 hours (configurable).
Key Benefit: If the Flask server crashes or restarts, users can still see their completed tasks and download results.

Document 10: Basic Task Manager (Development)
Purpose: Simpler, in-memory version for development and testing.
How It Differs from Document 9:
FeatureEnhanced (Doc 9)Basic (Doc 10)PersistenceJSON filesIn-memory onlySurvives restartsYesNoStartup timeSlower (loads files)InstantDisk usage~1KB per taskNoneComplexityHigherLowerUse caseProductionDevelopment/Testing
What's the Same:

Task lifecycle (PENDING → RUNNING → COMPLETED)
Background threading
Progress tracking
Concurrency control
Auto-open logic
API interface (same functions)

When to Use Each:

Development: Use Document 10 - faster iteration, simpler debugging
Production: Use Document 9 - reliability, audit trails, persistence

Easy Switching:
Both have identical public APIs, so you can switch by changing one import:
python# Development
from task_manager import task_manager

# Production
from enhanced_task_manager import task_manager

Document 11: Example Review - AEXEW
Purpose: Concrete implementation showing how all the pieces come together.
AEXEW Index:

Type: Equal-weighted index
Methodology: Fixed market cap distributed equally among 30 components
Universe: AEX family (Dutch blue-chip stocks)

Step-by-Step Breakdown:

Initialization:

python   def run_aexew_review(date, co_date, effective_date, ...):
       try:
           year = year or str(datetime.strptime(date, '%Y%m%d').year)
           current_data_folder = os.path.join(DATA_FOLDER2, date[:6])
Sets up date handling and folder paths.

Data Loading:

python   index_eod_df, stock_eod_df, stock_co_df = load_eod_data(...)
   ref_data = load_reference_data(..., sheet_names={'aex_family': 'AEX'})
   selection_df = ref_data['aex_family']
Gets market data and AEX family composition.

Symbol Mapping:

python   symbols_filtered = stock_eod_df[
       stock_eod_df['#Symbol'].str.len() == 12
   ][['Isin Code', '#Symbol']].drop_duplicates(subset=['Isin Code'])
Why 12 characters? Reuters Instrument Codes (RICs) are exactly 12 characters. Shorter codes are different identifier types.

Data Enrichment (Chained Operations):

python   selection_df = (selection_df
       .rename(columns={...})           # Standardize column names
       .merge(symbols_filtered, ...)    # Add Reuters symbols
       .merge(FX data, ...)             # Add currency conversion rates
       .merge(EOD prices, ...)          # Add calculation date prices
       .merge(CO prices, ...)           # Add cut-off date prices
   )
Builds a comprehensive DataFrame with all needed information.

Equal-Weight Calculation:

python   index_mcap = index_eod_df.loc[...]['Mkt Cap'].iloc[0]
   selection_df['Unrounded NOSH'] = (index_mcap / 30) / selection_df['Close Prc_EOD']
   selection_df['Rounded NOSH'] = selection_df['Unrounded NOSH'].round()
The Math:

Total index market cap = €1,000,000,000 (example)
Number of components = 30
Target per component = €1B / 30 = €33,333,333
Shares needed = €33,333,333 / stock price
Round to nearest integer


No Capping:

python   selection_df['Capping Factor'] = 1
   selection_df['Free Float'] = 1
AEXEW is equal-weighted, so no individual stock limits needed.

Inclusion/Exclusion Analysis:

python   analysis_results = inclusion_exclusion_analysis(
       selection_df, stock_eod_df, index, isin_column='ISIN code'
   )
Compares new composition vs current composition to find changes.

Output Preparation:

python   AEXEW_df = selection_df[[
       'Company', 'ISIN code', 'MIC', 'Rounded NOSH', 
       'Free Float', 'Capping Factor', 'Effective Date of Review', 'Currency'
   ]].rename(columns={...})
Selects required columns and standardizes names.

Excel Generation:

python   with pd.ExcelWriter(aexew_path) as writer:
       AEXEW_df.to_excel(writer, sheet_name='AEXEW Composition', index=False)
       inclusion_df.to_excel(writer, sheet_name='Inclusion', index=False)
       exclusion_df.to_excel(writer, sheet_name='Exclusion', index=False)
       selection_df.to_excel(writer, sheet_name='Full Universe', index=False)
Creates multi-sheet Excel workbook with all information.

Standardized Return:

python    return {
        "status": "success",
        "message": "Review completed successfully",
        "data": {"aexew_path": aexew_path}
    }
```

**What Makes This a Good Example:**
- Uses all major utilities (data loading, analysis, output generation)
- Handles errors gracefully
- Follows naming conventions
- Produces standardized output
- Clear, readable code structure

---

## How the System Works

### Complete Flow: User Clicks "Calculate" to Excel File

**Step 1: User Interaction**
```
User fills form in browser:
- Review Type: FRI4P
- Calculation Date: 20250320
- Cut-off Date: 20250221
- Effective Date: 23-Jun-25
- Currency: EUR
- Auto-open: Checked

User clicks "Calculate Index"
Step 2: Frontend Processing
javascript// index.html JavaScript
1. Validate form inputs
2. Disable submit button (prevent double-click)
3. Show loading spinner
4. Build FormData object
5. POST to /calculate endpoint
6. Start polling /task-status every 2 seconds
Step 3: Flask Route Handling
python# app.py
@app.route('/calculate', methods=['POST'])
1. Extract form data
2. Validate date formats
3. Get index configuration from config.py
4. Check if request is local (for auto-open)
5. Create task via task_manager.create_task()
6. Start task in background via task_manager.start_single_review_task()
7. Return JSON: {"status": "started", "task_id": "123e4567..."}
Step 4: Task Manager Initialization
python# enhanced_task_manager.py
1. Generate UUID for task
2. Create TaskResult object (status = PENDING)
3. Save task to JSON file (persistence)
4. Add to in-memory tasks dictionary
5. Spawn background thread
6. Return task_id to Flask
Step 5: Background Thread Execution
python# enhanced_task_manager.py - in thread
1. Update task status → RUNNING
2. Set progress → 10%
3. Call review_logic.run_review()
Step 6: Review Routing
python# review_logic.py
1. Look up "FRI4P" in REVIEW_FUNCTIONS dictionary
2. Find: run_fri4p_review()
3. Call function with parameters
4. Wrap result in standard format
5. Add timing information
6. Return to task manager
Step 7: Specific Review Execution
python# fri4p_review.py
1. Set up logging and folders
2. Update progress → 20%
3. Call load_eod_data() to get market data
4. Update progress → 40%
5. Call load_reference_data() to get FF, ICB, etc.
6. Update progress → 60%
7. Apply FRI4P methodology (selection, weighting, capping)
8. Update progress → 80%
9. Calculate number of shares
10. Run inclusion_exclusion_analysis()
11. Generate Excel file with xlsxwriter
12. Update progress → 100%
13. Return success with file path
Step 8: Data Loading (happens in Step 7)
python# data_loader.py
load_eod_data():
1. Try loading: TTMIndexUS1_GIS_EOD_INDEX_20250320.csv
2. Try loading: TTMIndexEU1_GIS_EOD_INDEX_20250320.csv
3. Combine available data (US + EU)
4. Add currency information to stock data
5. Return three DataFrames

load_reference_data():
1. Load FF.xlsx → Free float data
2. Load ICB.xlsx → Industry classifications
3. Load Developed Market.xlsx → Universe definition
4. Return dictionary of DataFrames
Step 9: Task Completion
python# enhanced_task_manager.py
1. Receive result from review function
2. Update task status → COMPLETED
3. Save result data
4. Set progress → 100%
5. Save task to JSON (final state)
6. If auto-open and local: os.startfile(excel_path)
7. Clean up thread reference
Step 10: Frontend Updates
javascript// index.html - polling callback
1. Receive task status update
2. Update progress bar (100%)
3. Update message ("Completed successfully")
4. Display download link to Excel file
5. Stop polling (task complete)
6. Re-enable submit button
```

**Step 11: User Downloads**
```
User clicks link → Browser downloads Excel file
Excel contains:
- Sheet 1: "FRI4P Composition" (30 stocks with shares, FF, capping)
- Sheet 2: "Inclusion" (new additions to index)
- Sheet 3: "Exclusion" (stocks removed from index)
- Sheet 4: "Full Universe" (all candidates considered)
```

### Parallel Status Polling

While Steps 7-9 are executing, the frontend is continuously polling:
```
Every 2 seconds:
GET /task-status/123e4567...

Response:
{
    "status": "success",
    "task": {
        "status": "running",
        "progress": 65,
        "message": "Calculating weights..."
    }
}

Frontend updates progress bar and message in real-time.
```

---

## Data Sources and Formats

### EOD (End of Day) Files

**Location:** Network folder `\\pbgfshqa08601v\gis_ttm\Archive`

**File Naming Convention:**
```
TTMIndex{AREA}1_GIS_EOD_{TYPE}_{DATE}.csv

Examples:
- TTMIndexUS1_GIS_EOD_INDEX_20250320.csv
- TTMIndexEU1_GIS_EOD_STOCK_20250320.csv
Where:

{AREA} = US or EU (geographic market)
{TYPE} = INDEX or STOCK
{DATE} = YYYYMMDD

Index EOD File Structure:
csv#Symbol;Mnemo;Name;ISIN;Curr;Close Prc;Mkt Cap;...
FRIX00003643;FRI4P;FR Index 4 Price;FRIX00003643;EUR;1523.45;45000000000;...
Key Columns:

#Symbol: Unique identifier (ISIN for indices)
Mnemo: Short code (FRI4P, AEX, etc.)
Close Prc: Closing price in index currency
Mkt Cap: Total market capitalization
Curr: Currency code

Stock EOD File Structure:
csv#Symbol;Name;ISIN;MIC;Index;Curr;Close Prc;Mkt Cap;FX/Index Ccy;...
AIR.PA;Airbus SE;NL0000235190;XPAR;PX1;EUR;152.30;120000000000;1.0;...
```

Key Columns:
- `#Symbol`: Reuters Instrument Code (RIC) - 12 characters
- `Isin Code`: ISIN identifier
- `MIC`: Market Identifier Code (XPAR, XAMS, etc.)
- `Index`: Which index the stock belongs to
- `FX/Index Ccy`: Currency conversion rate to index currency
- `Turnover`: Trading volume in currency

**CSV Format Quirks:**
- Delimiter: Semicolon (`;`) not comma
- Decimal: Period (`.`) for numbers
- Encoding: Latin-1 (not UTF-8)
- Headers: First row

### Reference Data Files

**Location:** `V:\PM-Indices-IndexOperations\Review Files\{YYYYMM}\`

**Common Reference Files:**

1. **FF.xlsx (Free Float)**
```
   Structure:
   - ISIN Code: | Free Float | ...
   - US0378331005 | 0.98 | ...
```
   Purpose: Defines what percentage of shares are publicly tradeable

2. **ICB.xlsx (Industry Classification)**
```
   Headers in row 4:
   - ISIN | ICB Industry | ICB Supersector | ICB Sector | ICB Subsector
```
   Purpose: Categorizes companies by industry

3. **Developed Market.xlsx / Emerging Market.xlsx**
```
   Lists of ISINs qualifying as developed/emerging markets
```
   Purpose: Geographic eligibility screening

4. **CAC Family.xlsx / AEX Family.xlsx**
```
   Multiple sheets (one per index):
   - PX1 sheet: Current PX1 composition
   - CAC40 sheet: Current CAC40 composition
   
   Headers in row 2:
   - Company | ISIN code | MIC | Preliminary Number of shares | ...
```
   Purpose: Current index constituents for inclusion/exclusion analysis

5. **Oekom Trust&Carbon.xlsx**
```
   Headers in row 2:
   - ISIN | ESG Score | Carbon Rating | ...
```
   Purpose: ESG (Environmental, Social, Governance) screening

6. **NACE.xlsx**
```
   - ISIN | NACE Code | NACE Description
```
   Purpose: European industry classification (alternative to ICB)

**Common Excel Format Patterns:**
- Headers often in row 1, but sometimes row 2, 3, or 4
- Multiple sheets per file
- Some sheets have metadata rows before headers
- Data quality varies (missing values, inconsistent formats)

---

## Review Process Explained

### What Is an Index Review?

An index review is a quarterly process where we:
1. **Evaluate** which companies should be in an index
2. **Calculate** how much of each company (number of shares)
3. **Document** what changed (additions, removals)
4. **Generate** implementation files for trading desks

### Common Index Methodologies

**1. Market Cap Weighted (e.g., FRI4P)**
```
Weight of Stock A = Market Cap of A / Total Market Cap of All Stocks
```
- Larger companies have more influence
- Most common methodology
- May include capping to prevent single stock dominance

**2. Equal Weighted (e.g., AEXEW)**
```
Weight of each stock = 1 / Number of Stocks
```
- Every company has same influence
- Target market cap divided equally
- Rebalanced quarterly to maintain equality

**3. Free Float Weighted (e.g., many indices)**
```
FF Market Cap = Market Cap × Free Float Factor
Weight = FF Market Cap / Total FF Market Cap
```
- Only counts publicly tradeable shares
- Excludes strategic holdings, government ownership
- More realistic measure of liquidity

**4. ESG Screened (e.g., CACEW, AEXEW)**
```
Start with parent universe → Apply ESG filters → Calculate weights
```
- Excludes companies failing ESG criteria
- May use third-party ratings (Oekom, SESAMm)
- Excludes controversial sectors (tobacco, weapons, etc.)

### Capping Explained

**Problem:** In market-cap weighted indices, one or two large companies can dominate.

**Example:**
```
Without capping:
- Stock A: 35% weight
- Stock B: 25% weight
- Stocks C-Z: 40% combined

Index is effectively tracking two companies, not a broad market.
```

**Solution: Capping**
```
With 10% cap:
- Stock A: 10% (capped from 35%)
- Stock B: 10% (capped from 25%)
- Stocks C-Z: 80% (received redistributed weight)
How Redistribution Works:

Cap Stock A at 10%, excess = 25%
Cap Stock B at 10%, excess = 15%
Total excess = 40%
Redistribute 40% to other stocks proportionally based on their original weights
If any newly-weighted stock exceeds cap, repeat process

Code Implementation:
pythoncalculate_capped_weights(weights, cap_limit=0.10)
```
Uses iterative algorithm to converge on capped weights that sum to 100%.

### Inclusion/Exclusion Analysis

**Purpose:** Identify what changed between this review and the last one.

**Inclusions (Additions):**
```
Companies in new composition BUT NOT in current index
→ These will be bought (added to portfolios)
```

**Exclusions (Removals):**
```
Companies in current index BUT NOT in new composition
→ These will be sold (removed from portfolios)
Why This Matters:

Portfolio managers need to know what to trade
Market communication (announced before implementation)
Audit trail (why did composition change?)
Impact analysis (liquidity, market moving potential)

Implementation:
pythoninclusion_exclusion_analysis(new_composition, current_composition, index)

Returns:
- inclusion_df: DataFrame of additions
- exclusion_df: DataFrame of removals
Number of Shares Calculation
Question: How do we determine how many shares of each stock to include?
Method 1: From Target Weights
pythonTarget weight of Stock A = 2.5%
Index total market cap = €10,000,000,000
Target market cap for Stock A = €10B × 0.025 = €250,000,000
Stock A price = €50
Shares needed = €250,000,000 / €50 = 5,000,000 shares
Method 2: From Free Float Market Cap
pythonStock A free float market cap = €500,000,000
Stock A price = €50
Shares = €500,000,000 / €50 = 10,000,000 shares
Method 3: Equal Weight Distribution
pythonIndex target market cap = €1,000,000,000
Number of stocks = 30
Per-stock target = €1B / 30 = €33,333,333
Stock A price = €100
Shares = €33,333,333 / €100 = 333,333 shares
```

**Important:** Always round to integers (you can't buy fractional shares in most cases).

---

## Quality Control and Validation

### Why Validation Matters

Index calculations involve:
- Millions/billions of euros in assets
- Trading decisions by portfolio managers
- Regulatory compliance requirements
- Reputational risk if errors occur

**One wrong calculation can:**
- Cause incorrect trades worth millions
- Trigger regulatory investigations
- Damage client trust
- Require costly corrections

### Validation Approach

**1. Automated Comparison**
- Run both old (manual/Coen) and new (automated) processes
- Compare outputs field by field
- Flag any discrepancies for investigation

**2. Multi-Level Checking**
```
Field Level: Does each value match?
↓
ISIN Level: Are the same companies included?
↓
Index Level: Does the overall composition make sense?
3. Tolerance-Based Matching
python# Exact match for non-numeric fields
MIC: "XPAR" == "XPAR" ✓

# Float tolerance for numeric fields (rounding differences)
Shares: 10,000,000.0 vs 10,000,000.00000001 ✓ (within 1e-8)
Shares: 10,000,000 vs 10,000,001 ✗ (difference too large)
4. Currency Equivalence
pythonGBP == GBX ✓  (different codes for pound sterling)
EUR == USD ✗  (actually different currencies)
Using the Comparison Tools
Typical Workflow:
bash# Step 1: Organize files
Coen/
├── AEXEW_EDWP_20250320.xlsx
├── FRI4P_EDWP_20250320.xlsx
└── ...

Dataiku/
├── AEXEW_Review_20250320_103045.xlsx
├── FRI4P_Review_20250320_103112.xlsx
└── ...

# Step 2: Edit comparison script paths
coen_folder = r"C:\...\Coen"
dataiku_folder = r"C:\...\Dataiku"

# Step 3: Run comparison
python compare_reviews.py

# Step 4: Review output
Review Comparison/
└── index_comparison_results_20250320_120000.xlsx
```

**Interpreting Results:**

**Summary Sheet:**
```
Mnemo | ISIN Count Match | ISIN Set Match | MIC Match | ... | Status
FRI4P | YES             | YES            | YES       | ... | OK
AEXEW | YES             | NO             | YES       | ... | ISSUE
GICP  | NO              | NO             | NO        | ... | ISSUE
```

**If Status = ISSUE:**
1. Check Field Mismatches sheet for specific differences
2. Check ISIN Differences sheet for missing/extra companies
3. Investigate root cause

**Common Causes of Mismatches:**

1. **Rounding Differences**
```
   Coen: 10,000,000
   Dataiku: 9,999,999.999999
   → Acceptable (within tolerance)
```

2. **Different Data Sources**
```
   Coen used stale data file
   Dataiku used updated data file
   → Investigate which is correct
```

3. **Methodology Difference**
```
   Coen capped at 9.9%
   Dataiku capped at 10.0%
   → Review specification, align implementations
```

4. **Currency Conversion**
```
   Different FX rates used
   → Check which date's FX rate should apply
```

5. **Inclusion Criteria**
```
   Different interpretations of eligibility rules
   → Clarify methodology, update scripts
When to Investigate
Acceptable Differences:

✓ Float precision beyond 10 decimal places
✓ Currency code variations (GBP/GBX, ILA/ILS)
✓ Trailing spaces in company names
✓ Minor formatting differences (Excel cell formats)

Unacceptable Differences:

✗ Different ISINs included
✗ Shares differing by >1
✗ Wrong MIC codes
✗ Capping factors outside tolerance
✗ Different number of constituents

Validation Best Practices

Always compare against reference:

First implementation → compare to manual process
After updates → compare to previous automated output
After bug fixes → compare to known-good baseline


Document expected differences:

If methodology changed, document why outputs differ
If data sources changed, explain impact
Keep validation reports for audit trail


Test edge cases:

What if a company has no free float data? (Should default to 1.0)
What if prices are zero? (Should error, not divide by zero)
What if files are missing? (Should handle gracefully)


Validate business logic:

python   # Sanity checks in your review script
   assert (composition_df['Number of Shares'] > 0).all(), "No zero shares"
   assert len(composition_df) == expected_count, f"Expected {expected_count} stocks"
   assert composition_df['ISIN Code'].duplicated().sum() == 0, "No duplicate ISINs"
```

---

## System Deployment and Maintenance

### File Organization
```
Project Root/
├── app.py                          # Flask application (Document 1)
├── config.py                       # Configuration (Document 8)
├── enhanced_task_manager.py        # Task manager (Document 9)
├── task_manager.py                 # Backup task manager (Document 10)
│
├── Review/
│   ├── review_logic.py             # Router (Document 2)
│   └── reviews/
│       ├── aexew_review.py         # Example review (Document 11)
│       ├── fri4p_review.py
│       ├── frd4p_review.py
│       └── ... (60+ review scripts)
│
├── utils/
│   ├── data_loader.py              # Data loading (Document 4)
│   ├── logging_utils.py
│   ├── capping_utils.py            # Capping calculations
│   └── inclusion_exclusion.py      # Change analysis
│
├── batch_processor.py              # Batch execution (Document 5)
│
├── templates/
│   └── index.html                  # Frontend (Document 3)
│
├── static/
│   └── images/
│       └── Euronext_Logo-RGB_black.png
│
├── output/                         # Generated Excel files
│   ├── FRI4P_Review_20250320_103045.xlsx
│   └── ...
│
├── task_storage/                   # Persistent task data
│   ├── 123e4567-e89b-12d3-a456-426614174000.json
│   └── ...
│
├── validation/                     # Comparison scripts
│   ├── compare_reviews.py          # General comparison (Document 7)
│   └── compare_eia_reviews.py      # EIA comparison (Document 6)
│
└── tests/                          # Test scripts
    └── test_aexew_review.py
Running the System
Development Mode:
bashpython app.py

Debug mode enabled
Auto-reload on code changes
Listens on localhost:5000
Uses task_manager.py (no persistence)

Production Mode:
bash# Use production WSGI server (not Flask development server)
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```
- Multiple worker processes
- Better performance
- Production-grade
- Uses enhanced_task_manager.py (persistence)

**Accessing the Interface:**
```
Browser: http://localhost:5000
or
Browser: http://<server-ip>:5000
Common Maintenance Tasks
1. Adding a New Index
python# Step 1: config.py
INDEX_CONFIGS["NEWIDX"] = {
    "index": "NEWIDX",
    "isin": "XX0000000000",
    "output_key": "newidx_path"
}

# Step 2: Create Review/reviews/newidx_review.py
def run_newidx_review(...):
    # Implementation

# Step 3: review_logic.py
from .reviews.newidx_review import run_newidx_review
REVIEW_FUNCTIONS["NEWIDX"] = run_newidx_review

# Done! Index appears in UI automatically
2. Updating Data Folder Paths
python# config.py - change these if network drives move
DLF_FOLDER = r"\\new-server\new-path\Archive"
DATA_FOLDER = r"V:\New-PM-Path\Review Files"
3. Adjusting Concurrency
python# config.py
BATCH_CONFIG = {
    "max_concurrent_reviews": 5,  # Increase if server can handle it
}

# enhanced_task_manager.py
self.max_concurrent_tasks = 5  # Match the config value
4. Clearing Old Tasks
bash# Via API
curl -X POST http://localhost:5000/cleanup-tasks

# Or manually delete JSON files
rm task_storage/*.json
5. Viewing Logs
bash# Console output shows real-time progress
[INFO] Starting FRI4P review for date 20250320
[INFO] Loading EOD data...
[INFO] Loaded index data from 2 file(s)
[INFO] Loading reference data...
```

### Troubleshooting

**Problem:** Review fails with "File not found"
```
Solution:
1. Check network connectivity to data sources
2. Verify date format (YYYYMMDD)
3. Check if data files exist for that date
4. Review data_loader.py logs for specific missing file
```

**Problem:** Progress stuck at 0%
```
Solution:
1. Check if max_concurrent_tasks limit reached
2. Look for exception in console output
3. Check task status via /task-status/<task_id>
4. Restart Flask server to clear stuck tasks
```

**Problem:** Results don't match reference
```
Solution:
1. Run comparison script (Documents 6 or 7)
2. Review Field Mismatches sheet
3. Check if using same data files
4. Verify methodology interpretation
5. Investigate any recent code changes
```

**Problem:** Server restart loses tasks
```
Solution:
1. Ensure using enhanced_task_manager.py (Document 9)
2. Check task_storage/ folder permissions
3. Verify JSON files are being created
4. Check disk space

Glossary
ISIN (International Securities Identification Number)

Unique 12-character identifier for securities
Format: 2-letter country code + 9-digit code + 1 check digit
Example: US0378331005 (Apple Inc.)

MIC (Market Identifier Code)

4-character code identifying trading venue
Examples: XPAR (Euronext Paris), XAMS (Euronext Amsterdam), XNAS (NASDAQ)

RIC (Reuters Instrument Code)

12-character identifier used in Reuters data feeds
Format: Ticker + dot + exchange suffix
Example: AIR.PA (Airbus on Paris exchange)

EOD (End of Day)

Market data as of market close
Includes closing prices, volumes, market caps
Used for index calculations

Free Float

Percentage of shares available for public trading
Excludes strategic holdings, government ownership, locked shares
Range: 0.0 to 1.0 (e.g., 0.95 = 95% freely tradeable)

Capping Factor

Adjustment factor to limit individual stock weights
Result of capping algorithm
Range: 0.0 to 1.0 (e.g., 0.5 = weight reduced by half)

Market Capitalization (Market Cap)

Total value of company's outstanding shares
Formula: Share price × Number of shares
Example: €100/share × 1M shares = €100M market cap

Effective Date

Date when new index composition takes effect
Format: DD-MMM-YY (e.g., "23-Jun-25")
Usually a few weeks after calculation date

Cut-off Date

Historical reference date for eligibility criteria
Used to prevent short-term market manipulation
Typically 4-6 weeks before calculation date

Inclusion/Exclusion

Inclusions: Stocks added to index
Exclusions: Stocks removed from index
Reported in quarterly review documentation


Summary
This system transforms a complex, manual, error-prone process into an automated, reliable, auditable workflow. By understanding how each component works and interacts, you can:

Create new review scripts following established patterns
Debug issues when they arise
Validate outputs against reference implementations
Maintain and enhance the system over time

The key principle: Consistency through standardization. Every review follows the same structure, uses the same utilities, produces the same output format, and integrates through the same interfaces. This makes the system maintainable, testable, and scalable.