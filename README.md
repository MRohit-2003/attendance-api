# Employee Attendance & Analytics API

FastAPI + MongoDB (PyMongo) implementation of the HROne attendance assignment.
All application code lives in `app/main.py`.

## Project structure

```
attendance-api/
├── app/
│   ├── __init__.py          # empty (makes `app` a package)
│   └── main.py              # ALL application code (single file, sectioned)
├── requirements.txt         # direct dependencies
├── README.md                # this file
├── REVIEW.md                # Part A: defect review of the starter code
└── DECISIONS.md             # Part C: design decisions
```

## Prerequisites

- **Python 3.11+** (tested on 3.12.3)
- **MongoDB 6.0+** running and reachable (tested on 7.0.14; `$densify` and
  `$setWindowFields` are used, so 6.0+ is required)
- `pip` (a virtual environment is recommended)

## Packages & frameworks (exact versions tested)

| Package | Version | Notes |
|---|---|---|
| fastapi | 0.143.0 | web framework |
| uvicorn (`[standard]`) | 0.54.0 | ASGI server (standard extras: httptools, uvloop, watchfiles, websockets) |
| pymongo (`[srv]`) | 4.18.3 | MongoDB driver (thread-safe; used with plain `def` endpoints) |
| pydantic | 2.14.0 | request validation (`StrictInt`, `Literal`, regex/model validators) |
| python-dotenv | 1.2.4 | optional local `.env` loading |
| starlette | 1.7.0 | pulled in by FastAPI |
| dnspython | 2.9.0 | pulled in by `pymongo[srv]` (only needed for `mongodb+srv://` URIs) |
| MongoDB server | 7.0.14 | database (any 6.0+ works) |

`requirements.txt` lists the direct dependencies unpinned (`fastapi`,
`uvicorn[standard]`, `pymongo[srv]`, `pydantic`, `python-dotenv`); the table
above pins the exact versions this was built and verified against.

## Setup on Windows (step by step)

The commands below use **PowerShell**. Run them from the repository root:
`attendance-api` (the directory containing `requirements.txt`).

### 1. Install prerequisites

Install the following before continuing:

- **Python 3.11 or newer** from [python.org](https://www.python.org/downloads/windows/).
  During installation, select **Add python.exe to PATH**.
- **MongoDB Community Server 6.0 or newer** from
  [mongodb.com](https://www.mongodb.com/try/download/community). The Windows
  installer can register MongoDB as a Windows service.

Open a new PowerShell window after installing Python, then confirm that both
commands are available:

```powershell
py --version
mongod --version
```

If MongoDB was installed as a Windows service, start it from an elevated
PowerShell window:

```powershell
Start-Service MongoDB
Get-Service MongoDB
```

If it was not installed as a service, start `mongod.exe` manually using the
data directory configured during installation. For example:

```powershell
New-Item -ItemType Directory -Force C:\data\db
mongod.exe --dbpath C:\data\db --port 27017
```

Leave that window running while the API is running. As an alternative, if
Docker Desktop is installed and running:

```powershell
docker run -d --name attendance-mongodb -p 27017:27017 mongo:7.0
```

### 2. Create and activate a virtual environment

From the repository root:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks the activation script, allow scripts for the current
user and activate again:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
.\.venv\Scripts\Activate.ps1
```

The prompt should now show `(.venv)`.

### 3. Install the Python dependencies

Upgrade `pip`, then install the dependencies listed by the project:

```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### 4. Configure MongoDB

The default local configuration works without any additional settings:

- MongoDB URI: `mongodb://localhost:27017`
- Database name: `attendance_db`

To use different values for the current PowerShell session:

```powershell
$env:MONGO_URI = "mongodb://localhost:27017"
$env:MONGO_DB = "attendance_db"
```

For repeatable local configuration, create a `.env` file in the repository
root:

```text
MONGO_URI=mongodb://localhost:27017
MONGO_DB=attendance_db
```

Do not commit credentials or private connection strings. Real environment
variables take precedence over `.env` values (`load_dotenv(override=False)`).

### 5. Start the API

With the virtual environment activated, run this from the repository root:

```powershell
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Keep this PowerShell window running. To stop the server, press `Ctrl+C`.

### 6. Verify the API

Open <http://127.0.0.1:8000/docs> in a browser for the interactive API
documentation. To check the health endpoint from PowerShell, use
`curl.exe` explicitly because `curl` may be a PowerShell alias:

```powershell
curl.exe http://127.0.0.1:8000/health
```

Expected response:

```json
{"status":"ok"}
```

`GET /health` returns 503 when MongoDB cannot be reached. All indexes are
created automatically at startup. If port 8000 is already in use, start the
server on another port, for example:

```powershell
python -m uvicorn app.main:app --host 127.0.0.1 --port 8001
```

### 7. Run a quick smoke test

The API accepts `punched_at` as an integer epoch timestamp in milliseconds
(IST = UTC+5:30). Run these commands in a second PowerShell window while the
server is running:

```powershell
$employee = '{"emp_code":"EMP0001","name":"Asha Rao","email":"asha@example.com","department":"Engineering","joined_on":"2026-01-05"}'
curl.exe -X POST http://127.0.0.1:8000/employees `
  -H "Content-Type: application/json" `
  -d $employee

$punchIn = '{"emp_code":"EMP0001","punched_at":1783311001000}'
curl.exe -X POST http://127.0.0.1:8000/attendance/punch-in `
  -H "Content-Type: application/json" `
  -d $punchIn
```

The first request creates an employee. The second records a punch-in; for the
example timestamp, the calculated `late_minutes` should be 10.

## Assumptions (decided per the brief, kept consistent)

- **Punch-out check order:** 404 unknown employee → 404 no punch-in record →
  409 already punched out → 422 time validation → atomic close (409 if the race
  is lost).
- **`present_days`** does not ignore records dated before `joined_on`; only
  `working_days` starts at `joined_on` (R7).
- **Trend range:** inclusive day count over 92 days → 422.
- **Trend `moving_avg_7d`** is computed over the *reported* (4-decimal rounded)
  attendance rates, using exact integer arithmetic (half-up, never `$round`).
- **PATCH** with final status ABSENT/LEAVE plus supplied punch times → 422;
  a PATCH that changes nothing → 422.
- **Explain** for `employee_monthly` does not require the employee to exist.
- Weekend records contribute to late/overtime totals but never to
  `present_days`/`working_days`; `attendance_rate` is null on non-working days
  and when headcount is 0.

## Notes

- `_id` never appears in any request or response; employees are keyed by
  `emp_code`, records by (`emp_code`, `date`).
- Analytics read the stored derived values; they never recompute them from
  punch times. Legacy documents (missing `history`/`half_day`/`late_minutes`/
  `overtime_minutes`/`work_hours`) are handled with defaults in the serializer
  and with `$ifNull` in every pipeline.
- `GET /admin/explain/{endpoint}` builds its query/pipeline with the same
  builder functions as the real endpoint, so the explained plan is the real
  plan. Every explained plan uses `IXSCAN` with no `COLLSCAN` (hints are used
  where the planner could otherwise choose a collection scan).
- Concurrency: plain `def` endpoints (PyMongo is thread-safe; FastAPI runs
  them in a threadpool). Punch-in/punch-out rely on unique indexes +
  atomic operations, not check-then-act.

## Troubleshooting

- **`/health` returns 503** — MongoDB is unreachable; check `MONGO_URI` and
  that `mongod` is running. The server fails fast (5s selection timeout),
  it does not hang.
- **`pymongo[srv]` install warnings about the `srv` extra** — harmless for
  plain `mongodb://` URIs; the extra only matters for `mongodb+srv://`.
- **Port already in use** — run with `--port 8001` (or kill the old process).
- **422 on `punched_at`** — the API takes integer epoch *milliseconds*
  (e.g. `1783311001000`); seconds, floats, strings and booleans are rejected.
