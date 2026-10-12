# DECISIONS.md

1. **Indexes.** Unique `emp_code` on employees (duplicate protection; serves lookups and `$lookup` joins) and unique `(emp_code, date)` on logs (one record per employee-day, making punch-in race-safe; serves monthly, PATCH, trend). Supporting: `(department, emp_code)` for filtered lists, `(department, joined_on)` and `(joined_on)` for headcount ranges, `(date desc, emp_code)` for the global list and leaderboard ranges, `(status, date desc, emp_code)` for status-filtered lists, `(emp_code, punch_in desc)` for punch-out. I rejected `{late_minutes: 1}`: the leaderboard always constrains a month of dates, so the date index already narrows the scan.

2. **Punch-in race.** Both compute the same date and call `insert_one`. The unique `(emp_code, date)` index lets exactly one succeed; the other raises `DuplicateKeyError`, which I catch and turn into 409. No check-then-insert.

3. **Ties.** Tied employees are all returned, so rows can exceed `limit`: I rank with `$rank` (1, 2, 2, 4), then keep `rank <= limit`; ties order by `emp_code` ascending.

4. **Headcount.** The summary starts from `employees` with `joined_on <= month end`, then `$lookup`s logs, so zero-log employees count 1 while later joiners are excluded; headcount-0 departments never appear.

5. **100x data.** I would keep a monthly per-employee rollup collection updated at write time, so analytics read hundreds of summary documents instead of scanning millions of logs.
