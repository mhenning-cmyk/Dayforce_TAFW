# How This Project Works

*A plain-language explanation of what this system does and why. No coding
background required. For setup/development instructions, see [README.md](README.md).*

## The problem this solves

Employees request time off (vacation, sick days, personal days, etc.) in
**Dayforce**, which is Bluedrop's HR and timekeeping system. Separately,
**NetSuite** tracks how many hours get billed against which project — but
NetSuite has no idea when someone is on vacation unless something tells it.

This project is that "something." It's an automated service that:

1. Checks Dayforce for employees' approved and canceled time-off requests.
2. Figures out which NetSuite project and task each employee's time should
   count against.
3. Saves all of that in a shared table that another tool (**Celigo**) reads
   from to actually update NetSuite.

Nobody has to manually re-type a vacation request into NetSuite. It happens
automatically, on a schedule.

## The data's journey

```
   Dayforce                This project              A results table          Celigo → NetSuite
 (time-off requests   ──▶  (runs on a schedule,  ──▶  (stores the      ──▶    (reads that table,
  live here)               does the steps below)      current answer)         updates NetSuite)
```

Dayforce is the source of truth for "who requested what time off." This
project doesn't change anything in Dayforce — it only reads from it, figures
out what NetSuite needs to know, and writes that somewhere Celigo can pick it
up.

## What happens each time it runs

**Step 1 — Get the list of employees.**
Pull the current employee roster from Dayforce.

**Step 2 — Figure out where each employee's time gets billed.**
For each employee, look up their name and department. Then check a small
reference list that says which NetSuite "project" and "task" number that
department bills against.

Only a handful of departments (the "BTSI" group) currently have a project/task
set up in NetSuite. If an employee's department isn't on that list, there's
nowhere in NetSuite for their time to go — so that employee is simply left out
of the results, rather than uploading a name with no project attached.

**Step 3 — Save that employee → project/task list.**
This becomes a small reference table so NetSuite/Celigo know, for every
in-scope employee, which project and task their hours belong to.

**Step 4 — Get everyone's time-off requests.**
For each in-scope employee, pull their approved and canceled time-off
requests from Dayforce, covering roughly one month in the past through three
months in the future. A multi-day request (say, a Monday-through-Wednesday
vacation) is broken into one entry per weekday — weekends are skipped, since
nobody is scheduled to work then anyway.

**Step 5 — Save the time-off results.**
This becomes the table Celigo reads to actually create, update, or cancel the
matching entries in NetSuite.

## How changes get handled

Time-off requests aren't static — people cancel plans, correct mistakes, or
have a request approved and then later reversed. Every time this runs, each
day of time off gets checked against what came before:

- **Brand new** → added as a new entry.
- **Unchanged since last time** → left alone.
- **Something about it changed** (the number of hours, the type of leave,
  etc.) → the old entry is marked **Canceled** and a corrected new entry is
  added. Nothing gets silently overwritten — there's always a record of what
  it used to say before the correction.
- **No longer approved** (someone canceled the request in Dayforce) → shows
  up here as **Canceled** too.

This means the results table always reflects the current, accurate picture,
while still keeping a trail of what changed and when.

## Things worth knowing

- **Only a few departments are wired up.** Time off for employees outside the
  BTSI department group won't appear in the results, because there's no
  NetSuite project/task to attach it to yet. Expanding this just means adding
  a line to the small department reference list — it doesn't require new code.
- **The Dayforce connection used here is deliberately limited.** It can see
  employee names, departments, and time-off requests, but not broader HR or
  payroll data. That's an intentional security/access decision, not a
  technical limitation of Dayforce itself.
- **Hours are split evenly across days when a request spans more than one
  day.** Dayforce tells us the total hours for a request (e.g., "24 hours off
  Monday through Wednesday") but not a day-by-day breakdown, so this system
  divides the total evenly across the weekdays involved.
- **This runs on a schedule inside Databricks**, a data platform used to host
  and automate this kind of process. Someone with access to that environment
  can see exactly when it last ran and what it did.
