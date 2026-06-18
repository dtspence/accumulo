# SimpleLoadBalancer Migration Halving — Analysis

## Summary

When an under-loaded tablet server has to be filled from **several** donor servers, the
balancer moves only **half** of the remaining imbalance per balance round, so a cluster
converges geometrically (e.g. `100 → 50 → 25 → …`, or `48 → 24 → 12 → 6 → …`) instead of
in a single pass.

The root cause is a one-line defect in `SimpleLoadBalancer.getMigrations()`: the recipient's
remaining deficit is computed as

```java
int needToLoad = goal - tooLittle.count - movedAlready;
```

Since 2014, `move()` already keeps `tooLittle.count` updated live as it proposes migrations,
so the tablets loaded into the recipient during the current pass are subtracted **twice** —
once via the live `tooLittle.count`, and again via `movedAlready`. The loop therefore believes
the recipient is "full" after it has received only **half** of its real deficit.

This is a latent regression: the code was correct from 2011–2014, and a 2014 change
(`ACCUMULO-2952`) broke it without updating this line.

---

## Symptom

Each balance round only closes half of the outstanding imbalance. Because the manager will
not compute a fresh set of migrations until the current ones have finished moving (see
*Gating* below), this manifests as a slow, multi-round crawl toward balance rather than a
single corrective pass.

The contrast the user observed:

```
round 1: 100 still out of place
round 2:  50
round 3:  25
...
```

vs. the expected "move everything that's needed in one pass".

---

## Where migrations are actually computed (delegation)

`HostRegexTableLoadBalancer` does **not** compute migrations itself. It groups tablet servers
into pools by regex, then for each table delegates to a per-table balancer:

- `HostRegexTableLoadBalancer.balance()` → for each table calls
  `getBalancerForTable(tableId).balance(...)`.
- `TableLoadBalancer.getBalancerForTable()` returns the configured per-table balancer, which
  defaults to **`SimpleLoadBalancer`**.
- `SimpleLoadBalancer.balance()` → `SimpleLoadBalancer.getMigrations()` is where the actual
  migration counts are produced.

So the halving originates in `SimpleLoadBalancer.getMigrations()`. `HostRegexTableLoadBalancer`
only surfaces it (by grouping servers and by gating/capping migrations).

**Files**
- `core/src/main/java/org/apache/accumulo/core/spi/balancer/HostRegexTableLoadBalancer.java`
- `core/src/main/java/org/apache/accumulo/core/spi/balancer/TableLoadBalancer.java`
- `core/src/main/java/org/apache/accumulo/core/spi/balancer/SimpleLoadBalancer.java`

---

## Why convergence is multi-round (gating)

Two guards ensure the balancer plans exactly **one pass** of migrations and then waits for the
data to actually move before planning again:

- `HostRegexTableLoadBalancer.balance()` returns early (without balancing) when migrations are
  outstanding and `migrations.size() >= maxOutstandingMigrations`. The default
  `maxOutstandingMigrations` is **0**, so *any* in-flight migration blocks the next round.
- `SimpleLoadBalancer.balance()` likewise skips `getMigrations()` when
  `params.currentMigrations()` is non-empty, and calls `getMigrations()` exactly once per
  invocation (returning a 1-second retry hint when more balancing is needed).

The single pass cannot fully converge on its own because the next pass's tablet selection
depends on tablets having actually moved (`move()` selects real online tablets from the donor;
an in-flight tablet can't be re-nominated). So:

> one pass per round → execute the migrations → balance again.

The **amount moved per round** is therefore whatever the single-pass algorithm produces — and
that is where the halving lives.

---

## Root cause: the double-count

`SimpleLoadBalancer.getMigrations()` uses a two-pointer sweep over servers sorted high→low by
tablet count. For each over-loaded server (`tooMany`, from the front) it unloads to the most
under-loaded server (`tooLittle`, from the back):

```java
int end = totals.size() - 1;
int movedAlready = 0;
int tooManyIndex = 0;
while (tooManyIndex < end) {
  ServerCounts tooMany = totals.get(tooManyIndex);
  int goal = even;
  if (tooManyIndex < numServersOverEven) {
    goal++;
  }
  int needToUnload = tooMany.count - goal;
  ServerCounts tooLittle = totals.get(end);
  int needToLoad = goal - tooLittle.count - movedAlready;   // <-- defect
  ...
  if (needToUnload >= needToLoad) {
    result.addAll(move(tooMany, tooLittle, needToLoad, donerTabletStats));
    end--;
    movedAlready = 0;
  } else {
    result.addAll(move(tooMany, tooLittle, needToUnload, donerTabletStats));
    movedAlready += needToUnload;
  }
  ...
}
```

And `move()` mutates the live counts for every tablet it proposes to move:

```java
tooMuch.count--;
tooLittle.count++;
```

When a single under-loaded server must be filled from multiple donors (the
`needToUnload < needToLoad` branch), `end` stays fixed while `tooManyIndex` advances through
successive donors. Each donation increments **both** `tooLittle.count` (via `move()`) **and**
`movedAlready`. The next iteration then computes
`needToLoad = goal - tooLittle.count - movedAlready`, subtracting the same donated tablets
twice.

`tooLittle.count` alone is already the correct remaining deficit (`move()` keeps it current).
The `- movedAlready` term is redundant and wrong.

---

## Why exactly half

The loop stops loading a recipient (drops it via `end--`) once it believes the recipient has
reached `goal`, i.e. once `needToLoad <= 0`:

```
goal - tooLittle.count - movedAlready <= 0
```

While filling a recipient from donors, all the tablets it has received this pass equal
`movedAlready`, so:

```
tooLittle.count = start + movedAlready
```

Substituting:

```
goal - (start + movedAlready) - movedAlready <= 0
=> goal - start - 2 * movedAlready <= 0
=> movedAlready >= (goal - start) / 2
=> received >= deficit / 2
```

So the recipient is declared full after receiving exactly **half** of its deficit. It is then
dropped and must wait for the next balance round, which again fills half of the remainder —
producing the geometric `deficit, deficit/2, deficit/4, …` sequence.

---

## When it does (and does not) bite

- **Bites** when a recipient's deficit exceeds any single donor's surplus, so it must be filled
  from several donors (the `needToUnload < needToLoad` branch). This is the common
  HostRegex situation: a server joins an under-filled pool, or a pool is lightly loaded across
  many slightly-heavier servers.
- **Does not bite** when one hugely over-loaded server feeds many under-loaded servers (the
  `needToUnload >= needToLoad` branch). There `movedAlready` is reset to `0` after each
  `end--`, so no double-count occurs and the pass balances in one shot.

This asymmetry — donor side relies solely on the live count, recipient side carries both the
live count and the stale accumulator — is itself a strong signal that the second subtraction
is an oversight rather than intentional throttling.

---

## History: a 2014 regression (not an original design choice)

`git blame` shows the two interacting pieces were introduced **three years apart**:

| Year | Commit | Author | What |
|------|--------|--------|------|
| 2011-10-20 | `c31bf3aa20` | John Vines | Introduced the two-pointer loop, `movedAlready`, and `needToLoad = goal - tooLittle.count - movedAlready`. At this time `ServerCounts.count` was **`final`**. |
| 2014-06-30 | `11d11e0da9` | Eric C. Newton | `ACCUMULO-2952` "keep better info on what tablet counts will be after balancing": made `ServerCounts.count` **mutable** and added `tooMuch.count--; tooLittle.count++;` in `move()`. Left the `needToLoad` line unchanged. |
| 2021-02-04 | `95a8df1804` | Brian Loss | Ported `DefaultLoadBalancer` to the stable SPI as `SimpleLoadBalancer`, carrying the code (and the defect) verbatim. |

### Why it was correct before 2014

Before 2014, `ServerCounts.count` was `final` — a **start-of-pass snapshot** that `move()`
could not (and did not) change. The two terms were complementary, not redundant:

- `tooLittle.count` = where the recipient *started* this pass (immutable snapshot).
- `movedAlready` = what has been *added* to it so far this pass.

`goal - tooLittle.count - movedAlready` was therefore the true remaining deficit. **Correct.**

### What 2014 changed

`ACCUMULO-2952`'s intent was to track *projected* post-balance counts so decisions stay
accurate as migrations are proposed — primarily for the **donor** side: a single over-loaded
server feeding several recipients needs its `needToUnload = tooMany.count - goal` to shrink as
it gives tablets away, which a `final` count could not do. Making `count` mutable fixed that.

But the same change made `tooLittle.count` a **live** running count
(`start + movedAlready`), while the recipient-side formula still subtracted `movedAlready`
separately. From 2014 onward:

```
goal - (start + movedAlready) - movedAlready  =  goal - start - 2 * movedAlready
```

— the in-pass loads are double-counted. The donor side was "modernized" to live counts; the
recipient side kept both the live count and the now-redundant accumulator. The complete 2014
change should also have dropped the `- movedAlready` term.

---

## Empirical evidence

A faithful replica of the `getMigrations()` two-pointer arithmetic, run round-by-round
(applying each pass's migrations before the next), reproduces the halving and isolates the
cause by toggling only the `movedAlready` subtraction:

**Scenario A — 25 servers @ 60 tablets + 1 server @ 10 (`even = 58`, deficit 48):**

```
bug ON  (current code):  moved per round = 24, 12, 6, 3, 2, 1   (6 rounds; sum 48)
bug OFF (fix):           moved per round = 48                   (1 round)
```

The bug-ON total (24+12+6+3+2+1 = 48) matches the existing
`SimpleLoadBalancerTest.testUnevenAssignment2`, which asserts exactly **48** tablets moved —
confirming the replica matches the real balancer.

**Scenario B — 16 servers @ 21 tablets + 1 server @ 4 (`even = 20`, deficit 16):**

```
bug ON:   moved per round = 8, 4, 2, 1, 1   (5 rounds)
bug OFF:  moved per round = 16              (1 round)
```

**Scenario C — 1 server @ 100 + 4 servers @ 0 (one heavy donor, many recipients):**

```
bug ON:   balances in ONE round (movedAlready resets each end--; no double-count)
```

Scenarios A and B are the multi-donor → one-recipient case (halving). Scenario C is the
one-donor → many-recipients case (unaffected), matching the analysis above.

---

## The fix

Rely solely on the live count and remove the redundant accumulator:

```java
int needToLoad = goal - tooLittle.count;
```

and delete the now-dead `movedAlready` variable and its updates
(`int movedAlready = 0;`, `movedAlready = 0;`, `movedAlready += needToUnload;`).

Total tablets moved is unchanged (a recipient with deficit `D` still receives `D` overall), so
invariants like `testUnevenAssignment2`'s "48 moved" still hold — the difference is that the
recipient is filled in a single pass instead of across `~log2(D)` rounds.

---

## Scope / affected code

- `core/src/main/java/org/apache/accumulo/core/spi/balancer/SimpleLoadBalancer.java` — the
  active SPI balancer that `HostRegexTableLoadBalancer` (and the default per-table balancer)
  delegate to. **Primary fix site.**
- `server/base/src/main/java/org/apache/accumulo/server/master/balancer/DefaultLoadBalancer.java`
  — the deprecated predecessor carries the identical code and the identical defect; out of
  scope unless that class is still in use.
