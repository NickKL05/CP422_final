# CAMEO lookup tables (Member 3)

Small reference tables used by `notebooks/03_dyad_tone_index` and `notebooks/04_actor_network`. They are
broadcast-joined to the GDELT events, so they live in the repo rather than in a table in one member's workspace.
That way the notebooks run the same way in every member's Databricks account, including Member 1's final Run All.

## Files

| File | Source | Rows | Used for |
| --- | --- | ---: | --- |
| `CAMEO.country.txt` | GDELT official lookup | 261 codes | Deciding which `Actor1CountryCode` / `Actor2CountryCode` values are real countries, and labelling them |
| `CAMEO.eventcodes.txt` | GDELT official lookup | 310 codes | Labelling `EventCode` / `EventRootCode` in readable output (e.g. the top conflict event types per dyad) |
| `cameo_code_remap.txt` | Written by Member 3 (cleaning rule shared with Member 2) | 3 codes | Sub-state codes rewritten to their state: `WSB` and `GZS` to `PSE`, `BAG` to `IRQ` |
| `cameo_nonstate_codes.txt` | Written by Member 3 | 25 codes | Region codes in `CAMEO.country.txt` that are **not** states, so they are excluded from dyads |

After the remap and the exclusions, **233 codes** count as states.

Source URLs (fetched 2026-11-26; the server reports both files last modified 2020-09-09):

- https://www.gdeltproject.org/data/lookups/CAMEO.country.txt
- https://www.gdeltproject.org/data/lookups/CAMEO.eventcodes.txt

The two GDELT files are committed **byte-for-byte as published** (tab-separated, header row, CRLF line endings),
so anyone can re-download them and compare checksums:

| File | MD5 |
| --- | --- |
| `CAMEO.country.txt` | `fb3c0f3537e4e7725a7f38a869085fee` |
| `CAMEO.eventcodes.txt` | `eba96a03968c8b9831399176b94de8a4` |

The code that loads them strips the trailing `\r`. Two cosmetic errors come from the source and are left as they are,
because neither is used as a join key: `REU` is spelled "Runion" and code `044` has garbled quote characters.

## Choices that affect the results

- **Event codes are strings.** `010` ("Make statement") and `10` are different codes, and `0211` has a meaningful
  leading zero. Never cast `EventCode`, `EventBaseCode` or `EventRootCode` to a number.
- **Sub-state codes are mapped to their state (cleaning rule shared with Member 2).** The first three rows of
  `CAMEO.country.txt` are not countries: `WSB` (West Bank) and `GZS` (Gaza Strip) are parts of the Palestinian
  territory, and `BAG` is Baghdad. Excluding them would shrink the Israel–Palestine and Iraq dyads, so they are
  rewritten to `PSE`, `PSE` and `IRQ` before any other dyad rule runs. The remap happens **before** the same-country
  check, so an event between `WSB` and `PSE` becomes `PSE`–`PSE` and is dropped as domestic. The notebook prints
  how many actor values each mapping rewrites.
- **Region codes are excluded from dyads.** The next 25 rows (`AFR`, `EUR`, `MEA`, `WST`, ...) are regions, not
  states. A "dyad" in this project is a pair of states, so rows with a region on either side are dropped, and the
  notebook prints how many rows each excluded code removes.
- **Dependent territories that GDELT lists as countries are kept** (e.g. `PRI`, `GUM`, `HKG`, `TWN`), because GDELT
  codes them as separate actors. This follows the source rather than a political judgement.
- **Codes missing from the lookup are reported, not guessed.** If the events use a country code that is not in
  `CAMEO.country.txt`, the notebook lists it with its row count and drops it from the dyads.
- **The `*Geo_CountryCode` columns are not used for dyads.** They hold 2-letter FIPS 10-4 codes, not 3-letter CAMEO
  codes, so mixing them with the actor columns would create wrong pairs.
