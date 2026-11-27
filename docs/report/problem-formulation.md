<!-- Draft v1 (Member 3), 2026-11-26. [TBD: ...] marks a number that comes from the full-data Databricks run
     (m3_run_params_full, m3_cleaning_funnel_full, dyad_weekly_features_full) or from another member's section. -->

## Problem Formulation

### Motivation

Analysts who monitor geopolitical risk read the news continuously, looking for signs that a relationship between two
countries is deteriorating before it turns violent. The Global Database of Events, Language, and Tone (GDELT) makes
this kind of monitoring possible at scale. Every day it machine-codes the world's news into roughly 100,000
structured event records (37.2 million in the year studied here). Each record says who did what to whom, on which
day, how widely the event was reported, and how positive or negative the coverage was. The question that motivates this project is whether the *tone* of that coverage carries
early-warning information. If coverage of a pair of countries turns more negative, does material conflict between
them tend to follow?

Answering this needs big-data methods for two reasons:
- **Volume.** One year of GDELT 1.0 daily event files holds 37,225,966 event records (November 2025 to October 2026).
- **Scale of the comparison.** The analysis has to be repeated for every pair of countries, every week. Building a
  per-pair sentiment index means filtering, joining, aggregating and windowing the full event stream, which this
  project does on a distributed platform: Spark on Databricks, with the ingestion and Pig stages on a local Hadoop
  cluster.

### Research question and hypothesis

**Research question.** Does a drop in news tone about a pair of countries predict a rise in material conflict between
that pair in the following week?

Let *d* be a dyad (an unordered pair of states) and *t* a calendar week.
- **Tone signal:** Δtone(*d*, *t*), the change in the dyad's mentions-weighted news tone in week *t* relative to its own
  recent baseline.
- **Outcome:** conflict(*d*, *t* + 1), the dyad's volume of material-conflict events in the next week.

The hypotheses are:

- **H₀:** Δtone(*d*, *t*) carries no information about conflict(*d*, *t* + 1) beyond what the dyad's own recent
  conflict history already provides.
- **H₁:** a drop in tone (Δtone < 0) is followed by a higher probability of a sharp rise in material conflict in week
  *t* + 1, after controlling for the dyad's recent conflict level.

The test is predictive, not causal. Member 4's model (Section [TBD: ML section number]) estimates whether adding the
tone features to a baseline of conflict-history features improves prediction of a sharp rise on held-out later weeks.

### Unit of analysis and definitions

| Concept | Definition used in this project |
| --- | --- |
| **State** | A CAMEO country code from GDELT's country lookup, excluding 25 region codes (e.g. Europe, Middle East). The sub-state codes for the West Bank and Gaza Strip are mapped to Palestine, and Baghdad to Iraq. This leaves 233 codes. |
| **Dyad** | An unordered pair of distinct states {A, B}. An event initiated by A towards B and one initiated by B towards A both belong to the same dyad. Events between actors of the same state are domestic and excluded. |
| **Event date** | GDELT's `SQLDATE`, the date the event took place, as opposed to the date the report was published. |
| **Recency filter** | An event row is used only if it was reported at most *N* days after it occurred. That excludes the many references to events from months or years earlier that appear in every daily file. *N* = [TBD: N] days, chosen from the observed reporting-lag distribution (Section [TBD: Methodology – Spark]). |
| **Week** | Monday to Sunday, keyed by its Monday. Only weeks whose events are completely contained in the loaded files are analysed: [TBD: n_weeks] weeks from [TBD: first_week] to [TBD: last_week]. |
| **Tone** | GDELT's `AvgTone`: the average tone of the documents reporting an event, from about −100 (extremely negative) to +100 (extremely positive). In practice most values lie between −10 and +10. |
| **Mentions-weighted tone** | For a set of events *E* (a day, a 7-day window or a week): tone(*E*) = Σ AvgTone·NumMentions / Σ NumMentions. Widely reported events count more, in proportion to how often they were mentioned. |
| **Rolling tone index** | The mentions-weighted tone of a dyad over the 7 calendar days ending on a given day. |
| **Material conflict** | Events with `QuadClass = 4`, i.e. CAMEO root codes 15–20: exhibiting force posture, coercion, assault, fighting, and unconventional mass violence. |
| **Sharp rise** | The prediction target, defined by Member 4 from next week's material-conflict count relative to the dyad's own trailing four-week mean and variability ([TBD: final label definition from the ML section]). |

The analysis covers the [TBD: selected_dyads] dyads that average at least five events per week over the first half
of the study period. Less active dyads produce a tone measured on only a handful of articles, which is mostly noise.

### Scope

The project uses GDELT 1.0 daily event files for November 2025 to October 2026. A one-month sample (October 2026,
3,195,940 records) is used for development, for the local Hadoop cluster, and for the end-to-end run of the code
deliverable. The 15-minute GDELT 2.0 update files have a different format and are used only in the streaming component
(Section [TBD: streaming section]).

The deliverables of the Spark stage are:
1. a daily rolling tone index for every active dyad, also loaded into HBase for low-latency lookup by dyad and date;
2. a weekly feature table for the prediction model;
3. a ranking of the most central states in the country-to-country event network;
4. a measured before-and-after study of data skew in the shuffle stages and of salting as a remedy.

### Limitations of the formulation

**GDELT measures media coverage, not events.** This is the most important limitation, and it shapes how every result
must be read. A GDELT "event" is a news report that a coding algorithm interpreted as an event. A rise in material
conflict between two countries can mean that more conflict happened, or that the same conflict was reported more:
because a major outlet started covering the region, because the story was syndicated more widely, or because overall
news volume changed. The consequences:

- **Coverage is uneven.** Countries that are prominent in English-language and international media are
  over-represented. The United States appears in [TBD: share of clean events involving USA] of the clean dyadic
  events. Quiet dyads may experience real conflict that GDELT barely records.
- **Tone and conflict share a source.** Both the predictor and the outcome come from the same articles. A burst of
  negative coverage and a burst of coded conflict events can be one news cycle seen twice, which would inflate any
  apparent lead–lag relationship. Using next week's conflict as the target, and conditioning on the dyad's own
  recent conflict level, reduces but does not remove this.
- **Coding errors.** The CAMEO coding is automatic and imperfect: actors can be assigned to the wrong country, and
  reports of past or hypothetical events can be coded as current. The recency filter and the cleaning rules remove
  the most obvious cases. They cannot detect a wrong code that looks plausible.
- **Tone describes language, not intent.** `AvgTone` scores the wording of the articles, not the attitude of the
  governments involved, and it differs between outlets and languages.

**Other limitations.**
- The study covers one year, which limits how many rare escalation episodes the model can learn from.
- The dyad selection favours relationships that are already newsworthy.
- The research design can establish predictive association, not causation.

These caveats are repeated where results are interpreted (Sections [TBD: results sections]).
