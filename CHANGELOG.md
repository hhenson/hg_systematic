Changelog
=========

A brief summary of the changes made.

Version 0.0.1
-------------

* Initial structure for the project

Version 0.0.2
-------------

* First pass implementation of the power auto-correlation strategy in notebook format.

Version 0.0.3
-------------

* Add "production" style code for strategy.

Version 0.0.4 (28-01-2025)
--------------------------

* Initial structure to support pricing and managing an index.
* basic contract roll operators and logic.

Version 0.0.5 (30-01-2025)
--------------------------

* Update python version dependencies and added returns signature.


Version 0.0.6 (31-01-2025)
--------------------------

* Expose month code logic

Version 0.0.8 (17-01-2025)
--------------------------

* Re-work index example code
* Change the structure to make use of more operators for the index impl.

Version 0.0.9 (17-01-2025)
--------------------------

* Move rounding out of component

Version 0.0.10 (17-01-2025)
---------------------------

* Fix new_period logic

Version 0.0.11 (18-01-2025)
---------------------------

* Small fixes

Version 0.0.12 (22-02-2025)
---------------------------

* Update to hgraph 0.4.0 and update to new switch_ signature

Version 0.0.13 (25-02-2025)
---------------------------

* Cleanups to index infrastructure in preparation for a new index computation approach

Version 0.0.14 (25-02-2025)
---------------------------

* Add rolling_contracts_for

Version 0.0.15 (27-02-2025)
---------------------------

* Clean-up rolling info


Version 0.0.16 (03-03-2025)
---------------------------

* Expose y, m, d of dt in MonthlyRollingInfo

Version 0.0.19 (18-03-2025)
---------------------------

* Work on single asset index

Version 0.0.28 (25-03-2025)
---------------------------

* Index calculation logic completed for Multi index and Single asset indices.

Version 0.0.30 (19-05-2025)
---------------------------

* Bug fix on trade date generation

Version 0.0.31 (21-05-2025)
---------------------------

* Update to support fixed weight multi-index

Version 0.0.34 (30-05-2025)
---------------------------

* Update to add StubIndex logic to support indices that are not computed but instead loaded from a price source.
