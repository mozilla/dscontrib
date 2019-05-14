# Mozilla Growth and Usage Dashboard Prototype - gudnightly

This is primarily a prototype for development of the Mozilla Growth and Usage Dashboard (GUD), but I have found it useful for some analysis, so have decided to make it available to others in case you find it useful too.

At its core, it supports plotting of growth metrics across a range of usage criteria, metrics, and slices.

Compared to the real GUD, it has some disadvantages:

 - It is slow (depending on the state of the cluster, it may be "slightly annoying" slow or might be "work on something else while it runs" slow)
 - I offer no support nor guarantee that it works
 - Although some validation of the metrics it produces has been performed, it should not be considered an authoritarive source of truth

It also has some advantages:

 - It supports features that are unlikely to appear in GUD anytime soon including year-over-year analysis, day of week analysis, normalization for comparisons, smoothing, etc.
 - It is relatively easy to add new functionality
 - It actually already exists ;)

For usage please see [the example notebook](https://dbc-caf9527b-e073.cloud.databricks.com/#notebook/117123/).

Known Issues:

 - The MetricPlot code is a huge mess.  I plan to refactor.
 - The Retention metric sometimes produces slightly (<1%) different values that GUD due to a difference in how profiles active in multiple slices are handled.
 - The Comparison and Table functions are not yet available.
 - We currently only support Desktop usage.

Questions or comments?  Want to add some functionality?  Please contact jmccrosky@mozilla.com.
