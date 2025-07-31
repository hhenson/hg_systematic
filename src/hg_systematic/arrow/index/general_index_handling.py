"""

A general index can be modelled as a portfolio of things. We will consider things to be symbols which represent
something that can be traded.

So Index: <<current_holding, previous_holding, target_holding>, current_values, level>,
where the previous and target holdings are only present during re-balancing of the index.
The current values are the current values of the underlying assets.

holding: TSD[<symbol>, TS[<quantity>]]
values: TSD[<symbol>, TS[<value>]]
level: TS[<value>]


To represent this in Arrow format we need to think about how to bundle this data into a collection of tuples.


"""