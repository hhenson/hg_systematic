Index
=====

An index is tool that can be used to abstract the performance of one or more assets, sectors, etc.

This provides a number of useful components that can be used to implement an index.

There are a number of different ways to implement an index, this package has explored the BCOM Index, as well as
an alternative mechanism to build custom indices.

For more information on the BCOM Index, see https://assets.bbhub.io/professional/sites/10/BCOM-Methodology.pdf.

Here are some of the common techniques and concepts:

The general process:

1. By convention an index is started at 100.0 for it's initial value.
2. An index is generally backed by one or more financial instruments (for example, stocks, futures, etc.)
3. The index has the concept of a weighting which describes the ratio of the instruments that make up the index.
4. The index 'holds' a position in those instruments, these can be referred to as units (as they are generally fractional).
5. There is a re-balance schedule, on the re-balance schedule the structure of the index is brought back in line to its target weights.
6. There is a re-balancing period that starts with the re-balance and ends once the target structure is achieved.
7. The structure of the index can change at the re-balance as well, when the index is based on futures then the is also typically a roll-schedule
   that is used to determine when to move between different future contracts as time moves forward.
8. The index will follow a holiday calendar schedule for publishing values.

We will focus on indices that are based on future contracts now.

When dealing with futures contracts in commodities, especially agricultural futures tend to have frequent trading disruption
where the market is non-tradable. This results in complications when attempting to structure indices over these instruments.

This adds the concept of a trading halt. This is used when rolling to delay the movement from the current holding to 
the target holding structure.

Another method used to simplify the construction of more complicated indices, is to break down indices into simple
single asset indices and then create more complex indices from these simple single asset indices. This allows
the complexity of dealing with the asset to be placed into one structure and then the more complex structures
can be assembled using this simple (or other complex) underlyers.

So we could indicate a structure as follows:

```
Index
  |
  +- SingleAssetIndex
  |
  +- MultiIndex
```

Where the MultiIndex consists of one or more Index (either SingleAssetIndex or MultiIndex).

The other interesting problem created by the pricing mechanism of an index is that the index is a path dependent creature.
This means that any issue in pricing when re-producing an index results in a deviation in the levels reported.

However, for most indices they are also possible to re-create mid-stream with limited state. The design attempts
to expose this characteristic to support a more aggressive iterative computation model.
