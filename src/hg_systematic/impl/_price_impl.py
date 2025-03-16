from frozendict import frozendict
from hgraph import service_impl, generator, TSS, TSD, TS, TimeSeriesSchema, CompoundScalar, Frame, graph, \
    EvaluationEngineApi, map_, no_key
from datetime import date, datetime

from hg_systematic.operators import price_in_dollars

__all__ = ["price_in_dollars_static_impl", "StaticPriceSchema"]


class StaticPriceSchema(CompoundScalar):
    """A very simplistic schema for replaying prices"""
    date: date
    symbol: str
    price: float


@service_impl(interfaces=[price_in_dollars])
@graph
def price_in_dollars_static_impl(symbol: TSS[str], prices: Frame[StaticPriceSchema], round_to: int = 2) -> TSD[str, TS[float]]:
    return map_(lambda price: price, _price_in_dollars_static_impl(prices, round_to), __keys__=symbol)


@generator
def _price_in_dollars_static_impl(prices: Frame[StaticPriceSchema], round_to: int, _api: EvaluationEngineApi=None) -> TSD[str, TS[float]]:
    # This approach (ticking everything without filter, etc. has the consequence of not having value immediately
    # available, however, in a dynamic implementation this would have a delay between the time of request to
    # value being available. This could have consequences when swapping out implementations.
    import polars as pl
    prices = prices.lazy().sort(
        by="date"
    ).cast(
        {'date': datetime}
    ).with_columns(
        pl.col("price").round(round_to)
    ).filter(
        pl.col("date").is_between(_api.start_time, _api.end_time)
    ).collect().partition_by(
        "date", maintain_order=True, include_key=False, as_dict=True
    )
    for dt, prices_ in prices.items():
        yield dt[0], frozendict(prices_.iter_rows())
