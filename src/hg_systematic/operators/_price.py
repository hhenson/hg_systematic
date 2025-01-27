from hgraph import subscription_service, TS, default_path


__all__ = ["price_in_dollars"]


@subscription_service
def price_in_dollars(symbol: TS[str], path: str = default_path) -> TS[float]:
    """
    Represent the current price of this symbol in dollars. The symbol can represent both simple and complex things.
    """


