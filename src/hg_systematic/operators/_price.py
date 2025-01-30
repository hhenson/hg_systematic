from hgraph import subscription_service, TS, default_path


__all__ = ["price_in_dollars", "SETTLEMENT_PRICE", "LIVE_PRICE", "returns_in_dollars",]

# Ticks with the settlement price when available.
SETTLEMENT_PRICE = "settlement_price"

# Ticks with the live price
LIVE_PRICE = "live_price"

@subscription_service
def price_in_dollars(symbol: TS[str], path: str = default_path) -> TS[float]:
    """
    Represent the current price of this symbol in dollars. The symbol can represent both simple and complex things.
    """


@subscription_service
def returns_in_dollars(symbol: TS[str], path: str = default_path) -> TS[float]:
    """
    The current returns in dollars for this symbol. This is based on close-to-close prices.
    """

