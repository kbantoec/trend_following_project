import dataset
import strategy

import functools
import numpy as np

from pyutil.portfolio.portfolio import Portfolio
from pyutil.performance.summary import NavSeries, fromReturns


@functools.lru_cache(maxsize=32)
def stats_from_parameters(name, price_set, fee_rate_bps=10, get_top=2, 
                          target_vol=40, periods=12, vol_weight=False):
    """Return statistics for various strategies and data sets.
    
    name         -- Name of the strategy. Can be 'Antonacci', 'TSMOM'.
    price_set    -- The price set to use. Can be 'ETF', 'Futures'. 
                    TSMOM can only be used with 'Futures'
    fee_rate_bps -- Transaction cost bps.
    get_top      -- If using Antonacci, number of assets to use.
    target_vol   -- Per asset target volatility level for TSMOM.
    periods      -- Number of previous periods to look at for judging performance.
    vol_weight   -- If using Antonacci, selected assets are equally weighted.
                    With this flag, they are weighted based on inverse volatility.
    """
    # Converting to percentage rates
    fee_rate = fee_rate_bps / 1e4
    target_vol = target_vol / 1e2
    
    # Preparing the price data based on user input
    if price_set == 'ETF':
        price = dataset.etf_price()
    elif price_set == 'Futures':
        price = dataset.futures_price()
    else:
        raise Exception("Unknown Price Set")
    
    # Executing the strategy based on user input
    if name == 'Antonacci':
        # Asset number is limited by available assets
        get_top = np.min([price.shape[1], get_top])
        
        # If the price set is ETF, use the excess returns for better signal
        # The futures prices are already excess returns
        if price_set == 'ETF':
            weight_price = excess_return_price(price)
        else:
            weight_price = price
        
        # Antonacci can be done with inverse volatility weight
        if vol_weight:
            weight = strategy.antonacci_inv_vol_weight(weight_price, get_top=get_top, periods=periods)
        else:
            weight = strategy.antonacci_weight(weight_price, get_top=get_top, periods=periods)
        
        # Aggregate the price
        price = strategy.antonacci_price(price)
    elif name == 'TSMOM':
        # Compute the aggregated price and weights for TSMOM. Can only be used with futures
        assert price_set == 'Futures', 'Long short strategy can only be done with futures.'
        weight = strategy.tsmom_weight(price, target_vol=target_vol, periods=periods)
        price = strategy.tsmom_price(price)
    else: 
        raise Exception("Unknown Strategy")
    
    # Calculate the statistics
    stats = portfolio_stats(price, weight, fee_rate=fee_rate, add_rf=(price_set == 'Futures'))
    
    return stats


def excess_return_price(price, monthly=False):
    """Return price generated after excess return.
    
    monthly -- The price is assumed to be daily. This flag changes it to monthly.
    """
    returns = price.pct_change()
    risk_free = dataset.risk_free_rate()
    
    # Converting the annualized risk free rate to monthly 
    if monthly:
        risk_free = risk_free / 12
    else:
        risk_free = risk_free / 360
    
    # Substracting risk free rate from returns
    risk_free = risk_free.loc[price.index]
    excess_return = returns.sub(risk_free.rate, axis=0)    
    excess_price = (excess_return + 1).cumprod()
    
    return excess_price


def portfolio_stats(price, weights, add_rf=False, fee_rate=0.001):
    """Return overview and statistics of a given portfolio.
    
    price    -- Underlying price set of the assets.
    weights  -- Weights calculated by the strategy.
    add_rf   -- Adds the monthly risk free rate to portfolio returns.
    fee_rate -- Transaction costs, default 10bps. 
    """
    # Initializing the portfolio class
    portfolio = Portfolio(price, weights)
    
    # We might have to do operationg like adding the risk free rate
    # to returns or substracting the transaction costs. These are not
    # implemented in the Portfolio class and creating our own 
    # returns objects to be able to intereve with NavSeris.
    
    # https://github.com/lobnek/pyutil/blob
    # /4d16e2013b265f1723a92ec57da38886007f6a50
    # /pyutil/portfolio/portfolio.py#L241
    returns = portfolio.weighted_returns.sum(axis=1)
    
    # Calculating risk free rate. May be used for returns, 
    # will be used for calculating Sharpe ratio
    risk_free = dataset.risk_free_rate() / 12
    risk_free = risk_free.resample('M').last()
    risk_free = risk_free.loc[returns.index]
    
    # Adding the risk free rate to returns if asked
    if add_rf:
        returns = returns.add(risk_free.rate, axis=0)
    
    # Turnover is not calculated in pyutil, calculating it
    turnover = portfolio.weights.diff().abs().sum(axis=1)
    
    # Calculating the fee cost
    fee = turnover * fee_rate
    
    # Deducting the fees from the returns
    returns -= fee
    
    # Creating the nav object to be used. 
    nav = fromReturns(returns)

    # Calculating averate risk free rate for calculation of Sharpe ratio
    rf_period = risk_free.shape[0]
    risk_free = risk_free * 12 # Previously it was divided to 12, reversing
    rf_rate = risk_free.dropna().add(1).prod().pow(1/rf_period).sub(1).rate
    
    # Calculating portfolio summary
    summary = nav.summary(r_f=round(rf_rate, 4))

    return {
        'portfolio': portfolio,
        'returns': returns,
        'turnover': turnover,
        'turnover_annual': turnover.rolling(12).sum(),
        'leverage': portfolio.leverage,
        'drawdown': nav.drawdown,
        'asset_class_weights': portfolio.weights.T.groupby(level=0).sum(),
        'ewm_volatility': nav.ewm_volatility(),
        'summary': summary,
        'nav': nav
    }
