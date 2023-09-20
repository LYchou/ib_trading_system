import pandas as pd
import numpy as np

def separate_orders(orders: pd.DataFrame) -> tuple:
    '''
    Function
    In IB, it is not allowed to buy and sell the same stock simultaneously within sub-accounts.
    Therefore, orders need to be submitted sequentially.
    This function provides the functionality to separate orders into two rounds:
    The first round consists of all sell orders and sell orders involved in cross orders.
    The second round consists of buy orders involved in cross orders.

    Parameters
    orders: pd.DataFrame
        A DataFrame containing order information, should at least include 'Action', 'Symbol', and 'SecType' columns.
        'Action' column indicates buy/sell actions, 'Symbol' column indicates stock symbols, and 'SecType' column indicates stock types.

    Returns
    tuple
        A tuple containing two elements:
        1. DataFrame of orders for the first round (sell orders and non-cross-account buy orders).
        2. DataFrame of orders for the second round (buy orders involved in cross-account trades).
    '''

    if len(orders):
        # Separate orders into buy and sell orders
        buy_orders_df = orders[orders['Action'] == 'BUY']
        sell_orders_df = orders[orders['Action'] == 'SELL']

        if (len(buy_orders_df) == 0 or len(sell_orders_df) == 0):
            # If there are no buy or sell orders, both the first and second rounds of orders are empty
            firstRound_orders = orders
            secondRound_orders = pd.DataFrame(columns=orders.columns)
        else:
            # Extract stock symbols and types for buy and sell orders
            buy_orders_equity_features_arr = buy_orders_df[['Symbol', 'SecType']].values
            sell_orders_equity_features_arr = sell_orders_df[['Symbol', 'SecType']].values

            # Convert stock symbols and types to tuples for comparison
            buy_orders_equity_features_tuples = [tuple(row) for row in buy_orders_equity_features_arr]
            sell_orders_equity_features_tuples = [tuple(row) for row in sell_orders_equity_features_arr]

            # Check for cross-account trades
            is_duplicate = np.isin(buy_orders_equity_features_tuples, sell_orders_equity_features_tuples).all(axis=1)

            # Extract buy orders involved in cross-account trades and non-cross-account buy orders
            cross_buy_orders_df = buy_orders_df.iloc[is_duplicate, :]
            not_cross_buy_orders_df = buy_orders_df.iloc[~is_duplicate, :]

            # The first round of orders includes sell orders and non-cross-account buy orders
            firstRound_orders = pd.concat([sell_orders_df, not_cross_buy_orders_df], axis=0)

            # The second round of orders includes buy orders involved in cross-account trades
            secondRound_orders = cross_buy_orders_df
    else:
        # If the orders DataFrame is empty, both the first and second rounds of orders are empty
        firstRound_orders = orders
        secondRound_orders = orders

    # Reset the index for clarity in the results
    return firstRound_orders.reset_index(drop=True), secondRound_orders.reset_index(drop=True)
