# drawdown_logic.py
import pandas as pd


def calculate_drawdown(nifty_data: pd.DataFrame) -> pd.DataFrame:
    # Calculate daily returns
    nifty_data['Daily_Return'] = nifty_data['price'].pct_change()

    # Calculate cumulative returns
    nifty_data['Cumulative_Return'] = (1 + nifty_data['Daily_Return']).cumprod()

    # Calculate drawdowns
    nifty_data['Peak'] = nifty_data['Cumulative_Return'].cummax()
    nifty_data['Drawdown'] = (nifty_data['Cumulative_Return'] - nifty_data['Peak']) / nifty_data['Peak']

    return nifty_data[['date', 'Drawdown']]
