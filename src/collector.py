import jdatetime
import pandas as pd
import pytse_client as tse
from loguru import logger

from src.tickers_data import DATA_DIR


class Collector():
    """
    Market data collector for Tehran Stock Exchange (tse).
    """
    def __init__(self):
        self.all_symbols = tse.all_symbols

    def collect(self, symbol:str, start_date:str='1400-01-01', end_date:str=jdatetime.datetime.today().strftime('%Y-%m-%d'), write_to_csv:bool=False):
        """
        Collect data for a symbol from start_date to end_date.

        Args:
            symbol: a stock symbol
            start_date: start date for collected data. Defaults to '1400-01-01'.
            end_date: end date for collected data. Defaults to today.
            write_to_csv: save file as csv format or not. Defaults to False.

        Returns:
            pandas.DataFrame: Containing all useful market data.
        """
        ticker = self.scrape(symbol)
        df_history =  self.slice_date(self.collect_history(ticker), start_date, end_date)
        df_client =  self.slice_date(self.collect_client(ticker), start_date, end_date)
        df = pd.concat([df_history, df_client], axis=1)

        # Slice Date
        df = self.slice_date(df, start_date, end_date)

        if write_to_csv:
            path = DATA_DIR/(symbol + '.csv')
            df.to_csv(path)
        return df

    def slice_date(self, df, start_date:str, end_date:str):
        """
        Slice a market dataframe based on start_date and end_date.
        """
        start = jdatetime.datetime.strptime(start_date, '%Y-%m-%d').togregorian()
        end = jdatetime.datetime.strptime(end_date, '%Y-%m-%d').togregorian()

        return df[start:end]

    def collect_all(self):
        pass

    def scrape(self, symbol):
        """
        Scrape data of symbol from pytse-client module and return a ticker object.
        """
        return tse.Ticker(symbol)


    def collect_history(self, ticker):
        """
        Collect Historical data of a ticker.
        """
        df_history = ticker.history.loc[:, ['volume', 'value', 'close']]
        df_history.index = ticker.history.loc[:, 'date']
        df_history = self.rolling_mean_value(df_history)
        df_history.assign(value_ratio_20 = (df_history['value'] / df_history['mean_value_20']).round(decimals=2))
        return df_history

    def collect_client(self, ticker):
        """
        Collect client-type data of a ticker.
        """
        df_client = ticker.client_types.loc[:, [
            'individual_buy_count', 'individual_buy_value',
            'individual_sell_count', 'individual_sell_value',
            'corporate_buy_value', 'corporate_sell_value',
            ]]
        df_client.index = ticker.client_types.loc[:, 'date'].apply(pd.Timestamp)
        df_client = df_client.sort_index(ascending=True)

        individual_buy_value = df_client['individual_buy_value'].apply(eval)
        individual_sell_value = df_client['individual_sell_value'].apply(eval)

        df_client['buy_per_capita'] = individual_buy_value / (
            df_client['individual_buy_count'].apply(eval) * 10_000_000)
        df_client['sell_per_capita'] = individual_sell_value / (
            df_client['individual_sell_count'].apply(eval) / 10_000_000)
        df_client['individual_power'] = df_client[['buy_per_capita', 'sell_per_capita']].apply(
            lambda x : self.get_individual_power(x['buy_per_capita'], x['sell_per_capita']), axis=1)

        df_client['individual_buy_percent'] = (individual_buy_value / (individual_buy_value + df_client['corporate_buy_value'].apply(eval))).round(decimals=3) * 100
        df_client['individual_sell_percent'] = (individual_sell_value / (individual_sell_value + df_client['corporate_sell_value'].apply(eval))).round(decimals=3) * 100

        return df_client.loc[:, ['buy_per_capita', 'sell_per_capita', 'individual_power', 'individual_buy_percent', 'individual_sell_percent']]

    def rolling_mean_value(self, df_history, days=20):
        """
        Computes the moving value average.
        """
        df_history['mean_value_20'] = df_history['value'].rolling(window=days, min_periods=1).mean()
        return df_history


    def get_individual_power(self, buy_per_capita, sell_per_capita):
        """
        Computes buyer/seller power based on average buy and sell value.
        """
        if buy_per_capita >= sell_per_capita:
            return (buy_per_capita / sell_per_capita).round(decimals=2)
        else:
            return - (sell_per_capita / buy_per_capita).round(decimals=2)

    def slice_date(self, df, start_date='1390-01-01', end_date=jdatetime.datetime.today().strftime('%Y-%m-%d')):
        """
        Slice a ticker using start and end date.
        """
        start = jdatetime.datetime.strptime(start_date, '%Y-%m-%d').togregorian()
        end = jdatetime.datetime.strptime(end_date, '%Y-%m-%d').togregorian()

        return df[start:end]


    def scrape_funda(self):
        pass


if __name__ == '__main__':
    collector = Collector()
    df = collector.collect('فولاد', write_to_csv=True)
    logger.info(df)
