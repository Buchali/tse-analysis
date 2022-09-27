import jdatetime
import pandas as pd
import pytse_client as tse
from loguru import logger


class Collector():
    """
    Market data collector for Tehran Stock Exchange (tse).
    """
    def __init__(self):
        self.all_symbols = tse.all_symbols

    def collect(self, symbol, start_date='1400-01-01', end_date=jdatetime.datetime.today().strftime('%Y-%m-%d'), write_to_csv=False):
        ticker = self.scrape(symbol)
        df = pd.concat([self.collect_history(ticker), self.collect_client(ticker)], axis=1)

        # Slice Date
        df = self.slice_date(df, start_date, end_date)
        return df

    def slice_date(self, df, start_date:str, end_date:str):
            """
            Slice a ticker using start and end date.
            """
            start = jdatetime.datetime.strptime(start_date, '%Y-%m-%d').togregorian()
            end = jdatetime.datetime.strptime(end_date, '%Y-%m-%d').togregorian()

            return df[start:end]

    def collect_all(self):
        pass

    def scrape(self, symbol):
        """
        Scrape data of symbol from pytse-client module.
        """
        return tse.Ticker(symbol)


    def collect_history(self, ticker):
        df_history = ticker.history.loc[:, ['volume', 'value', 'close']]
        df_history.index = ticker.history.loc[:, 'date']
        df_history = self.rolling_mean_value(df_history)
        df_history.assign(value_ratio_20 = (df_history['value'] / df_history['mean_value_20']).round(decimals=2))
        return df_history

    def collect_client(self, ticker):
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
        df_history['mean_value_20'] = df_history['value'].rolling(window=days, min_periods=1).mean()
        return df_history


    def get_individual_power(self, buy_per_capita, sell_per_capita):
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
    df = collector.collect('فولاد')
    logger.info(df)
