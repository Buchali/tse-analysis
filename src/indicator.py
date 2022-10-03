from loguru import logger

from src.collector import Collector


class Indicator:
    def __init__(self):
        self.collector = Collector()

    def pile(self, symbol, window=14):
        df = self.collector.collect(symbol)
        return df['individual_power'].rolling(window=window).sum()


if __name__ == '__main__':
    indicator = Indicator()
    pile = indicator.pile('فولاد')
    logger.info(pile)
