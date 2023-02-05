from multiprocessing import Queue
import time
import schedule

import bybit_stream
import fundings_analyzer

class DataHarvester:
    def __init__(self, qoutation, harvest_time, funding_rate):
        self.quotation = qoutation
        self.harvest_time = harvest_time
        self.funding_rate = funding_rate

    def data_stream(self):
        stream = bybit_stream.Bybit_Stream(
            '', '', 'trade.' + self.quotation, 
            self.harvest_time,
            self.funding_rate
        )
        q = Queue()
        stream.run(q)

if __name__ == '__main__':
    analyzer = fundings_analyzer.FundingAnalyzer()

    harvest_time = 600

    def run_harvest():
        funds = analyzer.get_fundings()[-20:]

        for i in funds:
            print(i[0], i[1])
            DataHarvester(i[0], harvest_time, i[1]).data_stream()

    schedule.every().day.at("02:55").do(run_harvest)
    schedule.every().day.at("10:55").do(run_harvest)
    schedule.every().day.at("18:55").do(run_harvest)

    while True:
        schedule.run_pending()
        time.sleep(1)