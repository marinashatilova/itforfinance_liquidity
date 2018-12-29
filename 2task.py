#Предворительный этап: Соединение базы данных и python
import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


class MySQLWorker(object):

    def __init__(self, usr, pswd, db):
        self.tikers = {}
        self.rows = str()

        self.connection = mysql.connector.connect(
            host="localhost",
            user=usr,
            password=pswd
        )
        self.cur = self.connection.cursor()
        self.cur.execute('USE {};'.format(db))


    def create_table(self):
        with open('ListingSecurityList_new.csv', 'r', encoding='cp1251') as f:
            tickers_text = f.read()
        for line in tickers_text.split('\n')[:-1]:
            unit = line.split(',')
            self.tikers[unit[7].replace('"', '')] = unit[5].replace('"', '')

    def select_ticker(self):
        ticker = input('Введите тикер: ')
        time = int(input('Введите время: '))

        if self.tikers[ticker] == 'Акция обыкновенная':
            t = "SELECT orderno, action, buysell, price, volume, time FROM CommonStock \
                where TIME<= '{}' and seccode = '{}';".format(time, ticker)
            self.cur.execute(t)
            self.rows = self.cur.fetchall()
        elif self.tikers[ticker] == 'Акция привилегированная':
            t = "SELECT orderno, action, buysell, price, volume, time FROM PreferredStock \
                where TIME<= '{}' and seccode = '{}';".format(time, ticker)
            self.cur.execute(t)
            self.rows = self.cur.fetchall()
        elif 'облигац' in self.tikers[ticker].lower():
            t = "SELECT orderno, action, buysell, price, volume, time FROM Bonds \
        where TIME<= '{}' and seccode = '{}';".format(time, ticker)
            self.cur.execute(t)
            self.rows = self.cur.fetchall()

    def vizualization(self):
        glass = []  # (buysell, price, volume)
        icebergs = []  # (orderno, hidden_volume, time)

        for elem in self.rows:
            # Если происходит размещение заявки
            if elem[1] == 1:
                
                if elem[4] != 0:
                    
                    if len(glass) > 0:
                        flag = False
                        for j in glass:
                            
                            if j[1] == elem[3] and j[0] == elem[2]:
                                
                                j[2] += elem[4]
                                flag = True
                               
                                break
                      
                        if not flag:
                            glass.append([elem[2], elem[3], elem[4]])
                    
                    else:
                        glass.append([elem[2], elem[3], elem[4]])
                else:
                    pass
            # Если снятие заявки
            elif elem[1] == 0:
                
                for j in glass:
                    
                    if j[1] == elem[3] and j[0] == elem[2]:
                        
                        j[2] += -elem[4]
                        
                        if j[2] == 0:
                            del (j)
                        
                        elif j[2] < 0:
                            
                            icebergs.append([elem[0], -j[2], elem[5]])
                            
                            del (j)
                        
                        break
            # Если сделка
            elif elem[1] == 2:
                
                for j in glass:
                    
                    if j[1] == elem[3] and j[0] == elem[2]:
                        
                        j[2] += -elem[4]
                        
                        if j[2] == 0:
                            del (j)
                        
                        elif j[2] < 0:
                            
                            icebergs.append([elem[0], -j[2], elem[5]])
                            
                            del (j)
                        break

        # Очищаем "стакан" от заявок с нулевым объемом:
        glass_new = []
        for j in range(0, len(glass)):
            if glass[j][2] != 0:
                glass_new.append(glass[j])

        
        df = pd.DataFrame.from_records(glass_new, columns=['buy/sell', 'price', 'volume'])
        df['buy_volume'] = np.where(df['buy/sell'] == 'B', df['volume'], 0)
        df['sell_volume'] = np.where(df['buy/sell'] == 'S', df['volume'], 0)
        del df['buy/sell']
        del df['volume']
        df.sort_values('price', inplace=True, ascending=False)
        df = df.reindex(columns=['buy_volume', 'price', 'sell_volume'])
        print(df)

        # график:
        buy = df.loc[(df['buy_volume'] > 0)]
        sell = df.loc[(df['sell_volume'] > 0)]
        bid_price = buy['price']
        bid_volume = buy['buy_volume']
        ask_price = sell['price']
        ask_volume = sell['sell_volume']

        fig, ax = plt.subplots()
        plt.plot(ask_price, ask_volume, color='red', label='asks', marker='o', markersize=2)
        plt.text(ask_price[-1:], ask_volume[-1:], str(int(ask_volume[-1:])))
        plt.plot(bid_price, bid_volume, color='green', label='bids', marker='o', markersize=2)
        plt.text(bid_price[:1], bid_volume[:1], str(int(bid_volume[:1])))
        plt.text(bid_price[-1:], max(max(ask_volume), max(bid_volume)),
                 'bid-ask spread = %s' % str(round(float(ask_price[-1:]) - float(bid_price[:1]), 3)))
        ax.set(xlabel='Price', ylabel='volume')
        plt.legend()
        plt.show()

        iceberg = pd.DataFrame.from_records(icebergs, columns=['orderno', 'hidden_volume', 'time'])
        print(iceberg)

    def release(self):
            self.cur.close()
            self.connection.close()

def main():

    my = MySQLWorker('root', 'root', 'tradein')
    my.create_table()
    my.select_ticker()
    my.vizualization()
    my.release()

    return 0


if __name__ == '__main__':
    exit(main())