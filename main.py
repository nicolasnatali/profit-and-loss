import csv
import sys
from collections import defaultdict, deque


class OrderManagementSystem:
    def __init__(self):
        self.queue = defaultdict(deque)
        self.total = float()

    def run(self, file: str) -> float:
        """
        Runs the order management system by placing trades for each entry in the input file.

        :param file: The file containing all orders.
        :return: The aggregate profit and loss for all paired trades.
        """
        try:
            with open(file, 'r') as csv_file:
                reader = csv.DictReader(csv_file, delimiter=',')
                print('OPEN_TIME,CLOSE_TIME,SYMBOL,QUANTITY,PNL,OPEN_SIDE,CLOSE_SIDE,OPEN_PRICE,CLOSE_PRICE')
                for order in reader:
                    order['TIME'] = int(order.get('TIME'))
                    order['PRICE'] = float(order.get('PRICE'))
                    order['QUANTITY'] = int(order.get('QUANTITY'))
                    self.place_order(order)
        except FileNotFoundError as e:
            print(e)
            sys.exit(1)
        return self.total

    def place_order(self, order: dict) -> None:
        """
        Submits an order that will either increase the existing buy/sell position or close out an existing position.

        :param order: The order for a particular symbol.
        :return: None
        """
        if not (pending := self.queue[order.get('SYMBOL')]) or (order.get('SIDE') is pending[0].get('SIDE')):
            pending.append(order)
            return
        while pending and order.get('QUANTITY'):
            quantity = min(order.get('QUANTITY'), pending[0].get('QUANTITY'))
            profit_and_loss = quantity * (order.get('PRICE') - pending[0].get('PRICE'))
            if order.get('SIDE') == 'B':
                self.total -= profit_and_loss
            else:
                self.total += profit_and_loss
            print(self.close_order(order, pending, quantity, profit_and_loss))
            pending[0]['QUANTITY'] -= quantity
            order['QUANTITY'] -= quantity
            if not pending[0].get('QUANTITY'):
                pending.popleft()
        if order.get('QUANTITY'):
            pending.append(order)

    @staticmethod
    def close_order(order: dict, pending: deque, quantity: int, profit_and_loss: float) -> str:
        """
        Creates an entry that represents a closed order following a successful trade.

        :param order: The order for a particular symbol that closes the pending trade.
        :param pending: The next pending trade for a particular symbol.
        :param quantity: The number of shares to buy/sell.
        :param profit_and_loss: The marginal profit/loss from closing the trade.
        :return: The paired trade.
        """
        closed_order = (f'{pending[0].get("TIME")},'
                        f'{order.get("TIME")},'
                        f'{order.get("SYMBOL")},'
                        f'{quantity},{profit_and_loss:.2f},'
                        f'{pending[0].get("SIDE")},'
                        f'{"S" if pending[0].get("SIDE") == "B" else "B"},'
                        f'{pending[0].get("PRICE"):.2f},'
                        f'{order.get("PRICE"):.2f}')
        return closed_order


def main():
    """
    Creates an order management system to process a list of orders, printing all completed trades with the total PnL.

    :return: None
    """
    oms = OrderManagementSystem()
    print(f'{oms.run(sys.argv[1]):.2f}')


if __name__ == '__main__':
    main()
