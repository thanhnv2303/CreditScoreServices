import csv
import os
import sys
import time

from calculate_credit_score.one_wallet_edit.get_wallet_info import GetWalletInfo

cur_path = os.path.dirname(os.path.realpath(__file__)) + "/../../"
file_input = "calculate_credit_score/one_wallet_edit/statisticreport.csv"
path = cur_path + file_input
with open(os.path.join(sys.path[0], path)) as file:
    statistic_report = list(csv.reader(file))

wallet = GetWalletInfo()


# Calculate x1, x3, x4 for one file
class CalculateOneWallet:
    def __init__(self, address):
        self.mns = float(statistic_report[0][1])
        self.sstd = float(statistic_report[0][2])
        self.k = 30
        self.address = address
        # timestamp cua thoi diem tra
        self.current_timestamp = time.time()
        # List of balanceChangeLogTimestamps
        self.listBalanceCLT = wallet.neo4j_get_wallet_balanceChangeLogTimestamps(address)

        # List of balanceChangeLogValues
        self.listBalanceCLV = wallet.neo4j_get_wallet_balanceChangeLogValues(address)
        if (self.listBalanceCLT is not None):
            list.reverse(self.listBalanceCLT)
            list.reverse(self.listBalanceCLV)

        # List of depositChangeLogTimestamps
        self.listDepositCLT = wallet.neo4j_get_wallet_depositChangeLogTimestamps(address)

        # List of depositChangeLogValues
        self.listDepositCLV = wallet.neo4j_get_wallet_depositChangeLogValues(address)

        if (self.listDepositCLT is not None):
            list.reverse(self.listDepositCLT)
            list.reverse(self.listDepositCLV)

        # List of borrowChangeLogTimestamps
        self.listBorrowCLT = wallet.neo4j_get_wallet_borrowChangeLogTimestamps(address)

        # List of borrowChangeLogValues
        self.listBorrowCLV = wallet.neo4j_get_wallet_borrowChangeLogValues(address)

        if (self.listBorrowCLT is not None):
            list.reverse(self.listBorrowCLT)
            list.reverse(self.listBorrowCLV)

    def get_zscore(self, value):
        return (value - self.mns) / self.sstd

    def get_tscore(self, value):
        t_score = self.get_zscore(value) * 100 + 500
        if (t_score > 1000):
            return 1000
        return t_score

    def get_x1(self):
        value = self.get_asset()
        return self.get_tscore(value)

    def get_asset(self):
        if (self.listBalanceCLT is None):
            return 0
        a = len(self.listBalanceCLT) - 1
        if (a < 0):
            balance = wallet.neo4j_get_wallet_balanceInUSD(self.address)
            if (balance is None):
                balance = 0
            deposit = wallet.neo4j_get_wallet_depositInUSD(self.address)
            if (deposit is None):
                deposit = 0
            borrow = wallet.neo4j_get_wallet_borrowInUSD(self.address)
            if (borrow is None):
                borrow = 0
            return deposit + balance - borrow
        asset = 0
        if (self.listDepositCLT is None):
            return self.listBalanceCLV[0]

        b = len(self.listDepositCLT) - 1
        if b < 0:
            deposit = wallet.neo4j_get_wallet_depositInUSD(self.address)
            if (deposit is None):
                deposit = 0
        else:
            deposit = self.listDepositCLV[b]

        if (self.listBorrowCLT is None):
            borrow = wallet.neo4j_get_wallet_borrowInUSD(self.address)
            if borrow is None:
                borrow = 0
            c = -1
        else:

            c = len(self.listBorrowCLT) - 1
            if (c < 0):
                borrow = wallet.neo4j_get_wallet_borrowInUSD(self.address)
                if borrow is None:
                    borrow = 0
            else:
                borrow = self.listBorrowCLV[c]

        while a > 0:
            asset += (self.listBalanceCLV[a] + deposit - borrow) * (
                    self.listBalanceCLT[a - 1] - self.listBalanceCLT[a]) / (self.k * 86400)
            if (c > 0 and (self.listBorrowCLT[c - 1] == self.listBalanceCLT[a - 1])):
                c = c - 1
                borrow = self.listBorrowCLV[c]
            if (b > 0 and (self.listDepositCLT[b - 1] == self.listBalanceCLT[a - 1])):
                b = b - 1
                deposit = self.listDepositCLV[b]
            a += -1

        asset += self.listBalanceCLV[0] + deposit - borrow
        return asset

    def get_x4(self):
        if (self.listBalanceCLT is None):
            return 0

        a = len(self.listBalanceCLT) - 1
        if (a < 0):
            balance = wallet.neo4j_get_wallet_balanceInUSD(self.address)
            if (balance is None):
                balance = 0
            deposit = wallet.neo4j_get_wallet_depositInUSD(self.address)
            if (deposit is None):
                deposit = 0
            borrow = wallet.neo4j_get_wallet_borrowInUSD(self.address)
            if (borrow is None):
                borrow = 0
            if balance + deposit - borrow == 0:
                return 0
            return deposit / (balance + deposit - borrow) * 1000
        x4 = 0
        asset = 0
        if (self.listDepositCLT is None):
            return 0

        b = len(self.listDepositCLT) - 1
        if (b < 0):
            deposit = wallet.neo4j_get_wallet_depositInUSD(self.address)
            if (deposit is None):
                return 0
        else:
            deposit = self.listDepositCLV[b]

        if (self.listBorrowCLT is None):
            borrow = wallet.neo4j_get_wallet_borrowInUSD(self.address)
            if borrow is None:
                borrow = 0
            c = -1
        else:
            c = len(self.listBorrowCLT) - 1
            if (c < 0):
                borrow = wallet.neo4j_get_wallet_borrowInUSD(self.address)
                if borrow is None:
                    borrow = 0
            else:
                borrow = self.listBorrowCLV[c]

        while a > 0:
            asset = (self.listBalanceCLV[a] + deposit - borrow)
            if (c > 0 and (self.listBorrowCLT[c - 1] == self.listBalanceCLT[a - 1])):
                c = c - 1
                borrow = self.listBorrowCLV[c]
            if (b > 0 and (self.listDepositCLT[b - 1] == self.listBalanceCLT[a - 1])):
                b = b - 1
                deposit = self.listDepositCLV[b]

            if (asset == 0):
                return 0
            x4 += deposit * (self.listBalanceCLT[a - 1] - self.listBalanceCLT[a]) / (asset * self.k * 86400)
            a += -1
        asset = (self.listBalanceCLV[0] + deposit - borrow)
        if (asset == 0):
            return 0
        x4 += deposit * (self.current_timestamp - self.listBalanceCLT[0]) / (asset * self.k * 86400)
        if (x4 > 1000):
            return 1000
        return x4 * 1000

    def get_x31(self):

        if (self.listBalanceCLT is None):
            return 0

        a = len(self.listBalanceCLT) - 1
        if (a < 0):
            balance = wallet.neo4j_get_wallet_balanceInUSD(self.address)
            if (balance is None or balance == 0):
                return 0
            borrow = wallet.neo4j_get_wallet_borrowInUSD(self.address)
            if (borrow is None):
                borrow = 0

            if borrow / balance > 1:
                return 0
            else:
                return (1 - borrow / balance) * 1000
        x31 = 0

        if (self.listBorrowCLT is None):
            borrow = wallet.neo4j_get_wallet_borrowInUSD(self.address)
            if borrow is None:
                borrow = 0
            c = -1
        else:

            c = len(self.listBorrowCLT) - 1
            if (c < 0):
                borrow = wallet.neo4j_get_wallet_borrowInUSD(self.address)
                if borrow is None:
                    return 1000
            else:
                borrow = self.listBorrowCLV[c]
        while a > 0:
            if (self.listBalanceCLV[a] == 0):
                x31 += 0
            else:
                x31 += borrow * (self.listBalanceCLT[a - 1] - self.listBalanceCLT[a]) / (
                        self.listBalanceCLV[a] * self.k * 86400)
            if (c > 0 and (self.listBorrowCLT[c - 1] == self.listBalanceCLT[a - 1])):
                c = c - 1
                borrow = self.listBorrowCLV[c]
            a += -1
        if (self.listBalanceCLT[0] == 0):
            return 0

        x31 += borrow * (self.current_timestamp - self.listBalanceCLT[0]) / (self.listBalanceCLT[0] * self.k * 86400)
        if x31 > 1:
            return 0
        return (1 - x31) * 1000

    def get_x32(self):

        if (self.listBalanceCLT is None):
            return 0

        a = len(self.listBalanceCLT) - 1
        if (a < 0):
            deposit = wallet.neo4j_get_wallet_depositInUSD(self.address)
            if (deposit is None or deposit == 0):
                return 0
            borrow = wallet.neo4j_get_wallet_borrowInUSD(self.address)
            if (borrow is None):
                borrow = 0

            if borrow / deposit > 1:
                return 0
            else:
                return (1 - borrow / deposit) * 1000

        x32 = 0

        if (self.listDepositCLT is None):
            return 0

        b = len(self.listDepositCLT) - 1
        if (b < 0):
            deposit = wallet.neo4j_get_wallet_depositInUSD(self.address)
            if (deposit is None):
                deposit = 0
        else:
            deposit = self.listDepositCLV[b]

        if (self.listBorrowCLT is None):
            borrow = wallet.neo4j_get_wallet_borrowInUSD(self.address)
            if borrow is None:
                borrow = 0
            c = -1
        else:

            c = len(self.listBorrowCLT) - 1
            if (c < 0):
                borrow = wallet.neo4j_get_wallet_borrowInUSD(self.address)
                if borrow is None:
                    return 1000
            else:
                borrow = self.listBorrowCLV[c]

        while a > 0:
            if (deposit == 0):
                x32 += 0
            else:
                x32 += borrow * (self.listBalanceCLT[a - 1] - self.listBalanceCLT[a]) / (deposit * self.k * 86400)
            if (c > 0 and (self.listBorrowCLT[c - 1] == self.listBalanceCLT[a - 1])):
                c = c - 1
                borrow = self.listBorrowCLV[c]
            if (b > 0 and (self.listDepositCLT[b - 1] == self.listBalanceCLT[a - 1])):
                b = b - 1
                deposit = self.listDepositCLV[b]
            a += -1
        if (self.listDepositCLV[0] == 0):
            x32 += 0
        else:
            x32 += borrow * (self.current_timestamp - self.listBalanceCLT[0]) / (
                    self.listDepositCLV[0] * self.k * 86400)
        if x32 > 1:
            return 0
        return (1 - x32) * 1000

    def get_x3(self):
        return self.get_x31() * 0.6 + self.get_x32() * 0.4
    def get_x12(self):
        deposit = wallet.neo4j_get_wallet_depositInUSD(self.address)
        if deposit is None:
            deposit = 0 
        borrow = wallet.neo4j_get_wallet_borrowInUSD(self.address)
        if borrow is None:
            borrow = 0
        balance = wallet.neo4j_get_wallet_balanceInUSD(self.address)
        if (balance is None):
            balance = 0
        asset_current = balance + deposit - borrow;
        if asset_current < 1000:
            return 0
        if asset_current < 10000:
            return asset_current/10
        if asset_current > 10000:
            return 1000 

# cal= CalculateOneWallet("0x1ca3Ac3686071be692be7f1FBeCd6686414sada")
# print (cal.get_x4())
# print (cal.get_x31())
# print (cal.get_x32())
# print (cal.get_x3())
# print (cal.get_x1())
# print (cal.get_asset())
