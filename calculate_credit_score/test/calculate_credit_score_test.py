import os
import sys

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.join(TOP_DIR, '../'))

from config.config import Neo4jConfig
from calculate_credit_score.test.standardized_score_services import get_standardized_score_info
from calculate_credit_score.test.get_wallet_info import GetWalletInfo
import numpy as np
from calculate_credit_score.test.calculate_x2 import CalculateX2
from calculate_credit_score.test.calculatex51 import CalculateX51
from py2neo import Graph

graph = Graph(Neo4jConfig.BOLT, auth=(Neo4jConfig.NEO4J_USERNAME, Neo4jConfig.NEO4J_PASSWORD))
wallet = GetWalletInfo()


class calculate_x134():
    def __init__(self):
        self.k = 100

        self.wallet_list = graph.run(" MATCH (a:Wallet) return a.address;").data()

        self.asset_list = self.get_asset_list()
        self.mns, self.sstd = get_standardized_score_info(self.asset_list)

    def get_z_score(self, value):

        return (value - self.mns) / self.sstd

    def get_t_score(self, value):

        return self.get_z_score(value) * 100 + 500

    def get_x1(self, address):
        asset = self.get_asset(address)
        return self.get_t_score(asset)

    def get_asset_list(self):
        # List of assets of wallet
        asset_list = []
        for wallet in self.wallet_list:
            address = wallet['a.address']
            asset = self.get_asset(address)
            asset_list.append(asset)

        return asset_list

    def get_x4(self, address):
        # timestamp cua thoi diem tra
        self.current_timestamp = wallet.neo4j_get_wallet_lastUpdatedAt(address)
        # List of balanceChangeLogTimestamps
        self.listBalanceCLT = wallet.neo4j_get_wallet_balanceChangeLogTimestamps(address)
        # List of balanceChangeLogValues
        self.listBalanceCLV = wallet.neo4j_get_wallet_balanceChangeLogValues(address)

        # List of depositChangeLogTimestamps
        self.listDepositCLT = wallet.neo4j_get_wallet_depositChangeLogTimestamps(address)
        # List of depositChangeLogValues
        self.listDepositCLV = wallet.neo4j_get_wallet_depositChangeLogValues(address)

        # List of borrowChangeLogTimestamps
        self.listBorrowCLT = wallet.neo4j_get_wallet_borrowChangeLogTimestamps(address)
        # List of borrowChangeLogValues
        self.listBorrowCLV = wallet.neo4j_get_wallet_borrowChangeLogValues(address)

        x4 = 0
        a = len(self.listBalanceCLT) - 1
        if (a < 0):
            balance = wallet.neo4j_get_wallet_balanceInUSD(address)
            deposit = wallet.neo4j_get_wallet_depositInUSD(address)
            borrow = wallet.neo4j_get_wallet_borrowInUSD(address)
            return balance + deposit + borrow
        asset = 0
        b = len(self.listDepositCLT) - 1
        if (b < 0):
            deposit = wallet.neo4j_get_wallet_depositInUSD(address)
        else:
            deposit = self.listDepositCLV[b]

        c = len(self.listBorrowCLT) - 1
        if (c < 0):
            borrow = wallet.neo4j_get_wallet_borrowInUSD(address)
        else:
            borrow = self.listBorrowCLV[c]
        while a > 0:
            asset = (self.listBalanceCLV[a] + deposit - borrow)
            if (self.listBalanceCLV[a] < self.listBalanceCLV[a - 1]) and c > 0 and (
                    self.listBorrowCLT[c - 1] == self.listBalanceCLT[a - 1]):
                c = c - 1
                borrow = self.listBorrowCLV[c]
            if (self.listBalanceCLV[a] > self.listBalanceCLV[a - 1]) and b > 0 and (
                    self.listDepositCLT[b - 1] == self.listBalanceCLT[a - 1]):
                b = b - 1
                deposit = self.listDepositCLT[b]

            if (asset == 0):
                return 1000
            x4 += deposit * (self.listBalanceCLT[a - 1] - self.listBalanceCLT[a]) / (asset * self.k * 86400)
            a += -1
        asset = (self.listBalanceCLV[0] + deposit - borrow)
        if (asset == 0):
            return 1000
        x4 += deposit * (self.current_timestamp - self.listBalanceCLT[0]) / (asset * self.k * 86400)
        return x4 * 1000

    def get_asset(self, address):
        # timestamp cua thoi diem tra
        self.current_timestamp = wallet.neo4j_get_wallet_lastUpdatedAt(address)
        # List of balanceChangeLogTimestamps
        self.listBalanceCLT = wallet.neo4j_get_wallet_balanceChangeLogTimestamps(address)
        # List of balanceChangeLogValues
        self.listBalanceCLV = wallet.neo4j_get_wallet_balanceChangeLogValues(address)

        # List of depositChangeLogTimestamps
        self.listDepositCLT = wallet.neo4j_get_wallet_depositChangeLogTimestamps(address)
        # List of depositChangeLogValues
        self.listDepositCLV = wallet.neo4j_get_wallet_depositChangeLogValues(address)

        # List of borrowChangeLogTimestamps
        self.listBorrowCLT = wallet.neo4j_get_wallet_borrowChangeLogTimestamps(address)
        # List of borrowChangeLogValues
        self.listBorrowCLV = wallet.neo4j_get_wallet_borrowChangeLogValues(address)

        a = len(self.listBalanceCLT) - 1
        if (a < 0):
            balance = wallet.neo4j_get_wallet_balanceInUSD(address)
            deposit = wallet.neo4j_get_wallet_depositInUSD(address)
            borrow = wallet.neo4j_get_wallet_borrowInUSD(address)
            return balance + deposit + borrow
        asset = 0
        b = len(self.listDepositCLT) - 1
        if (b < 0):
            deposit = wallet.neo4j_get_wallet_depositInUSD(address)
        else:
            deposit = self.listDepositCLV[b]

        c = len(self.listBorrowCLT) - 1
        if (c < 0):
            borrow = wallet.neo4j_get_wallet_borrowInUSD(address)
        else:
            borrow = self.listBorrowCLV[c]
        while a > 0:
            asset += (self.listBalanceCLV[a] + deposit - borrow) * (
                    self.listBalanceCLT[a - 1] - self.listBalanceCLT[a]) / (self.k * 86400)
            if (self.listBalanceCLV[a] < self.listBalanceCLV[a - 1]) and c > 0 and (
                    self.listBorrowCLT[c - 1] == self.listBalanceCLT[a - 1]):
                c = c - 1
                borrow = self.listBorrowCLV[c]
            if (self.listBalanceCLV[a] > self.listBalanceCLV[a - 1]) and b > 0 and (
                    self.listDepositCLT[b - 1] == self.listBalanceCLT[a - 1]):
                b = b - 1
                deposit = self.listDepositCLT[b]
            a += -1
        asset += (self.listBalanceCLV[0] + deposit - borrow) * (self.current_timestamp - self.listBalanceCLT[0]) / (
                self.k * 86400)
        return asset

    def get_borrow_and_deposit_history(self, address):
        # timestamp cua thoi diem tra
        self.current_timestamp = wallet.neo4j_get_wallet_lastUpdatedAt(address)
        # List of balanceChangeLogTimestamps
        self.listBalanceCLT = wallet.neo4j_get_wallet_balanceChangeLogTimestamps(address)
        # List of balanceChangeLogValues
        self.listBalanceCLV = wallet.neo4j_get_wallet_balanceChangeLogValues(address)

        # List of depositChangeLogTimestamps
        self.listDepositCLT = wallet.neo4j_get_wallet_depositChangeLogTimestamps(address)
        # List of depositChangeLogValues
        self.listDepositCLV = wallet.neo4j_get_wallet_depositChangeLogValues(address)

        # List of borrowChangeLogTimestamps
        self.listBorrowCLT = wallet.neo4j_get_wallet_borrowChangeLogTimestamps(address)
        # List of borrowChangeLogValues
        self.listBorrowCLV = wallet.neo4j_get_wallet_borrowChangeLogValues(address)

        # List that describe the order of events
        borrowAndDepositHistory = []
        # Number of element inside of listDepositCLT
        i = len(self.listDepositCLT) - 1
        # Number of element inside of listBorrowCLT
        j = len(self.listBorrowCLT) - 1
        while (i >= 0 or j >= 0):
            if (j < 0 or (i >= 0 and self.listDepositCLT[i] < self.listBorrowCLT[j])):
                borrowAndDepositHistory.append(0)
                i += -1
            else:
                borrowAndDepositHistory.append(1)
                j += -1
        return borrowAndDepositHistory

    def get_borrow_and_balance_history(self, address):
        # List that describe the order of events
        borrowAndBalanceHistory = []
        # timestamp cua thoi diem tra
        self.current_timestamp = wallet.neo4j_get_wallet_lastUpdatedAt(address)
        # List of balanceChangeLogTimestamps
        self.listBalanceCLT = wallet.neo4j_get_wallet_balanceChangeLogTimestamps(address)
        # List of balanceChangeLogValues
        self.listBalanceCLV = wallet.neo4j_get_wallet_balanceChangeLogValues(address)

        # List of depositChangeLogTimestamps
        self.listDepositCLT = wallet.neo4j_get_wallet_depositChangeLogTimestamps(address)
        # List of depositChangeLogValues
        self.listDepositCLV = wallet.neo4j_get_wallet_depositChangeLogValues(address)

        # List of borrowChangeLogTimestamps
        self.listBorrowCLT = wallet.neo4j_get_wallet_borrowChangeLogTimestamps(address)
        # List of borrowChangeLogValues
        self.listBorrowCLV = wallet.neo4j_get_wallet_borrowChangeLogValues(address)

        # Number of element inside of listBalanceCLT
        i = len(self.listBalanceCLT) - 1
        # Number of element inside of listBorrowCLT
        j = len(self.listBorrowCLT) - 1
        while (i >= 0 or j >= 0):
            if (j < 0 or (i > 0 and self.listBalanceCLT[i] < self.listBorrowCLT[j])):
                borrowAndBalanceHistory.append(0)
                i += -1
            else:
                borrowAndBalanceHistory.append(1)
                j += -1
        return borrowAndBalanceHistory

    def get_x31(self, address):
        # timestamp cua thoi diem tra
        self.current_timestamp = wallet.neo4j_get_wallet_lastUpdatedAt(address)
        # List of balanceChangeLogTimestamps
        self.listBalanceCLT = wallet.neo4j_get_wallet_balanceChangeLogTimestamps(address)
        # List of balanceChangeLogValues
        self.listBalanceCLV = wallet.neo4j_get_wallet_balanceChangeLogValues(address)

        # List of depositChangeLogTimestamps
        self.listDepositCLT = wallet.neo4j_get_wallet_depositChangeLogTimestamps(address)
        # List of depositChangeLogValues
        self.listDepositCLV = wallet.neo4j_get_wallet_depositChangeLogValues(address)

        # List of borrowChangeLogTimestamps
        self.listBorrowCLT = wallet.neo4j_get_wallet_borrowChangeLogTimestamps(address)
        # List of borrowChangeLogValues
        self.listBorrowCLV = wallet.neo4j_get_wallet_borrowChangeLogValues(address)

        self.borrow_and_balance_history = self.get_borrow_and_balance_history(address)

        self.borrow_and_deposit_history = self.get_borrow_and_deposit_history(address)

        x31 = 0
        # Number of element inside of listBalanceCLT
        i = len(self.listBalanceCLT) - 1
        # Number of element inside of listBorrowCLT
        j = len(self.listBorrowCLT) - 1
        if (len(self.borrow_and_balance_history) <= 0):
            return 0
        if (j < 0):
            return 1000
        if (i < 0):
            return 0
        for f in range(len(self.borrow_and_balance_history) - 1):
            if (self.listBalanceCLT[i] == 0):
                return 0
            if (self.borrow_and_balance_history[f] == 0):
                present_timestamp = self.listBalanceCLT[i]
            else:
                present_timestamp = self.listBorrowCLT[j]

            if (self.borrow_and_balance_history[f + 1] == 0):
                if (self.borrow_and_balance_history[f] == 0):
                    x31 += self.listBorrowCLV[j] * (self.listBalanceCLT[i - 1] - present_timestamp) / (
                            86400 * self.k * self.listBalanceCLV[i])
                    i += -1
                else:
                    x31 += self.listBorrowCLV[j] * (self.listBalanceCLT[i] - present_timestamp) / (
                            86400 * self.k * self.listBalanceCLV[i])
                    j += -1

            else:
                if (self.borrow_and_balance_history[f] == 1):
                    x31 += self.listBorrowCLV[j] * (self.listBorrowCLT[j - 1] - present_timestamp) / (
                            86400 * self.k * self.listBalanceCLV[i])
                    j += -1
                else:
                    x31 += self.listBorrowCLV[j] * (self.listBorrowCLT[j] - present_timestamp) / (
                            86400 * self.k * self.listBalanceCLV[i])
                    i += -1

        last_index = len(self.borrow_and_balance_history) - 1
        if (self.borrow_and_balance_history[last_index] == 0):

            x31 += self.listBorrowCLV[j] / self.listBalanceCLV[i] * (
                    self.current_timestamp - self.listBalanceCLT[i]) / (86400 * self.k)
        else:

            x31 += self.listBorrowCLV[j] / self.listBalanceCLV[i] * (self.current_timestamp - self.listBorrowCLT[j]) / (
                    86400 * self.k)

        if x31 < 1:
            return (1 - x31) * 1000
        else:
            return 0

    def get_x32(self, address):
        # timestamp cua thoi diem tra
        self.current_timestamp = wallet.neo4j_get_wallet_lastUpdatedAt(address)
        # List of balanceChangeLogTimestamps
        self.listBalanceCLT = wallet.neo4j_get_wallet_balanceChangeLogTimestamps(address)
        # List of balanceChangeLogValues
        self.listBalanceCLV = wallet.neo4j_get_wallet_balanceChangeLogValues(address)

        # List of depositChangeLogTimestamps
        self.listDepositCLT = wallet.neo4j_get_wallet_depositChangeLogTimestamps(address)
        # List of depositChangeLogValues
        self.listDepositCLV = wallet.neo4j_get_wallet_depositChangeLogValues(address)

        # List of borrowChangeLogTimestamps
        self.listBorrowCLT = wallet.neo4j_get_wallet_borrowChangeLogTimestamps(address)
        # List of borrowChangeLogValues
        self.listBorrowCLV = wallet.neo4j_get_wallet_borrowChangeLogValues(address)

        self.borrow_and_balance_history = self.get_borrow_and_balance_history(address)

        self.borrow_and_deposit_history = self.get_borrow_and_deposit_history(address)

        x32 = 0
        # Number of element inside of listDepositCLT
        i = len(self.listDepositCLT) - 1
        # Number of element inside of listBorrowCLT
        j = len(self.listBorrowCLT) - 1

        if (len(self.borrow_and_deposit_history) <= 0):
            return 0
        if (j < 0):
            return 1000
        if (i < 0):
            return 0

        for f in range(len(self.borrow_and_deposit_history) - 1):
            if (self.listDepositCLT[i] == 0):
                return 0
            if (self.borrow_and_deposit_history[f] == 0):
                present_timestamp = self.listDepositCLT[i]
            else:
                present_timestamp = self.listBorrowCLT[j]

            if (self.borrow_and_deposit_history[f + 1] == 0):
                if (self.borrow_and_deposit_history[f] == 0):
                    x32 += self.listBorrowCLV[j] * (self.listDepositCLT[i - 1] - present_timestamp) / (
                            86400 * self.k * self.listDepositCLV[i])
                    i += -1
                else:
                    x32 += self.listBorrowCLV[j] * (self.listDepositCLT[i] - present_timestamp) / (
                            86400 * self.k * self.listDepositCLV[i])
                    j += -1
            else:
                if (self.borrow_and_deposit_history[f] == 1):
                    x32 += self.listBorrowCLV[j] * (self.listBorrowCLT[j - 1] - present_timestamp) / (
                            86400 * self.k * self.listDepositCLV[i])
                    j += -1
                else:
                    x32 += self.listBorrowCLV[j] * (self.listBorrowCLT[j] - present_timestamp) / (
                            86400 * self.k * self.listDepositCLV[i])
                    i += -1

        last_index = len(self.borrow_and_deposit_history) - 1
        if (self.borrow_and_deposit_history[last_index] == 0):

            x32 += self.listBorrowCLV[j] / self.listDepositCLV[i] * (
                    self.current_timestamp - self.listDepositCLT[i]) / (86400 * self.k)
        else:

            x32 += self.listBorrowCLV[j] / self.listDepositCLV[i] * (self.current_timestamp - self.listBorrowCLT[j]) / (
                    86400 * self.k)

        if x32 < 1:
            return (1 - x32) * 1000
        else:
            return 0

    def get_x_list(self):
        x1_list = []
        x3_list = []
        x4_list = []
        addr_list = []
        for wallet in self.wallet_list:
            address = wallet['a.address']
            x1_list.append(self.get_x1(address))
            x31 = self.get_x31(address)
            x32 = self.get_x32(address)
            x3_list.append(x31 * 0.6 + x32 * 0.4)
            x4_list.append(self.get_x4(address))
            addr_list.append(address)
        return x1_list, x3_list, x4_list, addr_list

    # def get_x3_list(self):
    #    x3_list=[]
    #    for wallet in self.wallet_list:
    #        address = wallet['a.address']
    #        x31=self.get_x31(address)
    #        x32=self.get_x32(address)
    #        x3_list.append(x31*0.6+x32*0.4)
    #    return x3_list

    # def get_x4_list(self):
    #    x4_list=[]
    #    for wallet in self.wallet_list:
    #        address=wallet['a.address']
    #        x4_list.append(self.get_x4(address))
    #    return x4_list  

    # def get_address(self):


# 	addr = []
#     for wallet in self.wallet_list:
#         address=wallet['a.address']
#         addr.append(address)
#     return addr

print("Caculating Credit Score")

calc2 = CalculateX2()
x2 = calc2.get_x2_list()
calc5 = CalculateX51()
x5 = calc5.get_x51_list()
calc = calculate_x134()
[x1, x3, x4, addr] = calc.get_x_list()
# addr = calc.get_address()

# x3 = calc.get_x3_list()
# x4 = calc.get_x4_list()
x = np.zeros(len(x1))

for i in range(len(x1)):
    x[i] = 0.25 * x1[i] + 0.35 * x2[i] + 0.15 * x3[i] + 0.2 * x4[i] + 0.05 * x5[i]
    query = "match (w:Wallet) WHERE w.address =" + '"' + addr[i] + '"' + "SET w.creditScore = " + str(x[i])
    graph.run(query)
print("Done")
