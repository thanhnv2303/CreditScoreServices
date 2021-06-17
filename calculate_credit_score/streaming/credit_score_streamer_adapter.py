import logging

from calculate_credit_score.database.credit_score_database import Database
from exporter.console_exporter import ConsoleExporter

logger = logging.getLogger("CreditScoreStreamerAdapter")


class CreditScoreStreamerAdapter:
    def __init__(
            self,
            max_workers=8,
            item_exporter=ConsoleExporter(),
            database=Database(),
            list_token_filter="artifacts/token_credit_info/listToken.txt",
            token_info="artifacts/token_credit_info/infoToken.json"
    ):
        self.item_exporter = item_exporter

        self.max_workers = max_workers
        self.database = database

        self.logger = logging.getLogger('CreditScoreStreamerAdapter')

        self.list_token_filter = list_token_filter
        self.token_info = token_info

    def open(self):
        pass

    def get_current_block_number(self):
        """
        Xác định điểm đồng bộ hiện tại
        :return:
        """
        return 0

    def export_all(self, start_block=0, end_block=0):
        """
        Cập nhật thôn tin thị trường và tính điểm tín dụng cho các ví
        :return:
        """

        """
        Cập nhật giá thị trường cho các token
        """
        logger.info("Update token market info")
        # token_service = CreditScoreServiceV030(self.database, self.list_token_filter, self.token_info)
        # token_service.update_token_market_info(fileInput=self.list_token_filter, fileOutput=self.token_info)

        logger.info("-------------------------------------------------------------")
        logger.info("CalculateWalletCreditScoreJob ")
        """
        Tính điểm tín dụng 
        Có thể xử lý đa luồng cho dữ liệu. Xem mẫu ở jobs/job_run_multithread.py
        
        """

    def close(self):
        pass
