from api.qr import QR


class QROrder(QR):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.module_settings = {
            "module_name": "front.orders",
            "class_name": "ru.edgex.quickresto.modules.front.orders.OrderInfo",
            "module_date_field": "localCreateDate",
            "module_fields": [
                "createDate",
                "localCreateDate",
                "createTerminalSalePlace",
                "returned",
                "frontTotalPrice",
                "id",
                "payments",
            ],
            "sale_place_field": "createTerminalSalePlaceDocId",
        }

    def get(self, day):
        pass
