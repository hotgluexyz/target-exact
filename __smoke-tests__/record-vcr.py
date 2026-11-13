from hotglue_smoke_test.vcr.target import VCRTargetTestRunner

from target_exact.constants import SALES_ORDER_STATUS


class ConnectorTargetTestRunner(VCRTargetTestRunner):
    # The authenticator branches on Exact's token reply "access_token not expired"
    PRESERVE_KEYS = {"error", "error_description"}
    PRESERVE_VALUES = set(SALES_ORDER_STATUS)

    def module(self) -> str:
        return "target_exact"

    def launch(self):
        from target_exact.target import TargetExact

        TargetExact.cli()


if __name__ == "__main__":
    ConnectorTargetTestRunner.main()
