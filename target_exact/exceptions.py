from hotglue_etl_exceptions import InvalidPayloadError


class InvalidOrderNumberError(InvalidPayloadError):
    pass


class MissingItemError(InvalidPayloadError):
    pass


class InvalidOrderedByError(InvalidPayloadError):
    pass
