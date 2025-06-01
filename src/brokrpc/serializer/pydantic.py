from pydantic import BaseModel
from pydantic.main import IncEx
from pydantic_core import PydanticSerializationError

from brokrpc.abc import Serializer
from brokrpc.message import BinaryMessage, DecodedMessage, EncodedMessage, Message
from brokrpc.model import SerializerDumpError, SerializerLoadError


class PydanticSerializer[T: BaseModel](Serializer[Message[T], BinaryMessage]):
    # NOTE: allow `model_dump_json` & `model_validate_json` pydantic model methods customization
    def __init__(  # noqa: PLR0913
        self,
        model: type[T],
        *,
        indent: int | None = None,
        include: IncEx | None = None,
        exclude: IncEx | None = None,
        context: object = None,
        by_alias: bool = True,
        exclude_unset: bool = True,
        exclude_defaults: bool = False,
        exclude_none: bool = True,
        strict: bool | None = False,
    ) -> None:
        self.__model = model
        self.__indent = indent
        self.__include = include
        self.__exclude = exclude
        self.__context = context
        self.__by_alias = by_alias
        self.__exclude_unset = exclude_unset
        self.__exclude_defaults = exclude_defaults
        self.__exclude_none = exclude_none
        self.__strict = strict

    def encode_message(self, message: Message[T]) -> BinaryMessage:
        assert isinstance(message.body, self.__model)

        try:
            body = message.body.model_dump_json(
                indent=self.__indent,
                include=self.__include,
                exclude=self.__exclude,
                context=self.__context,
                by_alias=self.__by_alias,
                exclude_unset=self.__exclude_unset,
                exclude_defaults=self.__exclude_defaults,
                exclude_none=self.__exclude_none,
            ).encode()

        except PydanticSerializationError as err:
            details = "can't dump pydantic model"
            raise SerializerDumpError(details, message) from err

        return EncodedMessage(
            body=body,
            content_type="application/json",
            content_encoding="utf-8",
            original=message,
        )

    def decode_message(self, message: BinaryMessage) -> Message[T]:
        try:
            body = self.__model.model_validate_json(message.body, strict=self.__strict)

        except ValueError as err:
            details = "can't load pydantic model"
            raise SerializerLoadError(details, message) from err

        return DecodedMessage(
            original=message,
            body=body,
        )
