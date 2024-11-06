class ServerError(Exception):
    pass


class PublisherError(Exception):
    pass


class ConsumerError(Exception):
    pass


class SerializerLoadError(ValueError):
    pass


class SerializerDumpError(ValueError):
    pass
