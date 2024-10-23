import typing as t

from yarl import URL

from protomq.abc import Driver


def connect(url: str | URL) -> t.AsyncContextManager[Driver]:
    clean_url = URL(url) if isinstance(url, str) else url

    driver = clean_url.query.get("driver", "aiormq")

    if driver == "aiormq":
        from protomq.driver.aiormq import AiormqDriver

        return AiormqDriver.connect(clean_url)

    else:
        details = "unsupported driver"
        raise ValueError(details, driver)
