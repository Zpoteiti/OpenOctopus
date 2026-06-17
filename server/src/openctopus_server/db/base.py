from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


# Import all models so they register with Base.metadata.
from openctopus_server.db import models  # noqa: E402,F401
