from __future__ import annotations

import asyncio
import logging

from .account_usage_store import initialize_usage_store
from .lease_broker_store import initialize_lease_broker_store
from .main import _periodic_refresh_saved_auths

logger = logging.getLogger(__name__)


async def run() -> None:
    initialize_usage_store()
    initialize_lease_broker_store()
    await _periodic_refresh_saved_auths()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("auth keepalive worker stopped")


if __name__ == "__main__":
    main()
