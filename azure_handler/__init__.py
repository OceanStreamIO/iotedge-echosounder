# Storage (always available — no Azure SDK required for LocalStorage)
from .storage import (
    StorageBackend,
    LocalStorage,
    AzureBlobEdgeStorage,
    create_storage,
)


def __getattr__(name: str):
    """Lazy-load Azure IoT / Blob SDK symbols so standalone mode works
    without ``azure-iot-device`` or ``azure-storage-blob`` installed."""
    _connect = {"create_blob_service_client", "ensure_container_exists"}
    _iothub = {"create_client", "provision_iot_central_device"}
    _msg = {"send_to_hub"}

    if name in _connect:
        from . import connect
        return getattr(connect, name)
    if name in _iothub:
        from . import connect_iothub
        return getattr(connect_iothub, name)
    if name in _msg:
        from . import message_handler
        return getattr(message_handler, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "create_blob_service_client",
    "create_client",
    "ensure_container_exists",
    "provision_iot_central_device",
    "send_to_hub",
    "StorageBackend",
    "LocalStorage",
    "AzureBlobEdgeStorage",
    "create_storage",
]
