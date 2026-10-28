"""Validation shared by the IPv6-only tracker and peers."""

import ipaddress


def normalize_ipv6_address(value: str) -> str:
    """Return a plain IPv6 literal, rejecting IPv4 and scoped addresses."""
    if not isinstance(value, str) or not value or "%" in value:
        raise ValueError(f"Địa chỉ IPv6 không hợp lệ: {value!r}")
    try:
        address = ipaddress.IPv6Address(value)
    except ipaddress.AddressValueError as exc:
        raise ValueError(f"Địa chỉ IPv6 không hợp lệ: {value!r}") from exc
    if address.ipv4_mapped is not None:
        raise ValueError(f"Không chấp nhận IPv4 ánh xạ thành IPv6: {value!r}")
    return str(address)
