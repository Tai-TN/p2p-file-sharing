"""
Module Storage quản lý tập tin, chia mảnh (chunking), hash SHA256 và PieceManager cho P2P.
"""

from .metainfo import create_metainfo, load_metainfo, save_metainfo
from .piece_manager import PieceManager

__all__ = [
    'create_metainfo',
    'load_metainfo',
    'save_metainfo',
    'PieceManager'
]
